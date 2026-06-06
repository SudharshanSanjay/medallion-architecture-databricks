"""
forecast.py — sales revenue forecasting using Prophet + MLflow tracking.

Reads monthly_sales_trend from Gold Delta table,
fits a Prophet model, logs to MLflow, and writes
a revenue_forecast Delta table back to Gold.
"""

import os
import sys
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — no display needed
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mlflow
import mlflow.sklearn
from prophet import Prophet
from sklearn.metrics import mean_absolute_error

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp


def load_monthly_trend(spark: SparkSession, path: str) -> pd.DataFrame:
    """
    Load monthly_sales_trend Delta table into a Pandas DataFrame.
    Converts year_month string to datetime for Prophet.
    Scales revenue to Crore (÷1e7) for better model stability.
    """
    df_spark = spark.read.format("delta").load(path)
    df = df_spark.select("year_month", "monthly_revenue", "total_orders") \
                 .toPandas()

    df["ds"] = pd.to_datetime(df["year_month"] + "-01")
    # Scale to Crore — Prophet works better with smaller numbers
    df["y"]  = (df["monthly_revenue"] / 1e7).astype(float)
    df = df.sort_values("ds").reset_index(drop=True)

    # Drop partial current month
    df = df.iloc[:-1]

    return df


def train_prophet(df_train: pd.DataFrame) -> Prophet:
    """
    Fit a Prophet model optimised for short time series (12 months).
    Uses additive mode and linear trend — more stable with limited data.
    """
    model = Prophet(
        yearly_seasonality=False,   # not enough data for yearly patterns
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode="additive", # more stable than multiplicative
        changepoint_prior_scale=0.05, # conservative trend changes
        interval_width=0.80,          # 80% confidence intervals
    )
    # Add a gentle quarterly seasonality instead
    model.add_seasonality(
        name="quarterly",
        period=91.25,
        fourier_order=3
    )
    model.fit(df_train)
    return model


def evaluate_model(model: Prophet, df_test: pd.DataFrame) -> dict:
    """
    Predict on holdout test set and calculate accuracy metrics.
    MAE — average absolute error in rupees
    MAPE — average percentage error (lower is better)
    """
    forecast_test = model.predict(df_test[["ds"]])
    y_true = df_test["y"].values
    y_pred = forecast_test["yhat"].values

    mae  = mean_absolute_error(y_true, y_pred)
    mape = float(np.mean(np.abs((y_true - y_pred) / (y_true + 1e-9))) * 100)

    return {
        "mae":           round(mae, 2),
        "mape_pct":      round(mape, 2),
        "test_months":   len(df_test),
        "train_months":  len(df_test),
    }


def forecast_future(
    model: Prophet,
    df: pd.DataFrame,
    horizon_months: int = 6
) -> pd.DataFrame:
    """
    Generate forecast for the next N months beyond the data window.
    Returns full forecast DataFrame including historical fitted values.
    """
    future = model.make_future_dataframe(
        periods=horizon_months,
        freq="MS"  # month start frequency
    )
    return model.predict(future)


def save_forecast_chart(
    df: pd.DataFrame,
    df_forecast: pd.DataFrame,
    output_path: str
):
    """
    Save a forecast vs actuals chart to docs/images/.
    Shows historical revenue, fitted values and future forecast with
    confidence intervals.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 5))

    # Actuals
    ax.plot(df["ds"], df["y"],
            color="#1D9E75", linewidth=2,
            marker="o", markersize=4,
            label="Actual revenue")

    # Fitted + forecast
    hist_len = len(df)
    ax.plot(df_forecast["ds"][:hist_len], df_forecast["yhat"][:hist_len],
            color="#534AB7", linewidth=1.5, linestyle="--",
            label="Fitted (Prophet)")

    ax.plot(df_forecast["ds"][hist_len:], df_forecast["yhat"][hist_len:],
            color="#D85A30", linewidth=2,
            marker="s", markersize=4,
            label="Forecast (next 6 months)")

    # Confidence interval on forecast
    ax.fill_between(
        df_forecast["ds"][hist_len:],
        df_forecast["yhat_lower"][hist_len:],
        df_forecast["yhat_upper"][hist_len:],
        alpha=0.2, color="#D85A30",
        label="95% confidence interval"
    )

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.xticks(rotation=45)
    ax.set_title("Kali BMH Systems — Monthly Revenue Forecast",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue (₹ Crore)")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Forecast chart saved → {output_path}")


def write_forecast_table(
    spark: SparkSession,
    df_forecast: pd.DataFrame,
    df_actuals: pd.DataFrame,
    output_path: str
):
    """
    Write forecast results as a Gold Delta table.
    Includes both historical fitted values and future predictions.
    """
    # Merge actuals with forecast for comparison
    df_result = df_forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    df_result = df_result.merge(
        df_actuals[["ds", "y"]],
        on="ds", how="left"
    )
    df_result.columns = [
        "month", "forecast_revenue",
        "forecast_lower", "forecast_upper", "actual_revenue"
    ]
    df_result["month"] = df_result["month"].dt.strftime("%Y-%m")
    df_result["is_forecast"] = df_result["actual_revenue"].isna()
    df_result["forecast_revenue"] = df_result["forecast_revenue"].round(2)
    df_result["forecast_lower"]   = df_result["forecast_lower"].round(2)
    df_result["forecast_upper"]   = df_result["forecast_upper"].round(2)

    # Convert to Spark DataFrame and write as Delta
    df_spark = spark.createDataFrame(df_result) \
                    .withColumn("created_at", current_timestamp())

    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)

    count = spark.read.format("delta").load(output_path).count()
    print(f"  ✓ Forecast Delta table written → {output_path} ({count} rows)")
    return count


def run_forecast(
    spark: SparkSession,
    monthly_trend_path: str,
    forecast_output_path: str,
    chart_output_path: str,
    horizon_months: int = 6,
    holdout_months: int = 3,
    experiment_name: str = "kali_bmh_revenue_forecast"
):
    """
    Full forecasting pipeline:
    1. Load Gold monthly trend data
    2. Train Prophet model
    3. Evaluate on holdout
    4. Log to MLflow
    5. Write forecast Delta table
    6. Save chart
    """
    print("\n── Forecast layer ────────────────────────────────")

    # Load data
    print("  Loading monthly trend from Gold...")
    df = load_monthly_trend(spark, monthly_trend_path)
    print(f"  Months available: {len(df)}")

    # Train/test split
    df_train = df.iloc[:-holdout_months]
    df_test  = df.iloc[-holdout_months:]
    print(f"  Train: {len(df_train)} months, Test: {len(df_test)} months")

    # MLflow experiment
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="prophet_revenue_forecast"):

        # Log parameters
        mlflow.log_params({
            "model":           "Prophet",
            "horizon_months":  horizon_months,
            "holdout_months":  holdout_months,
            "seasonality_mode":"multiplicative",
            "train_months":    len(df_train),
        })

        # Train
        print("  Training Prophet model...")
        model = train_prophet(df_train)

        # Evaluate
        print("  Evaluating on holdout...")
        metrics = evaluate_model(model, df_test)
        mlflow.log_metrics({
            "mae":      metrics["mae"],
            "mape_pct": metrics["mape_pct"],
        })
        print(f"  MAE : ₹{metrics['mae']:,.2f}")
        print(f"  MAPE: {metrics['mape_pct']:.1f}%")

        # Forecast future
        print(f"  Forecasting next {horizon_months} months...")
        df_forecast = forecast_future(model, df, horizon_months)

        # Save chart
        save_forecast_chart(df, df_forecast, chart_output_path)
        mlflow.log_artifact(chart_output_path)

        # Write Delta table
        count = write_forecast_table(
            spark, df_forecast, df, forecast_output_path
        )
        mlflow.log_metric("forecast_rows", count)

        run_id = mlflow.active_run().info.run_id
        print(f"  MLflow run ID: {run_id}")

    print(f"  ✓ Forecast complete")
    return metrics