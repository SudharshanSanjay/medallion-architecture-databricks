import csv
import random
import argparse
import math
import os
from datetime import date, timedelta

CUSTOMERS = [
    'Arjun Mehta', 'Priya Nair', 'Vikram Singh', 'Divya Rao',
    'Kiran Iyer', 'Sanjay Rajan', 'Meena Pillai', 'Rohit Verma',
    'Ananya Krishnan', 'Suresh Babu', 'Lakshmi Venkat', 'Rahul Gupta'
]

PRODUCTS = [
    'Conveyor Belt', 'Bucket Elevator', 'Screw Conveyor',
    'Belt Feeder', 'Vibrating Screen', 'Rotary Valve',
    'Drag Chain Conveyor'
]

REGIONS = [
    'Tamil Nadu', 'Karnataka', 'Maharashtra',
    'Gujarat', 'Telangana', 'Rajasthan'
]

STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']

# Base prices per product — realistic B2B industrial equipment range
BASE_PRICES = {
    'Conveyor Belt':       25000,
    'Bucket Elevator':     35000,
    'Screw Conveyor':      28000,
    'Belt Feeder':         22000,
    'Vibrating Screen':    30000,
    'Rotary Valve':        18000,
    'Drag Chain Conveyor': 40000,
}

# Seasonal multiplier by month (1=Jan ... 12=Dec)
# Q4 (Oct-Dec) is peak, Q2 (Apr-Jun) is slower — typical B2B industrial pattern
SEASONAL_WEIGHTS = {
    1: 1.1, 2: 0.9, 3: 1.0, 4: 0.85, 5: 0.8,  6: 0.9,
    7: 1.0, 8: 1.05, 9: 1.1, 10: 1.2, 11: 1.3, 12: 1.25
}


def growth_factor(order_date: date, start_date: date, end_date: date) -> float:
    """
    Returns a multiplier between 1.0 (start) and 1.6 (end).
    Simulates 60% business growth over the data window.
    """
    total_days = (end_date - start_date).days or 1
    elapsed = (order_date - start_date).days
    progress = elapsed / total_days  # 0.0 to 1.0
    return 1.0 + 0.6 * progress


def seasonal_factor(order_date: date) -> float:
    """Returns the seasonal weight for the order's month."""
    return SEASONAL_WEIGHTS[order_date.month]


def generate_order_date(start_date: date, end_date: date) -> date:
    """
    Picks a random date biased toward recent months.
    More orders in recent months simulates business growth.
    """
    total_days = (end_date - start_date).days
    # Square root bias — later dates slightly more likely
    r = random.random() ** 0.7
    days_offset = int(r * total_days)
    return start_date + timedelta(days=days_offset)


def generate_unit_price(product: str, order_date: date,
                         start_date: date, end_date: date) -> float:
    """
    Generates a price with growth trend + noise.
    Prices increase ~40% over the data window.
    """
    base = BASE_PRICES[product]
    gf = growth_factor(order_date, start_date, end_date)
    noise = random.uniform(0.85, 1.15)  # ±15% random noise
    return round(base * gf * noise, 2)


def generate_quantity(order_date: date, start_date: date,
                       end_date: date) -> int:
    """
    Quantity slightly increases over time with seasonal bump.
    """
    sf = seasonal_factor(order_date)
    gf = growth_factor(order_date, start_date, end_date)
    base_qty = random.randint(1, 6)
    adjusted = base_qty * sf * gf
    return max(1, round(adjusted))


def generate_csv(output_path: str, rows: int = 3000):
    """
    Generates a CSV of sales orders with realistic trend and seasonality.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    end_date   = date.today()
    start_date = end_date - timedelta(days=365)

    fieldnames = [
        'order_id', 'customer_id', 'customer_name', 'product_sku',
        'product_name', 'quantity', 'unit_price', 'order_date',
        'region', 'status'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, rows + 1):
            product    = random.choice(PRODUCTS)
            order_date = generate_order_date(start_date, end_date)
            quantity   = generate_quantity(order_date, start_date, end_date)
            unit_price = generate_unit_price(product, order_date, start_date, end_date)

            writer.writerow({
                'order_id':      i,
                'customer_id':   random.randint(1, 300),
                'customer_name': random.choice(CUSTOMERS),
                'product_sku':   f"SKU-{random.randint(1, 100):04d}",
                'product_name':  product,
                'quantity':      quantity,
                'unit_price':    unit_price,
                'order_date':    order_date,
                'region':        random.choice(REGIONS),
                'status':        random.choice(STATUSES),
            })

    print(f"Generated {rows} rows → {output_path}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Growth trend: +60% volume, +40% prices over the year")
    print(f"Seasonal pattern: Q4 peak, Q2 trough")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate Kali BMH dummy sales data with trend and seasonality'
    )
    parser.add_argument(
        '--rows',
        type=int,
        default=3000,
        help='Number of rows to generate (default: 3000)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='data/raw/sales_orders_csv.csv',
        help='Output CSV path (default: data/raw/sales_orders_csv.csv)'
    )
    args = parser.parse_args()
    generate_csv(args.output, args.rows)