import csv
import random
from datetime import date, timedelta

CUSTOMERS = ['Arjun Mehta','Priya Nair','Vikram Singh','Divya Rao',
             'Kiran Iyer','Sanjay Rajan','Meena Pillai','Rohit Verma']
PRODUCTS  = ['Conveyor Belt','Bucket Elevator','Screw Conveyor',
             'Belt Feeder','Vibrating Screen','Rotary Valve','Drag Chain Conveyor']
REGIONS   = ['Tamil Nadu','Karnataka','Maharashtra','Gujarat','Telangana','Rajasthan']
STATUSES  = ['pending','confirmed','shipped','delivered','cancelled']

def random_date(days_back=365):
    return date.today() - timedelta(days=random.randint(0, days_back))

def generate_csv(path: str, rows: int = 300):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'order_id','customer_id','customer_name','product_sku',
            'product_name','quantity','unit_price','order_date','region','status'
        ])
        writer.writeheader()
        for i in range(1, rows + 1):
            writer.writerow({
                'order_id':      i,
                'customer_id':   random.randint(1, 200),
                'customer_name': random.choice(CUSTOMERS),
                'product_sku':   f"SKU-{random.randint(1,100):04d}",
                'product_name':  random.choice(PRODUCTS),
                'quantity':      random.randint(1, 10),
                'unit_price':    round(random.uniform(5000, 55000), 2),
                'order_date':    random_date(),
                'region':        random.choice(REGIONS),
                'status':        random.choice(STATUSES),
            })
    print(f"Generated {rows} rows → {path}")

if __name__ == '__main__':
    generate_csv('data/raw/sales_orders_csv.csv', rows=300)