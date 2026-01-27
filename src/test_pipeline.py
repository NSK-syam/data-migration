import pandas as pd
from datetime import datetime, timedelta
import random
from s3_exporter import S3Exporter

def generate_sample_orders(num_rows: int = 100) -> pd.DataFrame:
    """Generate sample order data."""
    customers = [f"CUST_{i:04d}" for i in range(1, 21)]
    products = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]
    
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_rows):
        data.append({
            "order_id": f"ORD_{i+1:06d}",
            "customer_id": random.choice(customers),
            "product": random.choice(products),
            "quantity": random.randint(1, 10),
            "price": round(random.uniform(10.0, 500.0), 2),
            "created_at": base_date + timedelta(days=random.randint(0, 30)),
        })
    
    return pd.DataFrame(data)


def generate_sample_customers(num_rows: int = 20) -> pd.DataFrame:
    """Generate sample customer data."""
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    
    data = []
    for i in range(1, num_rows + 1):
        data.append({
            "customer_id": f"CUST_{i:04d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "city": random.choice(cities),
            "created_at": datetime.now() - timedelta(days=random.randint(30, 365)),
        })
    
    return pd.DataFrame(data)


def test_local_export():
    """Test exporting sample data to local files."""
    print("Generating sample data...")
    orders_df = generate_sample_orders(100)
    customers_df = generate_sample_customers(20)
    
    print(f"\nOrders sample ({len(orders_df)} rows):")
    print(orders_df.head())
    
    print(f"\nCustomers sample ({len(customers_df)} rows):")
    print(customers_df.head())
    
    # Save locally for inspection
    orders_df.to_parquet("../test_orders.parquet", index=False)
    orders_df.to_csv("../test_orders.csv", index=False)
    customers_df.to_parquet("../test_customers.parquet", index=False)
    
    print("\nLocal test files created:")
    print("  - test_orders.parquet")
    print("  - test_orders.csv")
    print("  - test_customers.parquet")


def test_s3_export():
    """Test exporting sample data to S3."""
    print("Generating sample data...")
    orders_df = generate_sample_orders(100)
    customers_df = generate_sample_customers(20)
    
    print("Connecting to S3...")
    s3 = S3Exporter()
    
    print("Uploading orders (parquet)...")
    result1 = s3.export_dataframe(orders_df, "test/orders.parquet", "parquet")
    print(f"  ✓ {result1}")
    
    print("Uploading orders (csv)...")
    result2 = s3.export_dataframe(orders_df, "test/orders.csv", "csv")
    print(f"  ✓ {result2}")
    
    print("Uploading customers (parquet)...")
    result3 = s3.export_dataframe(customers_df, "test/customers.parquet", "parquet")
    print(f"  ✓ {result3}")
    
    print("\nS3 export complete!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--s3":
        test_s3_export()
    else:
        test_local_export()
        print("\nTo test S3 upload, run: python test_pipeline.py --s3")
