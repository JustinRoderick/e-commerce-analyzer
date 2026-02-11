from pathlib import Path

from loguru import logger
import pandas as pd
import typer

from e_commerce_analyzer.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR

app = typer.Typer()


@app.command()
def main(
    bronze_dir: Path = INTERIM_DATA_DIR / "bronze",
    processed_dir: Path = PROCESSED_DATA_DIR,
):
    
    processed_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Bronze source: {bronze_dir}")
    logger.info(f"Processed target: {processed_dir}")
    
    df_orders = pd.read_parquet(bronze_dir / "orders.parquet")
    df_order_items = pd.read_parquet(bronze_dir / "order_items.parquet")
    df_order_payments = pd.read_parquet(bronze_dir / "order_payments.parquet")
    df_customers = pd.read_parquet(bronze_dir / "customers.parquet")
    df_products = pd.read_parquet(bronze_dir / "products.parquet")
    df_sellers = pd.read_parquet(bronze_dir / "sellers.parquet")
    df_category_translation = pd.read_parquet(bronze_dir / "category_translation.parquet")
    df_order_reviews = pd.read_parquet(bronze_dir / "order_reviews.parquet")
    
    df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"])
    df_order_reviews["review_creation_date"] = pd.to_datetime(df_order_reviews["review_creation_date"])
    df_order_reviews["review_answer_timestamp"] = pd.to_datetime(df_order_reviews["review_answer_timestamp"])
    
    df_orders = df_orders.astype({
        "order_id": "int64",
        "customer_id": "int64",
        "seller_id": "int64",
        "product_id": "int64",
        "order_purchase_timestamp": "datetime64[ns]",
        "order_approved_at": "datetime64[ns]",
        "order_delivered_carrier_date": "datetime64[ns]",
        "order_delivered_customer_date": "datetime64[ns]",
        "order_estimated_delivery_date": "datetime64[ns]"
    })
    
    df_order_items = df_order_items.astype({
        "order_item_id": "int64",
        "order_id": "int64",
        "product_id": "int64",
        "seller_id": "int64",
        "price": "float64",
        "freight_value": "float64"
    })
    
    df_order_payments = df_order_payments.astype({
        "order_id": "int64",
        "payment_sequential": "int64",
        "payment_type": "object",
        "payment_installments": "int64",
        "payment_value": "float64"
    })
    
    df_customers = df_customers.astype({
        "customer_id": "int64",
        "customer_unique_id": "int64",
        "customer_zip_code_prefix": "int64",
        "customer_city": "object",
        "customer_state": "object"
    })
    
    df_products = df_products.astype({
        "product_id": "int64",
        "product_category_name": "object",
        "product_name_length": "int64",
        "product_description_length": "int64",  
    })
    
    df_sellers = df_sellers.astype({
        "seller_id": "int64",
        "seller_zip_code_prefix": "int64",
        "seller_city": "object",
        "seller_state": "object"
    })
    
    df_category_translation = df_category_translation.astype({
        "product_category_name": "object",
        "product_category_name_english": "object"
    })
    
    df_order_reviews = df_order_reviews.astype({
        "review_id": "int64",
        "order_id": "int64",
        "review_comment_title": "object",
        "review_comment_message": "object",
        "review_creation_date": "datetime64[ns]",
        "review_answer_timestamp": "datetime64[ns]",
        "review_score": "int64"
    })
    
    df_orders = df_orders.drop_duplicates()
    df_order_items = df_order_items.drop_duplicates()
    df_order_payments = df_order_payments.drop_duplicates()
    df_customers = df_customers.drop_duplicates()
    df_products = df_products.drop_duplicates()
    df_sellers = df_sellers.drop_duplicates()
    df_category_translation = df_category_translation.drop_duplicates()
    df_order_reviews = df_order_reviews.drop_duplicates()
    
    df_orders = df_orders.dropna()
    df_order_items = df_order_items.dropna()
    df_order_payments = df_order_payments.dropna()
    df_customers = df_customers.dropna()
    df_products = df_products.dropna()
    df_sellers = df_sellers.dropna()
    df_category_translation = df_category_translation.dropna()
    df_order_reviews = df_order_reviews.dropna()
    
    logger.info(f"Orders deduplicated: before={len(df_orders)}, after={len(df_orders)}")
    logger.info(f"Order items deduplicated: before={len(df_order_items)}, after={len(df_order_items)}")
    logger.info(f"Order payments deduplicated: before={len(df_order_payments)}, after={len(df_order_payments)}")
    logger.info(f"Customers deduplicated: before={len(df_customers)}, after={len(df_customers)}")
    logger.info(f"Products deduplicated: before={len(df_products)}, after={len(df_products)}")
    logger.info(f"Sellers deduplicated: before={len(df_sellers)}, after={len(df_sellers)}")
    logger.info(f"Category translation deduplicated: before={len(df_category_translation)}, after={len(df_category_translation)}")
    logger.info(f"Order reviews deduplicated: before={len(df_order_reviews)}, after={len(df_order_reviews)}")
    
    df_orders.to_parquet(processed_dir / "silver_orders.parquet", index=False)
    df_order_items.to_parquet(processed_dir / "silver_order_items.parquet", index=False)
    df_order_payments.to_parquet(processed_dir / "silver_order_payments.parquet", index=False)
    df_customers.to_parquet(processed_dir / "silver_customers.parquet", index=False)
    df_products.to_parquet(processed_dir / "silver_products.parquet", index=False)
    df_sellers.to_parquet(processed_dir / "silver_sellers.parquet", index=False)
    df_category_translation.to_parquet(processed_dir / "silver_category_translation.parquet", index=False)
    df_order_reviews.to_parquet(processed_dir / "silver_order_reviews.parquet", index=False)
    
    logger.info(f"Silver tables written: {processed_dir}")
    


if __name__ == "__main__":
    app()
