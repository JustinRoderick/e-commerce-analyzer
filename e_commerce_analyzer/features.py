from pathlib import Path

from loguru import logger
import numpy as np
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

    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if col in df_orders.columns:
            df_orders[col] = pd.to_datetime(df_orders[col], errors="coerce")

    if "shipping_limit_date" in df_order_items.columns:
        df_order_items["shipping_limit_date"] = pd.to_datetime(
            df_order_items["shipping_limit_date"], errors="coerce"
        )

    for col in ["review_creation_date", "review_answer_timestamp"]:
        if col in df_order_reviews.columns:
            df_order_reviews[col] = pd.to_datetime(df_order_reviews[col], errors="coerce")
    
    df_orders = df_orders.astype({
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
    })
    
    df_order_items = df_order_items.astype({
        "order_item_id": "string",
        "order_id": "string",
        "product_id": "string",
        "seller_id": "string",
        "price": "float64",
        "freight_value": "float64",
    })
    
    df_order_payments = df_order_payments.astype({
        "order_id": "string",
        "payment_sequential": "Int64",
        "payment_type": "string",
        "payment_installments": "Int64",
        "payment_value": "float64"
    })
    
    df_customers = df_customers.astype({
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string"
    })
    
    df_products = df_products.astype({
        "product_id": "string",
        "product_category_name": "string",
        "product_name_lenght": "Int64",
        "product_description_lenght": "Int64",
        "product_photos_qty": "Int64",
        "product_weight_g": "Int64",
        "product_length_cm": "float64",
        "product_height_cm": "float64",
        "product_width_cm": "float64",
    })
    
    df_sellers = df_sellers.astype({
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string"
    })
    
    df_category_translation = df_category_translation.astype({
        "product_category_name": "string",
        "product_category_name_english": "string"
    })
    
    df_order_reviews = df_order_reviews.astype({
        "review_id": "string",
        "order_id": "string",
        "review_comment_title": "string",
        "review_comment_message": "string",
        "review_score": "Int64"
    })

    df_orders = df_orders.drop_duplicates()
    df_order_items = df_order_items.drop_duplicates()
    df_order_payments = df_order_payments.drop_duplicates()
    df_customers = df_customers.drop_duplicates()
    df_products = df_products.drop_duplicates()
    df_sellers = df_sellers.drop_duplicates()
    df_category_translation = df_category_translation.drop_duplicates()
    df_order_reviews = df_order_reviews.drop_duplicates()
    
    df_orders = df_orders.dropna(subset=["order_id", "customer_id"])
    df_order_items = df_order_items.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id"])
    df_order_payments = df_order_payments.dropna(subset=["order_id"])
    df_customers = df_customers.dropna(subset=["customer_id", "customer_unique_id"])
    df_products = df_products.dropna(subset=["product_id"])
    df_sellers = df_sellers.dropna(subset=["seller_id"])
    df_category_translation = df_category_translation.dropna(subset=["product_category_name"])
    df_order_reviews = df_order_reviews.dropna(subset=["review_id", "order_id"])
    
    df_orders.to_parquet(processed_dir / "silver_orders.parquet", index=False)
    df_order_items.to_parquet(processed_dir / "silver_order_items.parquet", index=False)
    df_order_payments.to_parquet(processed_dir / "silver_order_payments.parquet", index=False)
    df_customers.to_parquet(processed_dir / "silver_customers.parquet", index=False)
    df_products.to_parquet(processed_dir / "silver_products.parquet", index=False)
    df_sellers.to_parquet(processed_dir / "silver_sellers.parquet", index=False)
    df_category_translation.to_parquet(processed_dir / "silver_category_translation.parquet", index=False)
    df_order_reviews.to_parquet(processed_dir / "silver_order_reviews.parquet", index=False)
    
    logger.info(f"Silver tables written: {processed_dir}")
    
    # creates Gold data on orders
    gold = df_orders[
        [
            "order_id",  
            "customer_id",  
            "order_status",  
            "order_purchase_timestamp",  
            "order_approved_at",  
            "order_delivered_carrier_date",  
            "order_delivered_customer_date",  
            "order_estimated_delivery_date",  
        ]
    ].copy()
    
    # Left join customers onto orders
    gold = gold.merge(
        df_customers[
            [
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
                "customer_zip_code_prefix",
            ]
        ],
        on="customer_id",
        how="left",
        validate="m:1",
    )
    
    # create order payment info
    payments_agg = (
        df_order_payments.groupby("order_id", dropna=False)
        .agg(
            total_payment_value=("payment_value", "sum"),
            payment_installments_mean=("payment_installments", "mean"),
            payment_installments_max=("payment_installments", "max"),
            payment_sequential_max=("payment_sequential", "max"),
        )
        .reset_index()
    )

    gold = gold.merge(
        payments_agg,
        on="order_id",
        how="left",
        validate="1:1",
    )
    
    
    
    print(gold.head())
    print(gold.columns)

    
if __name__ == "__main__":
    app()
