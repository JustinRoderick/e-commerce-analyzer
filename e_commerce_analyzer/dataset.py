from pathlib import Path

from loguru import logger
import pandas as pd
from tqdm import tqdm
import typer

from e_commerce_analyzer.config import INTERIM_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

OLIST_TABLES: dict[str, str] = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


@app.command()
def main(
    raw_dir: Path = RAW_DATA_DIR / "archive",
    bronze_dir: Path = INTERIM_DATA_DIR / "bronze",
    fmt: str = "parquet",
):

    bronze_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reading raw tables from: {raw_dir}")
    logger.info(f"Writing bronze tables to: {bronze_dir} (fmt={fmt})")

    for table_name, filename in tqdm(OLIST_TABLES.items(), total=len(OLIST_TABLES)):
        src = raw_dir / filename
        if not src.exists():
            raise FileNotFoundError(
                f"Missing expected raw file for table={table_name}: {src}.\n"
                "If you moved files, update OLIST_TABLES or pass --raw-dir."
            )

        df = pd.read_csv(src)

        df.insert(0, "_source_file", filename)

        if fmt.lower() == "parquet":
            try:
                out = bronze_dir / f"{table_name}.parquet"
                df.to_parquet(out, index=False)
            except Exception as e:
                logger.warning(
                    "Parquet write failed (missing engine like pyarrow?). "
                    "Falling back to CSV. Error was: {}",
                    repr(e),
                )
                out = bronze_dir / f"{table_name}.csv"
                df.to_csv(out, index=False)
        else:
            out = bronze_dir / f"{table_name}.csv"
            df.to_csv(out, index=False)

        logger.info(f"Wrote bronze table {table_name}: {out} (rows={len(df):,})")

    logger.success("Bronze ingestion complete.")


if __name__ == "__main__":
    app()