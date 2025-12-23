"""
Data Loader for Cognix V2.

Loads and prepares the source data for analytics.
Run this script to prepare the data from CSV.
"""

import os
import pandas as pd
from loguru import logger


def load_superstore_data(
    input_path: str = "data/superstore.csv",
    output_path: str = "data/raw.parquet"
) -> pd.DataFrame:
    """
    Load Superstore data from CSV and save as Parquet.
    
    Args:
        input_path: Path to source CSV file
        output_path: Path for output Parquet file
        
    Returns:
        Loaded and cleaned DataFrame
    """
    logger.info(f"Loading data from: {input_path}")
    
    # Read CSV with proper encoding
    df = pd.read_csv(input_path, encoding="latin1")
    
    # Select and rename relevant columns
    columns_to_keep = [
        "Order Date",
        "Region",
        "Category",
        "Sub-Category",
        "Sales",
        "Profit",
        "Quantity",
    ]
    
    df = df[columns_to_keep]
    
    # Rename columns to snake_case
    df = df.rename(columns={
        "Order Date": "order_date",
        "Sub-Category": "sub_category",
        "Region": "region",
        "Category": "category",
        "Sales": "sales",
        "Profit": "profit",
        "Quantity": "quantity",
    })
    
    # Convert date column
    df["order_date"] = pd.to_datetime(df["order_date"])
    
    # Ensure proper data types
    df["sales"] = df["sales"].astype(float)
    df["profit"] = df["profit"].astype(float)
    df["quantity"] = df["quantity"].astype(int)
    df["region"] = df["region"].astype(str)
    df["category"] = df["category"].astype(str)
    df["sub_category"] = df["sub_category"].astype(str)
    
    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    
    logger.info(f"Saved {len(df)} rows to: {output_path}")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Date range: {df['order_date'].min()} to {df['order_date'].max()}")
    logger.info(f"Regions: {df['region'].unique().tolist()}")
    logger.info(f"Categories: {df['category'].unique().tolist()}")
    
    return df


def get_data_summary(parquet_path: str = "data/raw.parquet") -> dict:
    """Get summary statistics for the data."""
    df = pd.read_parquet(parquet_path)
    
    return {
        "row_count": len(df),
        "columns": df.columns.tolist(),
        "date_range": {
            "min": str(df["order_date"].min()),
            "max": str(df["order_date"].max())
        },
        "regions": df["region"].unique().tolist(),
        "categories": df["category"].unique().tolist(),
        "sub_categories": df["sub_category"].unique().tolist(),
        "total_sales": float(df["sales"].sum()),
        "total_profit": float(df["profit"].sum()),
        "total_quantity": int(df["quantity"].sum())
    }


if __name__ == "__main__":
    import sys
    
    # Check if source file exists
    source_path = "data/superstore.csv"
    
    # Also check in PERSON M's folder
    if not os.path.exists(source_path):
        alt_path = "../PERSON M/data/superstore.csv"
        if os.path.exists(alt_path):
            source_path = alt_path
            logger.info(f"Using data from: {alt_path}")
    
    if not os.path.exists(source_path):
        logger.error(f"Source file not found: {source_path}")
        logger.info("Please place superstore.csv in the data/ folder")
        sys.exit(1)
    
    # Load and save data
    df = load_superstore_data(source_path)
    
    # Print summary
    summary = get_data_summary()
    print("\n" + "="*50)
    print("DATA SUMMARY")
    print("="*50)
    print(f"Rows: {summary['row_count']:,}")
    print(f"Date Range: {summary['date_range']['min']} to {summary['date_range']['max']}")
    print(f"Regions: {summary['regions']}")
    print(f"Categories: {summary['categories']}")
    print(f"Total Sales: ${summary['total_sales']:,.2f}")
    print(f"Total Profit: ${summary['total_profit']:,.2f}")
    print("="*50)

