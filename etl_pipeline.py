"""ETL Framework — Amman Digital Market (Tier 1 + Tier 2 + Tier 3)
Configurable + Logged + Reusable ETL Pipeline
"""

from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
from datetime import datetime
import logging

# ====================== SETUP LOGGING (Tier 3) ======================
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger("etl_framework")

logger = setup_logging()


# ====================== LOAD CONFIG (Tier 3) ======================
def load_config(config_path: str = "configs/customer_analytics.json"):
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info(f"Loaded config → Pipeline: {config['pipeline_name']} | Table: {config['output_table']}")
    return config


# ====================== EXTRACT ======================
def extract(engine, config, incremental: bool = True):
    logger.info("Starting data extraction...")

    last_ts = get_last_etl_timestamp(engine, config['pipeline_name']) if incremental else None

    if last_ts:
        logger.info(f"Incremental mode: orders after {last_ts.date()}")
        orders_query = f"""
            SELECT * FROM orders 
            WHERE order_date > '{last_ts}' 
            ORDER BY order_date
        """
        orders = pd.read_sql(orders_query, engine)
    else:
        logger.info("Full load mode (first run)")
        orders = pd.read_sql_table("orders", engine)

    customers = pd.read_sql_table("customers", engine)
    products = pd.read_sql_table("products", engine)
    order_items = pd.read_sql_table("order_items", engine)

    logger.info(f"Extracted: {len(customers)} customers, {len(products)} products, "
                f"{len(orders)} orders, {len(order_items)} order_items")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


# ====================== TRANSFORM (Tier 1) ======================
def transform(data_dict, config):
    logger.info("Starting transformation...")

    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    if len(orders) == 0:
        logger.warning("No new orders found in incremental load. Returning empty summary.")
        empty_df = pd.DataFrame(columns=[
            'customer_id', 'customer_name', 'total_orders', 'total_revenue',
            'avg_order_value', 'top_category', 'is_outlier', 'z_score'
        ])
        return empty_df

    merged = pd.merge(order_items, orders, on="order_id", how="inner")
    merged = pd.merge(merged, products, on="product_id", how="inner")
    merged = pd.merge(merged, customers[["customer_id", "customer_name"]], on="customer_id", how="inner")

    logger.info(f"After joining: {len(merged)} rows")

    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    merged = merged[merged["status"] != "cancelled"].copy()
    merged = merged[merged["quantity"] <= 100].copy()

    logger.info(f"After filtering: {len(merged)} rows")

    customer_summary = merged.groupby(["customer_id", "customer_name"]).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    ).reset_index()

    customer_summary["avg_order_value"] = customer_summary["total_revenue"] / customer_summary["total_orders"]

    if "category" in merged.columns:
        cat_rev = merged.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
        top_cat = cat_rev.loc[cat_rev.groupby("customer_id")["line_total"].idxmax()]
        top_cat = top_cat[["customer_id", "category"]].rename(columns={"category": "top_category"})
        customer_summary = pd.merge(customer_summary, top_cat, on="customer_id", how="left")
    else:
        customer_summary["top_category"] = "Unknown"

    customer_summary[["total_revenue", "avg_order_value"]] = customer_summary[["total_revenue", "avg_order_value"]].round(2)

    # Outlier Detection
    if not customer_summary.empty:
        mean_rev = customer_summary['total_revenue'].mean()
        std_rev = customer_summary['total_revenue'].std(ddof=0)
        customer_summary['z_score'] = (customer_summary['total_revenue'] - mean_rev) / std_rev if std_rev != 0 else 0
        customer_summary['is_outlier'] = customer_summary['z_score'].abs() > 3
    else:
        customer_summary['is_outlier'] = False
        customer_summary['z_score'] = 0.0

    columns_order = ['customer_id', 'customer_name', 'total_orders', 'total_revenue',
                     'avg_order_value', 'top_category', 'is_outlier', 'z_score']
    customer_summary = customer_summary[columns_order]

    logger.info(f"Transform completed: {len(customer_summary)} customers | Outliers: {customer_summary['is_outlier'].sum()}")
    return customer_summary


# ====================== VALIDATE ======================
def validate(df):
    logger.info("Running data quality validation...")

    if df.empty:
        logger.warning("Empty DataFrame - skipping detailed validation")
        return {"empty_data": True}

    no_nulls = not (df["customer_id"].isnull().any() or df["customer_name"].isnull().any())
    revenue_positive = (df["total_revenue"] > 0).all()
    no_duplicates = not df["customer_id"].duplicated().any()
    orders_positive = (df["total_orders"] > 0).all()

    logger.info(f"No nulls → {'PASS' if no_nulls else 'FAIL'}")
    logger.info(f"Revenue > 0 → {'PASS' if revenue_positive else 'FAIL'}")
    logger.info(f"No duplicates → {'PASS' if no_duplicates else 'FAIL'}")
    logger.info(f"Orders > 0 → {'PASS' if orders_positive else 'FAIL'}")

    if not (no_nulls and revenue_positive and no_duplicates and orders_positive):
        logger.error("Critical validation failed!")
        raise ValueError("Critical data quality checks failed!")

    logger.info("All validation checks PASSED ✓")
    return {"no_nulls": no_nulls, "revenue_positive": revenue_positive,
            "no_duplicates": no_duplicates, "orders_positive": orders_positive}


# ====================== LOAD ======================
def load(df, engine, config):
    csv_path = config.get("csv_path", f"output/{config['output_table']}.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df.to_sql(config["output_table"], engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    logger.info(f"Loaded {len(df)} rows to table '{config['output_table']}'")
    logger.info(f"Saved CSV → {csv_path}")


# ====================== QUALITY REPORT ======================
def generate_quality_report(df, validation_results, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    
    outliers = []
    if not df.empty and 'is_outlier' in df.columns:
        outliers = df[df['is_outlier'] == True][['customer_id', 'customer_name', 'total_revenue', 'z_score']].round(2).to_dict('records')

    report = {
        "etl_run_timestamp": datetime.now().isoformat(),
        "pipeline": "customer_analytics",
        "total_records_checked": len(df),
        "checks": {"passed": 4, "failed": 0, "outlier_count": len(outliers)},
        "flagged_outliers": outliers
    }

    report_path = os.path.join(output_dir, "quality_report.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=4)

    logger.info(f"Quality report generated → {report_path} ({len(outliers)} outliers)")


# ====================== TIER 2 HELPERS ======================
def init_etl_metadata(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS etl_metadata (
        run_id SERIAL PRIMARY KEY,
        pipeline_name VARCHAR(50) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        rows_extracted INTEGER DEFAULT 0,
        rows_loaded INTEGER DEFAULT 0,
        status VARCHAR(20),
        last_order_date TIMESTAMP,
        notes TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("etl_metadata table is ready")


def get_last_etl_timestamp(engine, pipeline_name):
    """Fixed version - using string formatting instead of named params"""
    query = f"""
        SELECT MAX(last_order_date) as last_ts
        FROM etl_metadata 
        WHERE pipeline_name = '{pipeline_name}' AND status = 'SUCCESS'
    """
    result = pd.read_sql(query, engine)
    return result.iloc[0]['last_ts'] if not result.empty and pd.notna(result.iloc[0]['last_ts']) else None


def log_etl_run(engine, pipeline_name, start_time, rows_extracted, rows_loaded, status="SUCCESS", notes=""):
    end_time = datetime.now()
    last_order_date = None
    try:
        result = pd.read_sql("SELECT MAX(order_date) as max_date FROM orders", engine)
        last_order_date = result.iloc[0]['max_date']
    except:
        pass

    insert_sql = """
    INSERT INTO etl_metadata 
    (pipeline_name, start_time, end_time, rows_extracted, rows_loaded, status, last_order_date, notes)
    VALUES (:pipeline_name, :start_time, :end_time, :rows_extracted, :rows_loaded, :status, :last_order_date, :notes)
    """
    params = {
        "pipeline_name": pipeline_name,
        "start_time": start_time,
        "end_time": end_time,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "status": status,
        "last_order_date": last_order_date,
        "notes": notes
    }
    with engine.begin() as conn:
        conn.execute(text(insert_sql), params)
    logger.info(f"ETL run logged | Pipeline: {pipeline_name} | Status: {status}")


# ====================== MAIN FRAMEWORK (Tier 3) ======================
def run_etl(config_path: str = "configs/customer_analytics.json", incremental: bool = True):
    start_time = datetime.now()
    config = load_config(config_path)
    pipeline_name = config["pipeline_name"]

    logger.info(f"🚀 Starting {pipeline_name} pipeline (Incremental: {incremental})")

    engine = create_engine("postgresql+psycopg://postgres:postgres@localhost:5433/amman_market")

    init_etl_metadata(engine)

    data_dict = extract(engine, config, incremental)
    rows_extracted = len(data_dict["orders"])

    df_transformed = transform(data_dict, config)
    validation_results = validate(df_transformed)

    load(df_transformed, engine, config)

    if not df_transformed.empty:
        generate_quality_report(df_transformed, validation_results)
    else:
        logger.info("No data processed - skipping quality report")

    log_etl_run(engine, pipeline_name, start_time, rows_extracted, len(df_transformed), "SUCCESS")

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ {pipeline_name} completed successfully in {duration:.2f} seconds!\n")

if __name__ == "__main__":
    run_etl("configs/customer_analytics.json", incremental=False)

    # run_etl("configs/customer_analytics.json", incremental=True) 
      
    # Second pipeline to demonstrate Tier 3 Framework
    run_etl("configs/product_analytics.json", incremental=False)
