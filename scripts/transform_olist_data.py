import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

storage_client = storage.Client()
bq_client = bigquery.Client()

# Configuration
PROJECT_ID = 'olist-etl-pipeline-481006'
RAW_BUCKET = 'olist-raw-data-pas'
PROCESSED_BUCKET = 'olist-processed-data-pas'
DATASET_ID = 'olist_warehouse_us'

def load_csv_from_gcs(bucket_name, file_name):
    """Load CSV from GCP"""
    print(f"loading {file_name} from {bucket_name}...")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    return pd.read_csv(pd.io.common.StringIO(data))
def upload_to_gcs(df, bucket_name, file_name):
    """Upload df to GCP as CSV"""
    print(f"uploading {file_name} to {bucket_name}...")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def load_to_bigquery(df, table_name):
    """Load df to bigquery"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"loading to BigQuery table: {table_id}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}.")

def transform_orders():
    """Transform orders data"""
    print("\n=== Processing Orders ===")
    df = load_csv_from_gcs(RAW_BUCKET, 'olist_orders_dataset.csv')

    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])

    upload_to_gcs(df, PROCESSED_BUCKET, 'orders_processed.csv')
    load_to_bigquery(df, 'orders')

    return df

def transform_order_items():
    """Transform order items data"""
    print("\n=== Processing Order Items ===")
    df = load_csv_from_gcs(RAW_BUCKET, 'olist_order_items_dataset.csv')
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
    upload_to_gcs(df, PROCESSED_BUCKET, 'orders_items_processed.csv')

    load_to_bigquery(df, 'order_items')
    return df

def transform_reviews():
    """Transform reviews data"""
    print("\n=== Processing reviews data(unstructured text) ===")
    df = load_csv_from_gcs(RAW_BUCKET, 'olist_order_reviews_dataset.csv')

    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')
    df['sentiment'] = df['review_score'].apply(
        lambda x: 'positive' if x >= 4 else 'negative' if x <= 2 else 'neutral'
    )

    df['title_length'] = df['review_comment_title'].str.len()
    df['message_length'] = df['review_comment_message'].str.len()

    #convert timestamps.
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'])

    #save processed data
    upload_to_gcs(df, PROCESSED_BUCKET, 'reviews_processed.csv')
    load_to_bigquery(df, 'reviews')

    return df


def transform_products():
    """Transform products data"""
    print("\n=== Processing products ===")
    df = load_csv_from_gcs(RAW_BUCKET, 'olist_products_dataset.csv')

    # Fill missing values
    df['product_category_name'] = df['product_category_name'].fillna('unknown')

    df = df.rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    })

    # Then fill missing values
    df['product_name_length'] = df['product_name_length'].fillna(0)
    df['product_description_length'] = df['product_description_length'].fillna(0)
    df['product_category_name'] = df['product_category_name'].str.replace('_', ' ')

    # Save processed data
    upload_to_gcs(df, PROCESSED_BUCKET, 'products_processed.csv')
    load_to_bigquery(df, 'products')

    return df


def main():
    """Main ETL pipeline"""
    print("=" * 50)
    print("Starting Olist ETL Pipeline")
    print("=" * 50)

    # Transform all datasets
    transform_orders()
    transform_order_items()
    transform_reviews()
    transform_products()

    print("\n" + "=" * 50)
    print("✓ ETL pipeline completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()