import pandas as pd
from bike_sharing_etl.bss import BikeSharingData
from google.cloud import storage, bigquery
import os
import functions_framework


# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# BigQuery dataset and table
BQ_DATASET = "bike_sharing"
BQ_TABLE = "mob_bike"
CLEANED_DATA_BUCKET = "ml_bss_data"


def upload_to_bigquery(df, dataset_id, table_id):
    """
    Upload cleaned DataFrame to BigQuery table.
    """
    try:
        # Log the DataFrame schema and data types
        print(f"Uploading DataFrame to BigQuery...")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame dtypes: {df.info()}")
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)

        print("ref table", table_ref)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, autodetect=True
        )
        print("job_config", job_config)

        # Load the data
        job = bigquery_client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )

        job.result()  # Wait for the job to complete

        print(f"Loaded {len(df)} rows into {dataset_id}:{table_id}.")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")
        raise


def save_cleaned_data_to_storage(df, local_cleaned_file_path):
    """
    Concatenate the newly cleaned DataFrame with the previously saved cleaned data
    in the Cloud Storage bucket and save the result.
    """
    try:
        # Temporary file paths
        merged_file_path = f"/tmp/merged_cleaned_data.csv"

        # Cloud Storage bucket and blob path
        bucket = storage_client.bucket(CLEANED_DATA_BUCKET)
        blob_path = "cleaned_data/merged_cleaned_data.csv"
        blob = bucket.blob(blob_path)

        # Check if the cleaned data file already exists in the bucket
        if blob.exists():
            print("Existing cleaned data file found. Downloading for concatenation...")
            blob.download_to_filename(merged_file_path)

            # Load the existing cleaned data
            existing_df = pd.read_csv(merged_file_path)
            print(f"Existing cleaned data loaded with {len(existing_df)} rows.")

            # Concatenate the new data with the existing data
            merged_df = pd.concat([existing_df, df], ignore_index=True)

            # Remove duplicates (optional, based on your use case)
            merged_df.drop_duplicates(inplace=True)
        else:
            print("No existing cleaned data found. Creating a new file...")
            merged_df = df

        # Save the merged DataFrame locally
        merged_df.to_csv(merged_file_path, index=False)

        # Upload the merged file back to Cloud Storage
        blob.upload_from_filename(merged_file_path)

        print(f"Cleaned data saved to storage: {CLEANED_DATA_BUCKET}/{blob_path}")

        # Remove temporary files
        os.remove(local_cleaned_file_path)
        os.remove(merged_file_path)

    except Exception as e:
        print(f"Error saving cleaned data to storage: {e}")
        raise


@functions_framework.cloud_event
def process_csv(cloud_event):
    """
    Triggered by a change to a Cloud Storage bucket.
    """
    try:
        data = cloud_event.data

        bucket_name = data["bucket"]
        file_name = data["name"]

        # Log event context
        print(f"Processing file: {file_name} from bucket: {bucket_name}")

        # Download the file
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        local_file_path = f"/tmp/{file_name}"
        blob.download_to_filename(local_file_path)

        # Process data
        bss_data = BikeSharingData(local_file_path)
        processed_data = bss_data.data

        # Upload data to BigQuery
        upload_to_bigquery(processed_data, BQ_DATASET, BQ_TABLE)

        # Save cleaned data to Cloud Storage for ML
        # save_cleaned_data_to_storage(processed_data, local_cleaned_file_path)

    except Exception as e:
        print(f"Error processing file {file_name} from bucket {bucket_name}: {e}")
        raise


[
    "",
    "",
    "bike_id",
    "dep_station_label",
    "ret_station_label",
    "formula_label",
    "formula_label",
    "formula_label",
    "formula_label",
    "covered_distance",
    "duration",
    "is_ebike",
]
