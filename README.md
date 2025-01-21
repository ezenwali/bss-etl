# **Bike Sharing Data ETL Pipeline with Storage Backup**

## **Project Overview**

This project automates the ingestion, cleaning, and loading of monthly bike-sharing data into BigQuery. Additionally, a cleaned version of the dataset is stored in a designated Google Cloud Storage bucket to support machine learning model development. The pipeline is triggered whenever a new CSV file is uploaded to a Cloud Storage bucket. The project also integrates **Cloud Build** to automatically deploy updates to the Cloud Function whenever changes are merged into the `master` branch.

## **Workflow**

1. A monthly CSV file is uploaded to a Cloud Storage bucket.
2. A Cloud Function is triggered by the file upload event.
3. The Python script:
   * Downloads the file from Cloud Storage.
   * Cleans and validates the data (e.g., handles missing values, formats dates, and filters invalid rows).
   * Saves the cleaned version of the file back to a  **cleaned data storage bucket** .
   * Uploads the cleaned data to a BigQuery table.
4. The cleaned data is now available in both BigQuery (for analytics) and Cloud Storage (for machine learning model development).

## **Automated Deployment with Cloud Build**

Whenever changes are merged into the `master` branch, **Cloud Build** automatically redeploys the updated Cloud Function.

## **Prerequisites**

1. **Google Cloud Platform Services** :

* Cloud Storage
* Cloud Functions
* BigQuery
* Cloud Build

2. **Service Account** :

* Grant the service account the following roles:
  * `Storage Object Viewer` (for accessing Cloud Storage files).
  * `Storage Object Creator` (for saving cleaned data).
  * `BigQuery Data Editor` (for writing to BigQuery).
  * `Cloud Functions Admin` and `Service Account User` (for Cloud Build to deploy the Cloud Function).
