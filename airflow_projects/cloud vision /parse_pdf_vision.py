import google.auth
import google.auth.transport.requests
import google.auth.transport.urllib3
import google.cloud.storage
import google.cloud.vision
import google.cloud.bigquery
import requests
import urllib3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set up default arguments for the DAG
default_args = {
    "owner": "me",
    "start_date": datetime(2022, 1, 1),
}

# Define a function to parse the PDF and load the data into BigQuery
def parse_pdf_and_load_to_bq(**kwargs):
    # Set up the Cloud Storage client and upload the PDF file
    storage_client = google.cloud.storage.Client()
    bucket = storage_client.get_bucket("my-bucket")
    blob = bucket.blob("my-pdf.pdf")
    blob.upload_from_filename("/path/to/my-pdf.pdf")

    # Set up the Cloud Vision client and parse the PDF
    vision_client = google.cloud.vision.ImageAnnotatorClient()
    response = vision_client.document_text_detection(
        image={"source": {"image_uri": f"gs://my-bucket/my-pdf.pdf"}},
        # Enable `include_geometry` to get bounding boxes for each block of text
        # and `include_page_numbers` to get the page numbers for each block
        features=[{"type": "DOCUMENT_TEXT_DETECTION", "include_geometry": True, "include_page_numbers": True}],
    )

    # Extract the text and layout information from the response
    text = ""
    page_numbers = []
    bounding_boxes = []
    for page in response.full_text_annotation.pages:
        for block in page.blocks:
            text += block.text + "\n"
            page_numbers.append(page.page_number)
            bounding_boxes.append(block.bounding_box)

    # Set up the BigQuery client and load the data into a table
    bigquery_client = google.cloud.bigquery.Client()
    dataset_id = "my_dataset"
    table_id = "my_table"
    dataset = bigquery_client.create_dataset(dataset_id)
    table = dataset.table(table_id)
    table.schema = [
        google.cloud.bigquery.SchemaField("text", "STRING"),
        google.cloud.bigquery.SchemaField("page_number", "INTEGER"),
        google.cloud.bigquery.SchemaField("bounding_box", "STRING"),
    ]
    rows = [
        {"text": t, "page_number": pn, "bounding_box": bb}
        for t, pn, bb in zip(text, page_numbers, bounding_boxes)
    ]
    errors = bigquery_client.insert_rows(table, rows)

  # Check for errors when inserting the rows
        if errors:
            raise ValueError(f"Errors occurred when inserting rows: {errors}")
    
    # Create the DAG and add a task to run the PDF parsing and BigQuery loading function
    dag = DAG(
        "parse_pdf_and_load_to_bq",
        default_args=default_args,
        schedule_interval=@daily
        )
    with dag:
        # Parse the PDF and save the extracted text to a temporary file
    parse_pdf_and_load_to_bq = PythonOperator(
        task_id='parse_pdf_and_load_to_bq',
        python_callable=parse_pdf_and_load_to_bq,
        op_args=[pdf_file_path],
        xcom_push=True,
        dag=dag
    )
