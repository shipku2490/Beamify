# Approach

This is an ETL data pipeline that takes the raw data from the Google Cloud Storage and perform transformation on specific columns of the files using the Apache Beam (Dataflow as a runner)
Dataflow is the managed no-ops service in the Google Cloud that spins up the cluster for you and scale, de-scale the cluster based on the volume of the data. The cluster is self-managed so no manual intervention is required. In the process we are doing the following 
1. Extract the files from GCS.
2. Run the python Apache Beam code.
3. Export the transformed data to GCS/BigQuery

# Pros

1. Self-managed service on Google Cloud
2. Extensive functions in Apache Beam for transformation
3. Auto-scalable

# Cons

1. Expensive in terms of Cost
2. If the data is not too huge, then it is unnecessary cost and over-engineering
3. Best Suitable for Streaming Data and not Batch Data



# Instructions to Run on Local Machine (Direct Runner)

1. Install Apache Beam locally in the python environment
```bash
pip install apache_beam[gcp]
```

2. Run the code with below command
```bash
python l1_transform.py --input sample_data.csv --output outputs
python l2_transform.py --input l1_data.csv --output outputs
```

3. Get the output as CSV/JSON locally using below 
```bash
more outputs*
```


# To run on the Google Cloud Manually (Dataflow Runner)

```bash
1. Run the below command 
python -m apache_beam.examples.wordcount \
    --region DATAFLOW_REGION \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://STORAGE_BUCKET/results/outputs \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --temp_location gs://STORAGE_BUCKET/tmp/
    ```

You can also directly export the final results to BigQuery by changing to the below code

```bash
beam.io.Write(beam.io.WriteToBigQuery(
                     table = 'TABLE',
                     dataset = 'DATASET',
                     project = 'PROJECT',
                     schema = TABLE_SCHEMA))
                 )
```

# To run the dataflow job automatically, you can also use Airflow/Composer on Google Cloud and create a DAG with below task 

```python
start_python_pipeline_dataflow_runner = BeamRunPythonPipelineOperator(
    task_id="start_python_pipeline_dataflow_runner",
    runner="DataflowRunner",
    py_file=GCS_PYTHON,
    pipeline_options={
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
        'output': GCS_OUTPUT,
    },
    py_options=[],
    py_requirements=['apache-beam[gcp]==2.26.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(
        job_name='{{task.task_id}}', project_id=GCP_PROJECT_ID, location="us-central1"
    ),
)