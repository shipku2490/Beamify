import time
import requests
import pydantic
import psycopg2
import csv
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import XCom
from pydantic import BaseModel
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule


class BlockchainDataModel(BaseModel):

    # define the fields of the data model using pydantic's field decorators
  availableLiquidity: str
  averageStableBorrowRate: float
  id: str
  lifetimeBorrows: str
  lifetimeCurrentVariableDebt: str
  lifetimeFlashLoanPremium: str
  lifetimeFlashLoans: str
  lifetimeLiquidated: str
  lifetimeLiquidity: str
  lifetimePrincipalStableDebt: str
  lifetimeRepayments: str
  lifetimeReserveFactorAccrued: str
  lifetimeScaledVariableDebt: str
  lifetimeWithdrawals: str
  liquidityIndex: int
  liquidityRate: float
  priceInEth: float
  priceInUsd: float
  stableBorrowRate: float
  timestamp: str
  totalATokenSupply: str
  totalCurrentVariableDebt: str
  totalLiquidity: str
  totalLiquidityAsCollateral: str
  totalPrincipalStableDebt: str
  totalScaledVariableDebt: str
  utilizationRate: float
  variableBorrowIndex: int
  variableBorrowRate: float

class Config:
      arbitrary_types_allowed = True


# Define the default_args dictionary
default_args = {
    'owner': 'xyz@gmail.com',
    'start_date': datetime.datetime(2022, 12, 31),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Removed the secrets 

blockchain_params = [
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/protocol-v2", "reserve_address": AAVEV2_LINK_ETHEREUM, "api_name": "AAVEV2_LINK_ETHEREUM"}, 
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/protocol-v2", "reserve_address": AAVEV2_TUSD_ETHEREUM, "api_name": "AAVEV2_TUSD_ETHEREUM"},
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/protocol-v2", "reserve_address": AAVEV2_WETH_ETHEREUM, "api_name": "AAVEV2_WETH_ETHEREUM"},
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic", "reserve_address": AAVEV2_LINK_MATIC, "api_name": "AAVEV2_LINK_MATIC"}, 
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic", "reserve_address": AAVEV2_WETH_MATIC, "api_name": "AAVEV2_WETH_MATIC"},
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum", "reserve_address": AAVEV3_WETH_ARBITRUM, "api_name": "AAVEV3_WETH_ARBITRUM"}, 
        {"endpoint": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism", "reserve_address": AAVEV3_WETH_OPTIMISM, "api_name": "AAVEV3_WETH_OPTIMISM"}, 
]

list_api_names = ["AAVEV2_LINK_ETHEREUM", "AAVEV2_TUSD_ETHEREUM", "AAVEV2_WETH_ETHEREUM", "AAVEV2_LINK_MATIC", "AAVEV2_WETH_MATIC", "AAVEV3_WETH_ARBITRUM", "AAVEV3_WETH_OPTIMISM"]

# Create a DAG object
dag = DAG(
    'import_blockchain_data_to_pg',
    default_args=default_args,
    schedule_interval="5 * * * *",  # Set the schedule interval to 1 minute
    catchup=False,
)

reserve_params_hist_query = """
  query ReserveParamsHist($reserveID: String) {
    reserve (id: $reserveID) {
      id
      name
      symbol
      paramsHistory(orderBy: timestamp, orderDirection: desc, first: 1000, skip: 0) {
        availableLiquidity
        averageStableBorrowRate
        id
        lifetimeBorrows
        lifetimeCurrentVariableDebt
        lifetimeFlashLoanPremium
        lifetimeFlashLoans
        lifetimeLiquidated
        lifetimeLiquidity
        lifetimePrincipalStableDebt
        lifetimeRepayments
        lifetimeReserveFactorAccrued
        lifetimeScaledVariableDebt
        lifetimeWithdrawals
        liquidityIndex
        liquidityRate
        priceInEth
        priceInUsd
        stableBorrowRate
        timestamp
        totalATokenSupply
        totalCurrentVariableDebt
        totalLiquidity
        totalLiquidityAsCollateral
        totalPrincipalStableDebt
        totalScaledVariableDebt
        utilizationRate
        variableBorrowIndex
        variableBorrowRate
      }
    }
  }
"""
def read_csv(**kwargs):
    # get the file path from the DAG context
    file_path = kwargs['ti'].xcom_pull(task_ids='call_%s_api' % kwargs["item_name"])
    
    # open the file and read the rows into a list
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # parse the rows into instances of BlockchainDataModel
    data = []
    for row in rows:
        try:
            data.append(BlockchainDataModel(**row))
        except ValidationError as e:
            # handle the validation error
            print(e)
    
    # return the parsed data
    return data

def fetch_aave_data(**kwargs):
    # Set the API endpoint and HTTP method
    endpoint = kwargs["endpoint"]
    method = 'POST'
    reserve_address = {'reserveID': kwargs["reserve_address"]}
    query = kwargs["query"]
    data = {'query': query, 'variables': reserve_address}
    output_path = "result_" + kwargs["item_name"] + ".csv"
    response_json = ""
    while True:
        # Send the request to the API
        response = requests.post(endpoint, json=data, stream=True)


        if response.status_code == 200:
          response_json = response.json()
        else:
          raise Exception(f"Request failed. Return code is {response.status_code}.")

        # Print the response
        subgraph_api = [item["endpoint"]]
        subgraph_df = pd.DataFrame([subgraph_api], columns=['subgraphAPI'])
        data1 = [response_json['data']['reserve']['id'],
                 response_json['data']['reserve']['name'],
                 response_json['data']['reserve']['symbol']
                ]
        df1 = pd.DataFrame([data1], columns=['id',
                                             'name',
                                             'symbol'
                                            ]
                           )
        df2 = pd.json_normalize(response_json['data']['reserve']['paramsHistory'])
        df1 = pd.DataFrame(np.repeat(df1.values, len(df2), axis=0))
        subgraph_df = pd.DataFrame(np.repeat(subgraph_df.values, len(df2), axis=0))
        df = pd.concat([subgraph_df, df1, df2], axis=1)

        df.to_csv(output_path)

        kwargs['ti'].xcom_push(key='df_for_%s' % kwargs["item_name"], value=output_path)


def transform_and_aggregate_data(**kwargs):
    reserves_df = pd.DataFrame()
    for item in kwargs["list_api_names"]:
      file_csv = kwargs['task_instance'].xcom_pull(key='df_for_%s' % item)
      df_data = pd.read_csv(file_csv)
      reserves_df = reserves_df.append(df_data)

    reserves_df = (reserves_df
                   .assign(dateTime = reserves_df.timestamp.apply(lambda x: datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S')))
                   .astype({'liquidityRate': 'float',
                            'variableBorrowRate': 'float',
                            'stableBorrowRate': 'float'
                           })
                  )
    
    cols = ['subgraphAPI', 'reserveID', 'name', 'symbol']
    reserves_df.columns = cols + reserves_df.columns.tolist()[len(cols):]
    
    RAY = 10**27 # 10 to the power 27
    SECONDS_PER_YEAR = 31536000
    
    # Deposit and Borrow calculations
    # APY and APR are returned here as decimals, multiply by 100 to get the percents
    reserves_df['depositAPR'] = reserves_df.liquidityRate/RAY
    reserves_df['variableBorrowAPR'] = reserves_df.variableBorrowRate/RAY
    reserves_df['stableBorrowAPR'] = reserves_df.stableBorrowRate/RAY
    
    reserves_df['depositAPY'] = (pow(1 + (reserves_df.depositAPR / SECONDS_PER_YEAR), SECONDS_PER_YEAR)) - 1
    reserves_df['variableBorrowAPY'] = (pow(1 + (reserves_df.variableBorrowAPR / SECONDS_PER_YEAR), SECONDS_PER_YEAR)) - 1
    reserves_df['stableBorrowAPY'] = (pow(1 + (reserves_df.stableBorrowAPR / SECONDS_PER_YEAR), SECONDS_PER_YEAR)) - 1
    
    reserves_df['subgraph'] = reserves_df.subgraphAPI.apply(lambda x: x.rsplit('/', 1)[-1])
    cols_to_move = ['subgraphAPI', 'subgraph']
    reserves_df = reserves_df[cols_to_move + [col for col in reserves_df.columns if col not in cols_to_move]]
    
    reserves_df.to_csv(output_path)

    kwargs['ti'].xcom_push(key='file_path', value=output_path)



def store_data_in_postgres(**kwargs):
    # Retrieve the data from the API
    csv_data = kwargs['task_instance'].xcom_pull(task_ids='transform_data')
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(host='localhost', dbname='test', user='user', password='password')
    # Create a cursor
    cursor = conn.cursor()
    # Parse the CSV data and insert it into the database
    reader = csv.DictReader(csv_data.splitlines())
    for row in reader:
        cursor.execute("INSERT INTO blockchain_data (availableLiquidity, averageStableBorrowRate, id, lifetimeBorrows, lifetimeCurrentVariableDebt, lifetimeFlashLoanPremium, lifetimeFlashLoans, lifetimeLiquidated, lifetimeLiquidity, lifetimePrincipalStableDebt, lifetimeRepayments) VALUES (%s, %s, %s)", (row['availableLiquidity'], row['averageStableBorrowRate'], row['id'], row["lifetimeBorrows"], row["lifetimeCurrentVariableDebt"], row["lifetimeFlashLoanPremium"], row["lifetimeFlashLoans"], row["lifetimeLiquidated"], row["lifetimeLiquidity"], row["lifetimePrincipalStableDebt"], row["lifetimeRepayments"]))
    # Commit the transaction
    conn.commit()
    # Close the cursor and connection
    cursor.close()
    conn.close()


with dag:
  for item in blockchain_params:
      # Define a task using the PythonOperator
      fetch_data_from_api = PythonOperator(
          task_id='call_%s_api' % item["api_name"],
          python_callable=fetch_aave_data,  # The Python function to call
          provide_context=True,  # Whether to pass the context (e.g. task instance, execution date) to the function
          op_kwargs={"endpoint": item["endpoint"],
                     "reserve_address": item["reserve_address"], 
                     "query": reserve_params_hist_query,
                     "item_name": item["api_name"]},  # Keyword arguments to pass to the function (optional)
          dag=dag,
      )


      check_raw_data_quality = PythonOperator(
          task_id='check_%s_raw_data_quality' % item["api_name"],
          python_callable=read_csv,
          op_kwargs = {"item_name": item["api_name"]}
          provide_context=True,
          dag=dag,
      )

      is_data_quality_ok = ShortCircuitOperator(
          task_id='is_%s_data_quality_ok' % item["api_name"],
          python_callable=lambda: True,
          trigger_rule=TriggerRule.ONE_FAILED,
          dag=dag

      )

      fetch_data_from_api >> check_raw_data_quality >> is_data_quality_ok

  transform_data = PythonOperator(
      task_id='transform_data',
      python_callable=transform_and_aggregate_data,  # The Python function to call
      provide_context=True,  # Whether to pass the context (e.g. task instance, execution date) to the function
      op_kwargs={"list_api_names": list_api_names},
      dag=dag,
  )

  store_data_task = PythonOperator(
      task_id='store_data_in_pg',
      python_callable=store_data_in_postgres,
      provide_context=True,
      dag=dag
 )
   
      
  is_data_quality_ok >> transform_data >> store_data_task
