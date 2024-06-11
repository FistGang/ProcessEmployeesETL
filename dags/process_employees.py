import datetime
import pendulum
import os
import requests
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd


def validate_data(file_path):
    """
    Validate the CSV file to ensure it has the correct headers.

    Args:
        file_path (str): The path to the CSV file to be validated.

    Raises:
        ValueError: If the CSV file headers do not match the expected format.
    """
    with open(file_path, "r") as file:
        headers = file.readline().strip().split(",")
        if headers != [
            "Serial Number",
            "Company Name",
            "Employee Markme",
            "Description",
            "Leave",
        ]:
            raise ValueError("CSV file header does not match expected format")


@dag(
    dag_id="process_employees_etl_pipeline",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    """
    Define the DAG for processing employee data.

    The DAG performs the following steps:
    1. Creates the `employees` table if it does not exist.
    2. Creates a temporary `employees_temp` table.
    3. Downloads and validates the employee data CSV file.
    4. Transforms the data and saves it to a new CSV file.
    5. Loads the transformed data into the `employees_temp` table.
    6. Merges the data from `employees_temp` into the `employees` table.
    """
    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER,
                "Processed Timestamp" TIMESTAMP,
                "Leave Category" TEXT
            );""",
    )

    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="pg_conn",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER,
                "Processed Timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                "Leave Category" TEXT
            );""",
    )

    @task
    def get_data():
        """
        Download the employee data CSV file and validate its format.

        Returns:
            str: The path to the downloaded CSV file.
        """
        data_path = "/opt/airflow/data/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        response = requests.get(url)

        with open(data_path, "w") as file:
            file.write(response.text)

        validate_data(data_path)

        return data_path

    @task
    def transform_data(data_path: str):
        """
        Transform the employee data by adding timestamps, formatting columns,
        and categorizing leave.

        Args:
            data_path (str): The path to the input CSV file.

        Returns:
            str: The path to the transformed CSV file.
        """
        df = pd.read_csv(data_path)

        current_time_utc = pd.Timestamp.now(tz="UTC")
        current_time_hanoi = current_time_utc.tz_convert("Asia/Ho_Chi_Minh")
        formatted_timestamp = current_time_hanoi.strftime("%d-%m-%Y %H:%M:%S")
        df["Processed Timestamp"] = formatted_timestamp
        df["Company Name"] = df["Company Name"].str.title()
        df["Description"] = df["Description"].str.capitalize()
        df["Leave Category"] = pd.cut(
            df["Leave"], bins=[-1, 0, 2, float("inf")], labels=["Low", "Medium", "High"]
        )

        transformed_data_path = "/opt/airflow/data/transformed_employees.csv"
        df.to_csv(transformed_data_path, index=False)

        return transformed_data_path

    @task
    def load_data(transformed_data_path: str):
        """
        Load the transformed data into the `employees_temp` table in the PostgreSQL database.

        Args:
            transformed_data_path (str): The path to the transformed CSV file.
        """
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(transformed_data_path, "r") as file:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        """
        Merge data from the `employees_temp` table into the `employees` table.

        Raises:
            Exception: If there is an error during the merge process.
        """
        query = """
            INSERT INTO employees
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM employees_temp
            ) t
            ON CONFLICT ("Serial Number") DO UPDATE
            SET
              "Employee Markme" = excluded."Employee Markme",
              "Description" = excluded."Description",
              "Leave" = excluded."Leave",
              "Processed Timestamp" = excluded."Processed Timestamp",
              "Leave Category" = excluded."Leave Category"
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            logging.info("Data merged successfully")
            return 0
        except Exception as e:
            logging.error(f"Error merging data: {e}")
            raise

    # Set task dependencies
    create_employees_table >> create_employees_temp_table
    data_path = get_data()
    transformed_data_path = transform_data(data_path)
    load_data(transformed_data_path) >> merge_data()


dag = ProcessEmployees()
