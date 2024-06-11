# Process Employees ETL Pipeline

An Apache Airflow ETL pipeline to process employee data by downloading, validating, transforming, and loading it into a PostgreSQL database.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [ETL Pipeline Overview](#etl-pipeline-overview)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

- **Python 3.9+**
- **Apache Airflow 2.8+**

## Installation

1. **Clone the repository**:

   ```sh
   git clone https://github.com/FistGang/ProcessEmployeesETL.git
   cd ProcessEmployeeETL.git
   ```

2. **Set up a virtual environment** (optional):

   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

4. **Configure Airflow**:
   - Initialize the Airflow database:

     ```sh
     airflow db init
     ```

   - Create a PostgreSQL connection in Airflow with ID `pg_conn`.
	    - Connection Id: `pg_conn`
	    - Connection Type: postgres
	    - Host: postgres
	    - Schema: airflow
	    - Login: airflow
	    - Password: airflow
	    - Port: 5432

5. **Start Airflow**:

   ```sh
   airflow webserver --port 8080
   airflow scheduler
   ```

6. **Deploy the DAG**:
   - Place `process_employees.py` in your Airflow DAGs folder.

## Using Containers

```bash
# Download the docker-compose.yaml file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Make expected directories and set an expected environment variable
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize the database (optional)
docker-compose up airflow-init

# Start up all services
docker-compose up
```

Then create a PostgreSQL connection in Airflow with ID `pg_conn` .

## Usage

1. **Trigger the DAG**:
   - Open the Airflow web UI at `http://localhost:8080`.
   - Activate and manually trigger the `process_employees_etl_pipeline` DAG.

2. **Monitor**:
   - Use the Airflow UI to monitor DAG execution and check logs for errors.

## ETL Pipeline Overview

1. **Create Tables**:
   - `employees`: Main table.
   - `employees_temp`: Temporary table for data processing.

2. **Get Data**:
   - Download and validate the employee CSV file.

3. **Transform Data**:
   - Add a processed timestamp, format columns, and categorize leave.

4. **Load Data**:
   - Load transformed data into `employees_temp`.

5. **Merge Data**:
   - Merge data from `employees_temp` into `employees`.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

For questions or issues, please open an issue on GitHub.

