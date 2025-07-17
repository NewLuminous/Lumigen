# Lumigen: Declarative ETL Framework

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

**Lumigen is a powerful, configuration-driven ETL framework designed to simplify and accelerate data pipeline development. Define your entire data flow—from source to sink—in a single, expressive YAML file and let Lumigen handle the execution.**

Whether you're moving data between databases, processing streams from Kafka, or transforming files, Lumigen provides a unified interface to connect, transform, and load your data with minimal code.

## Key Features

*   **Declarative Pipelines:** Define complex ETL jobs in a single, easy-to-read YAML file. No boilerplate code required.
*   **Extensible Connectors:** Out-of-the-box support for popular data sources and sinks like Oracle, PostgreSQL, MySQL, Kafka, and more.
*   **Pluggable Execution Engines:** Run your jobs using the best engine for the task. Lumigen supports:
    *   **Standard Python:** For lightweight, single-node processing.
    *   **Apache Spark:** For large-scale distributed data processing.
    *   **Apache SeaTunnel:** For high-performance data integration between a vast array of sources.
*   **Powerful Transformations:** Apply a chain of transformations to your data in-flight. Use built-in transformers (XML/JSON parsing, SQL) or easily add your own.
*   **Configuration-First:** All aspects of the job, from database credentials and Spark settings to transformation logic, are managed in one place.

## How It Works

Lumigen abstracts the entire ETL process into three main stages, all defined in your configuration file:

1.  **Read:** Lumigen connects to your specified `source` (database, message queue, file system) and begins reading data.
2.  **Transform:** The data is passed through a `transformation_pipeline` where each step modifies the data in sequence. This could be anything from parsing JSON to executing complex SQL queries with UDFs.
3.  **Write:** The final, transformed data is loaded into your `sink`, with support for various save modes like `overwrite`, `append`, and `merge`.

Lumigen intelligently selects the right backend runner (Python, Spark, or SeaTunnel) based on your configuration, ensuring optimal performance and resource utilization.

## Example

Here is an example that reads from a PostgreSQL table, transforms the data using SQL to create a new column, and writes the result to a new table.

*For more examples, see the `config-examples/` directory.*

```yaml
source:
  type: postgres
  host: localhost
  port: 5432
  database: pagila
  user: postgres
  password: ********

sink:
  type: postgres
  host: localhost
  port: 5432
  database: postgres
  user: postgres
  password: ********

executor:
  type: spark
  app_name: process_actors_table
  hive_support: false
  config:
    spark.eventLog.enabled: false

transformation_pipeline:
  - SQL:
      query: "SELECT actor_id, CONCAT(UPPER(first_name), ' ', UPPER(last_name)) AS full_name, last_update FROM dual"

job_properties:
  spark_submit_options:
    master: local[*]
  source_table: public.actor
  sink_table: public.actor_transformed
  data_save_mode: OVERWRITE
```

## Getting Started

### Prerequisites

*   Python 3.9+
*   An execution engine environment configured (e.g., local Python with Pandas, or access to a Spark cluster with `SPARK_HOME` set).
*   For SeaTunnel, `SEATUNNEL_HOME` must be set.

### Installation

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/newluminous/lumigen.git
    cd lumigen
    ```

2.  **(Recommended) Create and Activate a Virtual Environment**
    A virtual environment keeps your project's dependencies isolated.
    ```bash
    python3.9 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Lumigen in Editable Mode**
    This command reads the `pyproject.toml` file, installs all required dependencies, and makes the `lumigen` command available in your shell. The `-e` (editable) flag means that changes you make to the source code will be active immediately without reinstalling.
    ```bash
    pip install -e .
    ```

### Environment Configuration

Lumigen uses a `.env` file to load environment-specific variables. This keeps local configuration out of your main YAML files.

1.  **Create a `.env` file** in the project's root directory (`lumigen/`).

2.  **Add your environment variables.** For example, to specify the location of your Spark installation:
    ```dotenv
    # .env
    
    # Path to your Spark installation
    SPARK_HOME=/path/to/your/spark-x.x.x-bin-hadoop3
    ```
    Lumigen will automatically load this file at runtime.

### Running a Job

With your environment configured and the project installed, running a job is simple. Use the `lumigen` command followed by the path to your configuration file.

```bash
lumigen -c /path/to/your/config.yaml
```

Lumigen will parse the config, select the appropriate runner, and execute the job.

## License

Lumigen is licensed under the [Apache License 2.0](LICENSE).