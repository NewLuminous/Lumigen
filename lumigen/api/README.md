# Lumigen Job Runner API

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**The Lumigen Job Runner API provides a powerful, RESTful interface for submitting, managing, and monitoring ETL jobs within the Lumigen framework.**

This API allows you to programmatically control every aspect of the job lifecycle, from dynamically submitting new pipelines to previewing data transformations and retrieving logs from running jobs. It's built with FastAPI for high performance and includes interactive documentation via Swagger UI.

## Key Features

*   **Remote Job Submission:** Submit entire Lumigen YAML configurations as a JSON payload to launch new jobs.
*   **Lifecycle Management:** Programmatically start, stop, and delete jobs.
*   **Real-time Monitoring:** Get the current status, detailed info, and resource metrics (CPU/Memory) for any job.
*   **Live Data Previews:** Securely preview data transformations and schema changes *before* running a full job, without writing any data to the sink.
*   **Log Streaming:** Fetch logs directly from running or completed jobs for easier debugging.
*   **Pluggable Job Managers:** The API abstracts the execution backend. By default, it uses Docker to run jobs in isolated containers, but it can be configured to manage jobs on a Kubernetes cluster.
*   **Asynchronous by Design:** Endpoints for long-running tasks (like job submission) return immediately, allowing you to build responsive applications.

## How It Works

The API acts as a server that receives requests and translates them into actions performed by a **Job Manager**. The `JobManagerAdapter` is a pluggable component that handles the actual job execution.

1.  **Request:** A client sends an HTTP request to an endpoint, e.g., `POST /v1/jobs/submit` with a job configuration in the request body.
2.  **API Layer:** The FastAPI endpoint validates the request and hands it off to the configured `JobManagerAdapter` (e.g., `DockerAdapter` or `KubernetesAdapter`).
3.  **Job Manager:** The adapter performs the requested action:
    *   For `submit_job`, it might create a temporary config file and run a `docker run` or `kubectl apply -f` command.
    *   For `get_job_status`, it might query the Docker daemon or Kubernetes API server for the state of the container/pod.
4.  **Response:** The API formats the response from the Job Manager and sends it back to the client.

## API Endpoints Overview

All endpoints are prefixed with `/v1`.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| **Jobs** | | |
| `POST` | `/jobs/submit` | Submits and starts a new ETL job in the background. |
| `GET` | `/jobs/status?id={job_id}` | Gets the current status of a job (e.g., `RUNNING`, `FINISHED`). |
| `POST` | `/jobs/stop` | Stops a running job. |
| `POST` | `/jobs/delete` | Permanently deletes a job's history and resources. |
| `GET` | `/jobs/` | Lists all known jobs. |
| `GET` | `/jobs/info?id={job_id}`| Gets detailed information about a job, including start/finish times. |
| `GET` | `/jobs/logs?id={job_id}` | Retrieves logs from a job's container/pod. |
| `GET` | `/jobs/metrics?id={job_id}`| Gets real-time resource usage (CPU/memory). |
| **Previews & Schema** | | |
| `POST`| `/jobs/preview`| Runs the transformation pipeline on a sample of data and returns the result without writing to the sink. |
| `POST`| `/jobs/column-mapping` | Compares the schema of the transformed data against the sink table schema. |
| `POST`| `/connection/schema-mapping` | Compares two provided schemas and returns a mapping analysis (client-side). |
| **Connection Utilities** | | |
| `POST` | `/connection/test` | Validates connection details for a source or sink. |
| `POST` | `/connection/data-objects` | Lists all available tables and views for a given connection. |
| `POST` | `/connection/describe` | Returns the schema (column names and data types) of a specific table or view. |

*For a complete, interactive list of endpoints and their schemas, see the auto-generated Swagger documentation.*

## Getting Started

### Prerequisites

*   Python 3.9+
*   The chosen job execution runtime must be available (e.g., Docker Desktop running, or `kubeconfig` pointing to a cluster).

### Installation & Running the API

1.  Navigate to the `api/` directory.

2.  Install the required API-specific dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Configure your environment. Create a `.env` file in the root directory of the project to set API and backend settings:
    ```dotenv
    # .env
    # Choose the backend: 'docker' or 'kubernetes'
    JOB_MANAGER_TYPE=docker
    JOB_IMAGE_NAME=your-repo/lumigen-job-runner:latest

    # If using Kubernetes
    KUBECONFIG=/path/to/your/.kube/config

    # Choose the metrics provider: 'metrics-api' or 'prometheus'
    METRICS_PROVIDER=prometheus
    PROMETHEUS_URL=http://localhost:9090
    ```

4.  Run the API server using Uvicorn:
    ```bash
    uvicorn api.main:app --host 0.0.0.0 --port 7777 --reload
    ```
    *   The `--reload` flag is great for development, as it automatically restarts the server when you change the code.

5.  Access the interactive documentation by navigating to [http://localhost:7777/docs](http://localhost:7777/docs) in your browser.

## Example: Validating a Connection via cURL

This example tests a PostgreSQL connection before using it in a job.

```bash
curl -X 'POST' \
  'http://localhost:7777/v1/connection/test' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "pagila",
    "user": "postgres",
    "password": "mysecretpassword"
}'
```

A successful request will return a `200 OK` with a confirmation message. An unsuccessful request will return a `422` or `500` error with details about the connection failure.

## License

The Lumigen API is licensed under the [Apache License 2.0](LICENSE).