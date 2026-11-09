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

*   Python 3.10+
*   The chosen job execution runtime must be available (e.g., Docker Desktop running, or `kubeconfig` pointing to a cluster).

### 1. Build the Docker Image

Before running jobs, you must build the Lumigen Docker image.

> **Note:** The Lumigen Docker image is built on top of the base image `newluminous/spark-k8s`. If the base image is not present locally, it is built from the [containerized-data-engines](https://github.com/NewLuminous/containerized-data-engines/tree/main/spark-k8s) repository under the `spark-k8s/` directory.

A helper script is provided to handle the build process securely:

```bash
./build.sh
```

*Options for `build.sh`:*
*   `-n, --name`: Image name (Default: `newluminous/lumigen`)
*   `-t, --tag`: Image tag (Default: `3.3.3` or your base tag)
*   `-c, --conf-dir`: Path to an external Spark configuration directory

### 2. Configure Your Environment

Create a `.env` file in the `api/` directory (or the project root) to set your API and backend settings. Choose the configuration that matches your desired job execution backend:

**Option A: Using Docker (Recommended for Local Development)**
```dotenv
# .env
JOB_MANAGER_TYPE=docker
JOB_IMAGE_NAME=newluminous/lumigen:latest

# Spark execution mode (local by default for Docker)
SPARK_MASTER=local[*]
```

**Option B: Using Kubernetes (Recommended for Production)**
```dotenv
# .env
JOB_MANAGER_TYPE=kubernetes
JOB_IMAGE_NAME=newluminous/lumigen:latest
EXECUTOR_IMAGE_NAME=newluminous/spark-k8s:latest

# Kubernetes Authentication & Namespaces
KUBECONFIG=/path/to/your/.kube/config
KUBE_NAMESPACE=lumigen
KUBE_EXECUTOR_NAMESPACE=spark
KUBE_SERVICE_ACCOUNT=spark
KUBE_EXECUTOR_SERVICE_ACCOUNT=default
KUBE_CHECKPOINT_PVC_NAME=spark-checkpoints-pvc
KUBE_LOG_PVC_NAME=spark-log-pvc

# Spark Master URL (Targeting the Kubernetes API Server)
SPARK_MASTER=k8s://https://<your-k8s-api-server>:<port>

# Spark Auto-Scaling (Dynamic Resource Allocation)
SPARK_MIN_EXECUTORS=1
SPARK_MAX_EXECUTORS=10

SPARK_EVENT_LOG_ENABLED=true

# Real-time Metrics (Optional: 'metrics-api' or 'prometheus')
METRICS_PROVIDER=prometheus
PROMETHEUS_URL=http://localhost:9090
```

### 3. Setup Kubernetes Pre-requisites (For Option B Only)

If you chose to run using **Option B (Kubernetes)**, you must establish the required namespaces and configure the Role-Based Access Control (RBAC) permissions before launching jobs. This grants the Spark driver running in your application namespace permission to dynamically orchestrate executor pods.

Apply the configurations found under `k8s/spark-rbac/`:

```bash
# 1. Create the main application namespace if it doesn't exist
kubectl create namespace lumigen

# 2. Deploy the RBAC configuration and the executor namespace
kubectl apply -f k8s/spark-rbac/
```

### 4. Run the API Server

You can run the API server using Docker Compose (Recommended) or locally via Python.

**Method A: Running via Docker Compose (Recommended)**

This is the easiest way to run the API. The provided `docker-compose.yml` mounts the Docker socket (allowing the API container to spawn sibling "job" containers) and sets up the necessary volume mounts for configurations and checkpoints.

```bash
docker-compose up -d
```

**Method B: Running Locally via Uvicorn**

If you prefer to run the API directly on your host machine for development/debugging:

1.  Navigate to the `api/` directory.

2.  Install the required API-specific dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Run the API server using Uvicorn:
    ```bash
    uvicorn api.main:app --host 0.0.0.0 --port 7777 --reload
    ```
    *   The `--reload` flag is great for development, as it automatically restarts the server when you change the code.

### 5. Access the API

Once running, access the interactive API documentation (Swagger UI) by navigating to:
**[http://localhost:7777/docs](http://localhost:7777/docs)**

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