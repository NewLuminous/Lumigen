# Preparing Kubernetes for Lumigen Spark Jobs

This guide describes how to configure your Kubernetes cluster to run Lumigen Spark jobs using the `KubernetesAdapter`.

---

## 1. Architectural Overview

Lumigen uses a **split-namespace architecture** to isolate workloads and enforce least-privilege security:

```
┌─────────────────────────────────────────┐       ┌─────────────────────────────────────────┐
│ Control / Driver Plane                  │       │ Execution Plane                         │
│ Namespace: lumigen                      │       │ Namespace: spark-execution              │
│                                         │       │                                         │
│ • Lumigen API Server / Job Manager      │       │ • Dynamic Spark Executor Pod 1          │
│ • Spark Driver Pod (SA: spark) ─────────┼──────►│ • Dynamic Spark Executor Pod 2          │
│ • Secrets & Configurations              │       │ • Dynamic Spark Executor Pod N          │
│ • spark-checkpoints-pvc (RWX)           │       │                                         │
│ • spark-log-pvc (RWX)                   │       │ (Managed via Cross-Namespace RBAC)      │
└─────────────────────────────────────────┘       └─────────────────────────────────────────┘
```

1. **Driver Namespace (`lumigen`):** Hosts the API server, configuration secrets, and the Spark Driver pods.
2. **Execution Namespace (`spark-execution`):** An isolated sandbox where the Driver dynamically provisions and terminates Executor pods.
3. **Cross-Namespace RBAC:** The Driver's ServiceAccount in `lumigen` is granted scoped permissions to control Pods, Services, and ConfigMaps inside `spark-execution`.

---

## 2. Quick Start

Apply all prerequisites to your cluster with:

```bash
# 1. Create Namespaces and configure RBAC
kubectl apply -f api/k8s/01-namespaces-rbac.yaml

# 2. Provision Persistent Volumes (Requires an RWX StorageClass like NFS/EFS/Longhorn)
kubectl apply -f api/k8s/02-storage-pvcs.yaml
```

---

## 3. Step-by-Step Manual Setup

### 3.1. Namespaces
Create the Driver and Execution namespaces:
```bash
kubectl create namespace lumigen
kubectl create namespace spark-execution
```

### 3.2. Service Account (Driver Identity)
The Spark Driver needs an explicit ServiceAccount identity in the `lumigen` namespace:

```yaml
# spark-driver-sa.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: lumigen
```
```bash
kubectl apply -f spark-driver-sa.yaml
```

### 3.3. Execution Role
Create a Role in the `spark-execution` namespace that allows managing executor pods, services (for driver-executor communication), and shuffle tracking configmaps:

```yaml
# spark-execution-role.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-execution-role
  namespace: spark-execution
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "watch", "list", "delete", "update", "patch"]
```
```bash
kubectl apply -f spark-execution-role.yaml
```

### 3.4. RoleBinding (Cross-Namespace Permission)
Bind the `spark` ServiceAccount from `lumigen` to the `spark-execution-role` in `spark-execution`:

```yaml
# spark-driver-role-binding.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver-role-binding
  namespace: spark-execution
subjects:
- kind: ServiceAccount
  name: spark
  namespace: lumigen
roleRef:
  kind: Role
  name: spark-execution-role
  apiGroup: rbac.authorization.k8s.io
```
```bash
kubectl apply -f spark-driver-role-binding.yaml
```

### 3.5. Storage & Volumes (RWX PVCs)

> ⚠️ **Important:** Both PVCs must use `accessModes: ["ReadWriteMany"]` (RWX). Multiple pods on different nodes write and read from these volumes simultaneously. Ensure your cluster has a shared storage provider configured (NFS, AWS EFS, Longhorn, CephFS, etc.).

#### A. Streaming Checkpoints (`spark-checkpoints-pvc`)
Required for streaming ETL jobs to store state store snapshots, commit logs, and topic offset tracking:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: spark-checkpoints-pvc
  namespace: lumigen
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
```

#### B. Spark Event Logs (`spark-log-pvc`)
Required for Spark History Server to display job metrics, stages, tasks, and visual DAGs:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: spark-log-pvc
  namespace: lumigen
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 20Gi
```

---

## 4. Mapping to Lumigen API Configuration

Ensure the following environment variables or `api/config.py` settings match your Kubernetes setup:

| Setting | Value | Description |
| :--- | :--- | :--- |
| `KUBE_NAMESPACE` | `lumigen` | Namespace where the API and Driver pods run |
| `KUBE_EXECUTOR_NAMESPACE` | `spark-execution` | Namespace where Spark spawns executor pods |
| `KUBE_SERVICE_ACCOUNT` | `spark` | ServiceAccount bound to the driver pod |
| `KUBE_EXECUTOR_SERVICE_ACCOUNT` | `default` | ServiceAccount assigned to executor pods |
| `KUBE_CHECKPOINT_PVC_NAME` | `spark-checkpoints-pvc` | PVC mounted for streaming checkpoints |
| `KUBE_LOG_PVC_NAME` | `spark-log-pvc` | PVC mounted for Spark History event logs |
| `SPARK_EVENT_LOG_DIR` | `file:///tmp/spark-events` | Local container path where the PVC is mounted |
| `SPARK_MASTER` | `k8s://https://kubernetes.default.svc` | In-cluster Kubernetes API endpoint |