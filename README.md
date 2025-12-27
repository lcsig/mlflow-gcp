# MLFlow on Google Cloud Run

Deploy MLFlow 3.6.0 with authentication on Google Cloud Run, backed by Cloud SQL and Cloud Storage.

## Features

- [+] Serverless MLFlow 3.6.0 deployment on Cloud Run
- [+] Basic HTTP authentication (username/password)
- [+] PostgreSQL backend (Cloud SQL)
- [+] Cloud Storage for artifacts
- [+] Auto-scaling (0-1 instances)
- [+] Up to 2 CPUs per instance
- [+] Pure Python deployment (no Terraform)

## Architecture

![Architecture](./arch.svg)

```
Client → Cloud Run (MLFlow + Auth) → Cloud SQL (PostgreSQL)
                                   → Cloud Storage (Artifacts)
```

## Prerequisites

- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- `docker` installed
- Python 3.10+ with pip

## Pre-commit (recommended)

This repo includes a pre-commit setup to:
- auto-format and lint Python (`ruff`, `ruff-format`)
- validate YAML and catch common git issues
- detect secrets/keys (helps prevent accidentally committing `config.yaml` passwords)

Install and enable it:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## Quick Start

### Step 1: Setup

Install dependencies:

```bash
pip install -r deploy_requirements.txt
```

### Step 2: Configure

Create your local config from the template, then edit it:

```bash
cp config.yaml.bak config.yaml
```

Now edit `config.yaml` with your settings:

```yaml
gcp:
  project_id: "your-gcp-project-id"

cloud_sql:
  database_password: "your-secure-db-password"  # pragma: allowlist secret

storage:
  bucket_name: "your-unique-bucket-name"  # Must be globally unique

mlflow:
  username: "admin"
  password: "your-secure-password"  # pragma: allowlist secret
```

**Important:** Change all passwords and use a unique bucket name.
`config.yaml` is meant to be **local-only** and is ignored by git.

### Step 3: Deploy

Authenticate with GCP and deploy:

```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login
gcloud config set project your-gcp-project-id

# Deploy (takes 5-10 minutes)
./deploy.py
```

The deployment will:
- Enable required GCP APIs
- Create Cloud SQL PostgreSQL instance
- Create Cloud Storage bucket
- Build and push Docker image
- Deploy MLFlow to Cloud Run
- Configure authentication

The script will output your MLFlow URL and tracking URI.

### Step 4: Test

```bash
./test_connection.py
```

## Usage

### Access Web UI

Visit the URL from deployment output in your browser. You'll be prompted for username and password. <!-- pragma: allowlist secret -->

### Python Code

```python
import mlflow
import os

# Set tracking URI (from deployment output)
os.environ["MLFLOW_TRACKING_URI"] = "https://admin:YOUR_TOKEN@your-url"  # pragma: allowlist secret

# Use MLFlow
with mlflow.start_run():
    mlflow.log_param("param", 1)
    mlflow.log_metric("metric", 0.95)
```

## Configuration

### config.yaml Structure

Start by copying the template:

```bash
cp config.yaml.bak config.yaml
```

```yaml
# GCP Settings
gcp:
  project_id: "your-project"
  region: "us-central1"

# Cloud Run Settings
cloud_run:
  service_name: "mlflow-server"
  max_instances: 1          # Maximum concurrent instances
  min_instances: 0          # Scale to zero when idle
  cpu_limit: "2"            # CPUs per instance
  memory_limit: "4Gi"       # Memory per instance

# Cloud SQL Settings
cloud_sql:
  instance_name: "mlflow-db"
  database_version: "POSTGRES_15"
  tier: "db-f1-micro"       # Instance size
  database_name: "mlflow"
  database_user: "mlflow"
  database_password: "CHANGE_ME"  # pragma: allowlist secret

# Cloud Storage Settings
storage:
  bucket_name: "your-unique-bucket-name"
  location: "us-central1"
  lifecycle_days: 90        # Delete old artifacts after N days

# MLFlow Authentication
mlflow:
  username: "admin"
  password: "CHANGE_ME"  # pragma: allowlist secret

# Docker Settings
docker:
  image_name: "mlflow"
  tag: "latest"
```

## Python Scripts

### deploy.py

Deploys all infrastructure (APIs, SQL, Storage, Docker image, Cloud Run).

```bash
./deploy.py [config.yaml]
```

### destroy.py

Destroys all infrastructure with confirmation (Cloud Run, SQL, Storage).

```bash
./destroy.py [config.yaml]
```

### test_connection.py

Tests connection, authentication, and displays tracking URI.

```bash
./test_connection.py [config.yaml]
```

## Scaling Configuration

Modify in `config.yaml`:

```yaml
cloud_run:
  min_instances: 0    # Scale to zero when idle (serverless)
  max_instances: 1    # Maximum concurrent instances
  cpu_limit: "2"      # 2 CPUs per instance
  memory_limit: "4Gi" # 4GB RAM per instance
```

Cloud Run will:
- Scale to 0 when no requests (no cost)
- Start new instance on first request (cold start ~10s)
- Scale up to max_instances under load
- Use cpu_idle=true for cost optimization


## Clean Up

To destroy all resources:

```bash
./destroy.py
```

This will:
- Delete Cloud Run service
- Delete Cloud SQL instance (permanent data loss)
- Delete Cloud Storage bucket (permanent data loss)

## File Structure

```
.
├── app/                   # Docker image code
│   ├── Dockerfile         # MLFlow container image
│   ├── requirements.txt   # MLFlow dependencies
│   ├── auth_wrapper.py    # Authentication proxy
│   └── start.sh           # Container entrypoint
├── deploy.py              # Deployment script
├── destroy.py             # Teardown script
├── test_connection.py     # Connection test
├── deploy_requirements.txt # Deployment dependencies
├── config.yaml.bak        # Configuration template (commit this)
├── config.yaml            # Local configuration (do not commit)
├── README.md              # Documentation
├── .gitignore             # Git ignore
└── LICENSE                # MIT License
```



## License

MIT License - See LICENSE file for details
