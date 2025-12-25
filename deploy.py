#!/usr/bin/env python3
"""
MLFlow GCP Deployment Script
Deploys MLFlow to Cloud Run with Cloud SQL and Cloud Storage
"""

import os
import sys
import time
import yaml
import subprocess
from typing import Dict
from google.cloud import run_v2
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.api_core import exceptions
from google.auth import default


class MLFlowDeployer:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize deployer with configuration"""
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.project_id = self.config["gcp"]["project_id"]
        self.region = self.config["gcp"]["region"]

        # Initialize clients
        self.run_client = run_v2.ServicesClient()
        self.storage_client = storage.Client(project=self.project_id)

        # Initialize SQL Admin API client using google-api-python-client
        credentials, _ = default()
        self.sql_client = build("sqladmin", "v1", credentials=credentials)

        print(f"[+] Initialized deployer for project: {self.project_id}")

    def enable_apis(self):
        """Enable required GCP APIs"""
        apis = [
            "run.googleapis.com",
            "sqladmin.googleapis.com",
            "storage.googleapis.com",
            "artifactregistry.googleapis.com",
        ]

        print("[+] Enabling required APIs")
        for api in apis:
            try:
                subprocess.run(
                    [
                        "gcloud",
                        "services",
                        "enable",
                        api,
                        f"--project={self.project_id}",
                    ],
                    check=True,
                    capture_output=True,
                )
                print(f"  [+] Enabled {api}")
            except subprocess.CalledProcessError as e:
                print(f"  [!] Warning: Could not enable {api}: {e}")

    def _get_project_number(self) -> str:
        """Return the numeric project number as a string."""
        result = subprocess.run(
            [
                "gcloud",
                "projects",
                "describe",
                self.project_id,
                "--format=value(projectNumber)",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def _get_cloud_run_service_account(self) -> str:
        """
        Return the service account email that Cloud Run should run as.
        If not configured, defaults to the project's Compute Engine default service account.
        """
        sa = self.config.get("cloud_run", {}).get("service_account")
        if sa:
            return sa
        project_number = self._get_project_number()
        return f"{project_number}-compute@developer.gserviceaccount.com"

    def _ensure_project_iam_roles(self, service_account_email: str, roles: list[str]):
        """Grant project-level IAM roles to a service account (best-effort idempotent)."""
        member = f"serviceAccount:{service_account_email}"
        for role in roles:
            try:
                subprocess.run(
                    [
                        "gcloud",
                        "projects",
                        "add-iam-policy-binding",
                        self.project_id,
                        f"--member={member}",
                        f"--role={role}",
                        "--quiet",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                print(f"  [+] Ensured {role} on {service_account_email}")
            except subprocess.CalledProcessError as e:
                # Don't hard-fail here; Cloud Run deploy will surface any remaining permission issues.
                msg = (e.stderr or e.stdout or str(e)).strip()
                print(
                    f"  [!] Warning: Could not grant {role} to {service_account_email}: {msg}"
                )

    def _ensure_bucket_iam(self, bucket_name: str, service_account_email: str):
        """Ensure the Cloud Run service account can read/write artifacts in the bucket."""
        member = f"serviceAccount:{service_account_email}"
        try:
            subprocess.run(
                [
                    "gcloud",
                    "storage",
                    "buckets",
                    "add-iam-policy-binding",
                    f"gs://{bucket_name}",
                    f"--member={member}",
                    "--role=roles/storage.objectAdmin",
                    "--quiet",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            print(
                f"  [+] Ensured roles/storage.objectAdmin on bucket gs://{bucket_name}"
            )
        except subprocess.CalledProcessError as e:
            msg = (e.stderr or e.stdout or str(e)).strip()
            print(f"  [!] Warning: Could not grant bucket access: {msg}")

    def create_storage_bucket(self) -> str:
        """Create GCS bucket for artifacts"""
        bucket_name = self.config["storage"]["bucket_name"]
        location = self.config["storage"]["location"]

        print(f"[+] Creating storage bucket: {bucket_name}")

        try:
            bucket = self.storage_client.bucket(bucket_name)
            if bucket.exists():
                print("  [+] Bucket already exists")
                return bucket_name

            bucket = self.storage_client.create_bucket(
                bucket_name, location=location, project=self.project_id
            )

            # Enable versioning
            bucket.versioning_enabled = True
            bucket.patch()

            # Set lifecycle policy
            lifecycle_days = self.config["storage"]["lifecycle_days"]
            bucket.add_lifecycle_delete_rule(age=lifecycle_days)
            bucket.patch()

            print("  [+] Bucket created successfully")
            return bucket_name

        except exceptions.Conflict:
            print("  [+] Bucket already exists")
            return bucket_name
        except Exception:
            print("  [-] Error creating bucket: {e}")
            raise

    def create_sql_instance(self) -> Dict[str, str]:
        """Create Cloud SQL PostgreSQL instance"""
        instance_name = self.config["cloud_sql"]["instance_name"]
        db_version = self.config["cloud_sql"]["database_version"]
        tier = self.config["cloud_sql"]["tier"]

        print(f"[+] Creating Cloud SQL instance: {instance_name}")

        # If the configured instance already exists, reuse it.
        if self._sql_instance_exists(instance_name):
            print(f"  [+] SQL instance already exists, reusing: {instance_name}")
            self._create_database(instance_name)
            self._create_database_user(instance_name)
            connection_name = f"{self.project_id}:{self.region}:{instance_name}"
            return {
                "instance_name": instance_name,
                "connection_name": connection_name,
            }

        # Build instance configuration for Discovery API
        instance_body = {
            "name": instance_name,
            "databaseVersion": db_version,
            "region": self.region,
            "settings": {
                "tier": tier,
                "backupConfiguration": {"enabled": True, "startTime": "03:00"},
                "ipConfiguration": {"ipv4Enabled": True, "requireSsl": False},
            },
        }

        try:
            # Insert instance using Discovery API
            request = self.sql_client.instances().insert(
                project=self.project_id, body=instance_body
            )
            request.execute()

            print("  [+] Waiting for instance creation (this may take several minutes)")
            self._wait_for_sql_instance_ready(instance_name, timeout_seconds=20 * 60)

            print("  [+] SQL instance created")

            # Create database
            self._create_database(instance_name)

            # Create user
            self._create_database_user(instance_name)

            connection_name = f"{self.project_id}:{self.region}:{instance_name}"

            return {
                "instance_name": instance_name,
                "connection_name": connection_name,
            }

        except Exception as e:
            # Check if instance already exists
            if "already exists" in str(e).lower():
                print("  [+] Instance already exists")
                self._create_database(instance_name)
                self._create_database_user(instance_name)
                connection_name = f"{self.project_id}:{self.region}:{instance_name}"
                return {
                    "instance_name": instance_name,
                    "connection_name": connection_name,
                }
            print(f"  [-] Error creating SQL instance: {e}")
            raise

    def _sql_instance_exists(self, instance_name: str) -> bool:
        """Return True if the Cloud SQL instance exists."""
        try:
            self.sql_client.instances().get(
                project=self.project_id, instance=instance_name
            ).execute()
            return True
        except HttpError as e:
            # 404 = not found
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 404:
                return False
            raise

    def _wait_for_sql_instance_ready(
        self, instance_name: str, timeout_seconds: int = 1200
    ):
        """Wait until the SQL instance is RUNNABLE (or until timeout)."""
        deadline = time.time() + timeout_seconds
        last_state = None
        while time.time() < deadline:
            try:
                inst = (
                    self.sql_client.instances()
                    .get(project=self.project_id, instance=instance_name)
                    .execute()
                )
                state = inst.get("state")
                if state and state != last_state:
                    print(f"  [+] Instance state: {state}")
                    last_state = state
                if state == "RUNNABLE":
                    return
            except HttpError:
                # Instance may not be visible immediately after insert.
                pass
            time.sleep(10)

        raise TimeoutError(
            f"Timed out waiting for Cloud SQL instance '{instance_name}' to become RUNNABLE"
        )

    def _create_database(self, instance_name: str):
        """Create database in SQL instance"""
        db_name = self.config["cloud_sql"]["database_name"]
        print(f"  [+] Creating database: {db_name}")

        try:
            subprocess.run(
                [
                    "gcloud",
                    "sql",
                    "databases",
                    "create",
                    db_name,
                    f"--instance={instance_name}",
                    f"--project={self.project_id}",
                ],
                check=True,
                capture_output=True,
            )
            print("    [+] Database created")
        except subprocess.CalledProcessError:
            print("    [+] Database may already exist")

    def _create_database_user(self, instance_name: str):
        """Create database user"""
        db_user = self.config["cloud_sql"]["database_user"]
        db_password = self.config["cloud_sql"]["database_password"]

        print(f"  [+] Creating database user: {db_user}")

        try:
            subprocess.run(
                [
                    "gcloud",
                    "sql",
                    "users",
                    "create",
                    db_user,
                    f"--instance={instance_name}",
                    f"--password={db_password}",
                    f"--project={self.project_id}",
                ],
                check=True,
                capture_output=True,
            )
            print("    [+] User created")
        except subprocess.CalledProcessError:
            print("    [+] User may already exist")

    def build_and_push_image(self) -> str:
        """Build and push Docker image to GCR"""
        image_name = self.config["docker"]["image_name"]
        tag = self.config["docker"]["tag"]
        gcr_image = f"gcr.io/{self.project_id}/{image_name}:{tag}"

        print("[+] Building Docker image")

        # Build image
        subprocess.run(["docker", "build", "-t", gcr_image, "app/"], check=True)

        print("[+] Configuring Docker for GCR")
        subprocess.run(
            ["gcloud", "auth", "configure-docker", "gcr.io", "--quiet"], check=True
        )

        print("[+] Pushing image to GCR")
        subprocess.run(["docker", "push", gcr_image], check=True)

        print(f"  [+] Image pushed: {gcr_image}")
        return gcr_image

    def deploy_cloud_run(self, image_uri: str, bucket_name: str, sql_connection: str):
        """Deploy MLFlow to Cloud Run"""
        service_name = self.config["cloud_run"]["service_name"]

        print(f"[+] Deploying Cloud Run service: {service_name}")

        db_user = self.config["cloud_sql"]["database_user"]
        db_password = self.config["cloud_sql"]["database_password"]
        db_name = self.config["cloud_sql"]["database_name"]
        mlflow_user = self.config["mlflow"]["username"]
        mlflow_pass = self.config["mlflow"]["password"]

        backend_uri = f"postgresql+psycopg2://{db_user}:{db_password}@/{db_name}?host=/cloudsql/{sql_connection}"
        artifact_root = f"gs://{bucket_name}/artifacts"
        service_account = self._get_cloud_run_service_account()

        cmd = [
            "gcloud",
            "run",
            "deploy",
            service_name,
            f"--image={image_uri}",
            f"--region={self.region}",
            f"--project={self.project_id}",
            "--platform=managed",
            "--allow-unauthenticated",
            f"--service-account={service_account}",
            f"--min-instances={self.config['cloud_run']['min_instances']}",
            f"--max-instances={self.config['cloud_run']['max_instances']}",
            f"--cpu={self.config['cloud_run']['cpu_limit']}",
            f"--memory={self.config['cloud_run']['memory_limit']}",
            "--no-cpu-throttling",
            f"--add-cloudsql-instances={sql_connection}",
            f"--set-env-vars=MLFLOW_BACKEND_STORE_URI={backend_uri}",
            f"--set-env-vars=MLFLOW_DEFAULT_ARTIFACT_ROOT={artifact_root}",
            f"--set-env-vars=MLFLOW_AUTH_USERNAME={mlflow_user}",
            f"--set-env-vars=MLFLOW_AUTH_PASSWORD={mlflow_pass}",
            "--port=8080",
        ]

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print("  [+] Cloud Run service deployed")

            # Extract URL from output
            for line in result.stdout.split("\n"):
                if "https://" in line and service_name in line:
                    url = line.strip().split()[-1]
                    return url

            # Get URL using gcloud
            url_cmd = [
                "gcloud",
                "run",
                "services",
                "describe",
                service_name,
                f"--region={self.region}",
                f"--project={self.project_id}",
                "--format=value(status.url)",
            ]
            result = subprocess.run(url_cmd, check=True, capture_output=True, text=True)
            return result.stdout.strip()

        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            stdout = (e.stdout or "").strip()
            print(f"  [-] Error deploying Cloud Run: {e}")
            if stdout:
                print(stdout)
            if stderr:
                print(stderr, file=sys.stderr)
            raise

    def deploy_all(self):
        """Deploy complete MLFlow infrastructure"""
        print("\n" + "=" * 60)
        print("MLFlow GCP Deployment")
        print("=" * 60 + "\n")

        # Enable APIs
        self.enable_apis()

        # Create storage bucket
        bucket_name = self.create_storage_bucket()

        # Create SQL instance
        sql_info = self.create_sql_instance()

        # Ensure Cloud Run runtime service account has required permissions
        service_account = self._get_cloud_run_service_account()
        print(
            f"[+] Ensuring IAM permissions for Cloud Run service account: {service_account}"
        )
        self._ensure_project_iam_roles(
            service_account,
            roles=[
                "roles/cloudsql.client",
            ],
        )
        self._ensure_bucket_iam(
            bucket_name=bucket_name, service_account_email=service_account
        )

        # Build and push Docker image
        image_uri = self.build_and_push_image()

        # Deploy Cloud Run
        service_url = self.deploy_cloud_run(
            image_uri, bucket_name, sql_info["connection_name"]
        )

        print("\n" + "=" * 60)
        print("Deployment Complete!")
        print("=" * 60)
        print(f"\nMLFlow URL: {service_url}")
        print(f"Username: {self.config['mlflow']['username']}")
        print(f"Password: {self.config['mlflow']['password']}")
        print("\nTracking URI:")
        mlflow_user = self.config["mlflow"]["username"]
        mlflow_pass = self.config["mlflow"]["password"]
        tracking_uri = (
            f"https://{mlflow_user}:{mlflow_pass}@{service_url.replace('https://', '')}"
        )
        print(f"  {tracking_uri}")
        print("\nSet in your code:")
        print(f'  export MLFLOW_TRACKING_URI="{tracking_uri}"')
        print("=" * 60 + "\n")


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "config.yaml"

    if not os.path.exists(config_path):
        print(f"[-] Config file not found: {config_path}")
        print("[+] Copy config.yaml and fill in your values")
        sys.exit(1)

    try:
        deployer = MLFlowDeployer(config_path)
        deployer.deploy_all()
    except Exception as e:
        print(f"\n[-] Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
