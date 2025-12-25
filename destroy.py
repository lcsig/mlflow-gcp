#!/usr/bin/env python3
"""
MLFlow GCP Destroy Script
Removes all MLFlow infrastructure from GCP
"""

import subprocess
import sys

import yaml
from google.cloud import storage


class MLFlowDestroyer:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize destroyer with configuration"""
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.project_id = self.config["gcp"]["project_id"]
        self.region = self.config["gcp"]["region"]
        self.storage_client = storage.Client(project=self.project_id)

        print(f"[+] Initialized destroyer for project: {self.project_id}")

    def confirm_destroy(self) -> bool:
        """Ask user for confirmation"""
        print("\n" + "!" * 60)
        print("WARNING: This will destroy all MLFlow infrastructure")
        print("!" * 60)
        print("\nThis includes:")
        print("  - Cloud Run service")
        print("  - Cloud SQL database (ALL DATA WILL BE LOST)")
        print("  - GCS bucket (ALL ARTIFACTS WILL BE LOST)")
        print("")

        response = input("Are you sure you want to destroy everything? (yes/no): ")
        if response.lower() != "yes":
            return False

        response = input("Type 'destroy' to confirm: ")
        return response == "destroy"

    def delete_cloud_run(self):
        """Delete Cloud Run service"""
        service_name = self.config["cloud_run"]["service_name"]

        print(f"[+] Deleting Cloud Run service: {service_name}")

        try:
            subprocess.run(
                [
                    "gcloud",
                    "run",
                    "services",
                    "delete",
                    service_name,
                    f"--region={self.region}",
                    f"--project={self.project_id}",
                    "--quiet",
                ],
                check=True,
                capture_output=True,
            )
            print("  [+] Service deleted")
        except subprocess.CalledProcessError:
            print("  [!] Service may not exist or already deleted")

    def delete_sql_instance(self):
        """Delete Cloud SQL instance"""
        instance_name = self.config["cloud_sql"]["instance_name"]

        print(f"[+] Deleting Cloud SQL instance: {instance_name}")

        try:
            # List all instances
            result = subprocess.run(
                [
                    "gcloud",
                    "sql",
                    "instances",
                    "list",
                    f"--project={self.project_id}",
                    "--format=value(name)",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            instances = result.stdout.strip().split("\n")

            if instance_name not in instances:
                print("  [+] Instance does not exist")
                return

            print(f"  [+] Deleting instance: {instance_name}")
            subprocess.run(
                [
                    "gcloud",
                    "sql",
                    "instances",
                    "delete",
                    instance_name,
                    f"--project={self.project_id}",
                    "--quiet",
                ],
                check=True,
                capture_output=True,
            )
            print("    [+] Instance deleted")

        except subprocess.CalledProcessError as e:
            print(f"  [!] Error deleting SQL instances: {e}")

    def delete_storage_bucket(self):
        """Delete GCS bucket"""
        bucket_name = self.config["storage"]["bucket_name"]

        print(f"[+] Deleting storage bucket: {bucket_name}")

        try:
            bucket = self.storage_client.bucket(bucket_name)
            if bucket.exists():
                # Delete all objects first
                blobs = list(bucket.list_blobs())
                if blobs:
                    print(f"  [+] Deleting {len(blobs)} objects")
                    bucket.delete_blobs(blobs)

                # Delete bucket
                bucket.delete()
                print("  [+] Bucket deleted")
            else:
                print("  [+] Bucket does not exist")
        except Exception as e:
            print(f"  [!] Error deleting bucket: {e}")

    def destroy_all(self):
        """Destroy all infrastructure"""
        if not self.confirm_destroy():
            print("\n[+] Destruction cancelled")
            return

        print("\n" + "=" * 60)
        print("Destroying MLFlow Infrastructure")
        print("=" * 60 + "\n")

        # Delete in reverse order of creation
        self.delete_cloud_run()
        self.delete_sql_instance()
        self.delete_storage_bucket()

        print("\n" + "=" * 60)
        print("Destruction Complete")
        print("=" * 60 + "\n")


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "config.yaml"

    try:
        destroyer = MLFlowDestroyer(config_path)
        destroyer.destroy_all()
    except Exception as e:
        print(f"\n[-] Destruction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
