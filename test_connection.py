#!/usr/bin/env python3
"""Test MLFlow server connection and authentication"""

import sys
import yaml
import requests
import subprocess
from getpass import getpass


def test_connection(config_path: str = "config.yaml"):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    project_id = config["gcp"]["project_id"]
    region = config["gcp"]["region"]
    service_name = config["cloud_run"]["service_name"]
    username = config["mlflow"]["username"]

    print("[+] Testing MLFlow connection")

    # Get service URL
    result = subprocess.run(
        [
            "gcloud",
            "run",
            "services",
            "describe",
            service_name,
            f"--region={region}",
            f"--project={project_id}",
            "--format=value(status.url)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    url = result.stdout.strip()
    if not url:
        print("[-] Service not found")
        sys.exit(1)

    print(f"[+] URL: {url}")

    # Test authentication
    password = getpass("Enter password: ")
    response = requests.get(url, auth=(username, password), timeout=30)

    if response.status_code == 200:
        tracking_uri = f"https://{username}:{password}@{url.replace('https://', '')}"
        print("[+] Connected successfully\n")
        print(f'export MLFLOW_TRACKING_URI="{tracking_uri}"')
    elif response.status_code == 401:
        print("[-] Authentication failed")
        sys.exit(1)
    else:
        print(f"[-] Failed with status {response.status_code}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
        test_connection(config_path)
    except Exception as e:
        print(f"[-] Error: {e}")
        sys.exit(1)
