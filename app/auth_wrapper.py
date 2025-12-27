#!/usr/bin/env python3
"""
MLFlow Authentication Wrapper
Provides basic HTTP authentication for MLFlow server
"""

import os
import sys
from functools import wraps

import requests
from flask import Flask, Response, request
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Get credentials from environment
USERNAME = os.environ.get("MLFLOW_AUTH_USERNAME", "admin")
PASSWORD_HASH = generate_password_hash(os.environ.get("MLFLOW_AUTH_PASSWORD", "admin"))

MLFLOW_HOST = "localhost"
MLFLOW_PORT = 5000


def check_auth(username, password):
    """Validate username and password"""
    return username == USERNAME and check_password_hash(PASSWORD_HASH, password)


def authenticate():
    """Send 401 response for authentication"""
    return Response(
        "Authentication required\nPlease provide valid credentials",
        401,
        {"WWW-Authenticate": 'Basic realm="MLFlow Login"'},
    )


def requires_auth(f):
    """Decorator for routes requiring authentication"""

    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
@requires_auth
def proxy(path):
    """Proxy all requests to MLFlow server after authentication"""
    url = f"http://{MLFLOW_HOST}:{MLFLOW_PORT}/{path}"

    # Forward query parameters
    if request.query_string:
        url += f"?{request.query_string.decode()}"

    # Prepare headers (exclude hop-by-hop headers only)
    headers = {
        key: value
        for key, value in request.headers
        if key.lower() not in ["host", "connection", "authorization"]
    }

    # Forward request to MLFlow with streaming
    try:
        resp = requests.request(
            method=request.method,
            url=url,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=300,
            stream=True,  # Enable streaming for artifact proxy
        )

        # Only exclude hop-by-hop headers, preserve content headers for streaming
        excluded_headers = {"connection", "keep-alive", "transfer-encoding"}
        response_headers = {
            name: value
            for name, value in resp.headers.items()
            if name.lower() not in excluded_headers
        }

        # Stream the response instead of buffering
        return Response(
            resp.iter_content(chunk_size=8192),
            status=resp.status_code,
            headers=response_headers,
        )
    except Exception as e:
        print(f"[!] Proxy error: {e}", file=sys.stderr)
        return Response(f"Error connecting to MLFlow: {str(e)}", 502)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[+] Starting MLFlow authentication proxy on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
