#!/usr/bin/env python3
"""
Startup script for DeepFace Ray API

Usage:
    uv python run.py                    # Start with default settings
    uv python run.py --workers 8       # Start with 8 workers
    uv python run.py --port 8080       # Start on port 8080
    uv python run.py --debug           # Start in debug mode
"""

import argparse
import os
import sys
import uvicorn


def _tls_params_from_env() -> tuple[bool, str | None, str | None, str | None]:
    tls_enabled = os.getenv("TLS_ENABLED", "false").lower() == "true"
    certfile = os.getenv("TLS_CERTFILE")
    keyfile = os.getenv("TLS_KEYFILE")
    ca_certs = os.getenv("TLS_CA_CERTS")
    if tls_enabled and (not certfile or not keyfile):
        print("TLS_ENABLED=true but TLS_CERTFILE or TLS_KEYFILE missing; starting without TLS", file=sys.stderr)
        return False, None, None, None
    return tls_enabled, certfile, keyfile, ca_certs


def main():
    parser = argparse.ArgumentParser(description="Start DeepFace Ray API")
    parser.add_argument("--host", default="0.0.0.0", help="Host address")
    parser.add_argument("--port", type=int, default=8000, help="Port number")
    parser.add_argument("--workers", type=int, default=4, help="Number of Ray workers")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--ray-address", default=os.getenv("RAY_ADDRESS", "auto"), help="Ray cluster address")
    parser.add_argument("--no-ray", action="store_true", help="Disable Ray (for testing)")

    args = parser.parse_args()

    # Set environment variables
    os.environ["NUM_WORKERS"] = str(args.workers)
    os.environ["RAY_ADDRESS"] = args.ray_address

    if args.no_ray:
        os.environ["DISABLE_RAY"] = "1"

    print(f"Starting DeepFace Ray API...")
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print(f"Workers: {args.workers}")
    print(f"Ray Address: {args.ray_address}")
    print(f"Debug Mode: {args.debug}")

    # Do not pre-initialize Ray here to keep memory footprint minimal; the app lifespan handles it.

    tls_enabled, certfile, keyfile, ca_certs = _tls_params_from_env()

    # Start the server
    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.debug,
        workers=1,  # Always use 1 uvicorn worker with Ray
        log_level="debug" if args.debug else "info",
        ssl_certfile=certfile if tls_enabled else None,
        ssl_keyfile=keyfile if tls_enabled else None,
        ssl_ca_certs=ca_certs if tls_enabled and ca_certs else None,
        # ssl_version, ssl_ciphers, etc., can be added via env if needed
    )


if __name__ == "__main__":
    main()
