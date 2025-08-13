#!/usr/bin/env python3
"""
Startup script for DeepFace Ray API

Usage:
    python run.py                    # Start with default settings
    python run.py --workers 8       # Start with 8 workers
    python run.py --port 8080       # Start on port 8080
    python run.py --debug           # Start in debug mode
"""

import argparse
import os
import sys
import uvicorn
import ray
import ssl


def _build_ssl_context_from_env() -> ssl.SSLContext | None:
    tls_enabled = os.getenv("TLS_ENABLED", "false").lower() == "true"
    if not tls_enabled:
        return None
    certfile = os.getenv("TLS_CERTFILE")
    keyfile = os.getenv("TLS_KEYFILE")
    ca_certs = os.getenv("TLS_CA_CERTS")
    if not certfile or not keyfile:
        print("TLS_ENABLED=true but TLS_CERTFILE or TLS_KEYFILE missing; starting without TLS", file=sys.stderr)
        return None
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # Restrict to TLS 1.2+ with preference for TLS 1.3 when available
    try:
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        # If Python/OpenSSL supports TLS 1.3, it will be used automatically
    except Exception:
        pass
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    if ca_certs:
        try:
            context.load_verify_locations(cafile=ca_certs)
        except Exception as e:
            print(f"Warning: failed to load CA certs: {e}", file=sys.stderr)
    return context


def main():
    parser = argparse.ArgumentParser(description="Start DeepFace Ray API")
    parser.add_argument("--host", default="0.0.0.0", help="Host address")
    parser.add_argument("--port", type=int, default=8000, help="Port number")
    parser.add_argument("--workers", type=int, default=4, help="Number of Ray workers")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--ray-address", default="auto", help="Ray cluster address")
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

    if not args.no_ray:
        try:
            # Test Ray connection
            if args.ray_address == "auto":
                print("Initializing Ray in local mode...")
            else:
                print(f"Connecting to Ray cluster at {args.ray_address}...")

            ray.init(address=args.ray_address, ignore_reinit_error=True)
            print(f"Ray initialized successfully")
            ray.shutdown()
        except Exception as e:
            print(f"Warning: Ray initialization failed: {e}")
            print("The API will still start but without Ray acceleration")

    ssl_ctx = _build_ssl_context_from_env()

    # Start the server
    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.debug,
        workers=1,  # Always use 1 uvicorn worker with Ray
        log_level="debug" if args.debug else "info",
        ssl=ssl_ctx,
    )


if __name__ == "__main__":
    main()
