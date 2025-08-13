#!/usr/bin/env python3
"""
Simple test client for DeepFace Ray API

Usage: python test_client.py
"""

import requests
import json
import time
from PIL import Image, ImageDraw
import io
import os

BASE_URL = "http://localhost:8000"

def create_test_image(size=(300, 300), face_color="lightblue", filename=None):
    """Create a simple test image with a face-like shape."""
    image = Image.new('RGB', size, 'white')
    draw = ImageDraw.Draw(image)

    # Draw a simple face-like oval
    face_bbox = (50, 50, size[0]-50, size[1]-50)
    draw.ellipse(face_bbox, fill=face_color, outline='black', width=2)

    # Draw eyes
    eye_size = 20
    left_eye = (100, 120, 120, 140)
    right_eye = (180, 120, 200, 140)
    draw.ellipse(left_eye, fill='black')
    draw.ellipse(right_eye, fill='black')

    # Draw mouth
    mouth = (130, 180, 170, 200)
    draw.arc(mouth, start=0, end=180, fill='black', width=3)

    if filename:
        image.save(filename, 'JPEG')

    # Convert to bytes
    img_bytes = io.BytesIO()
    image.save(img_bytes, format='JPEG')
    return img_bytes.getvalue()

def test_health():
    """Test health endpoint."""
    print("\n=== Testing Health Endpoint ===")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_models():
    """Test models endpoint."""
    print("\n=== Testing Models Endpoint ===")
    try:
        response = requests.get(f"{BASE_URL}/models")
        print(f"Status: {response.status_code}")
        print(f"Available models: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_analyze():
    """Test face analysis endpoint."""
    print("\n=== Testing Face Analysis ===")
    try:
        # Create test image
        test_img = create_test_image(face_color="lightgreen")

        files = {'image': ('test.jpg', test_img, 'image/jpeg')}

        response = requests.post(f"{BASE_URL}/analyze", files=files)
        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print(f"Analysis results: {json.dumps(result, indent=2)}")
        else:
            print(f"Error: {response.text}")

        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_verify():
    """Test face verification endpoint."""
    print("\n=== Testing Face Verification ===")
    try:
        # Create two test images
        img1 = create_test_image(face_color="lightblue")
        img2 = create_test_image(face_color="lightcoral")

        files = {
            'img1': ('test1.jpg', img1, 'image/jpeg'),
            'img2': ('test2.jpg', img2, 'image/jpeg')
        }

        response = requests.post(f"{BASE_URL}/verify", files=files)
        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print(f"Verification result: {json.dumps(result, indent=2)}")
        else:
            print(f"Error: {response.text}")

        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_extract_embedding():
    """Test embedding extraction endpoint."""
    print("\n=== Testing Embedding Extraction ===")
    try:
        # Create test image
        test_img = create_test_image(face_color="lightyellow")

        files = {'image': ('test.jpg', test_img, 'image/jpeg')}

        response = requests.post(f"{BASE_URL}/extract-embedding", files=files)
        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print(f"Embedding length: {len(result.get('embedding', []))}")
            print(f"Facial area: {result.get('facial_area', {})}")
        else:
            print(f"Error: {response.text}")

        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    """Run all tests."""
    print("DeepFace Ray API Test Client")
    print("="*50)

    tests = [
        ("Health Check", test_health),
        ("Models", test_models),
        ("Face Analysis", test_analyze),
        ("Face Verification", test_verify),
        ("Embedding Extraction", test_extract_embedding),
    ]

    results = {}

    for test_name, test_func in tests:
        print(f"\nRunning {test_name}...")
        start_time = time.time()

        try:
            success = test_func()
            duration = time.time() - start_time
            results[test_name] = {"success": success, "duration": duration}
        except Exception as e:
            results[test_name] = {"success": False, "error": str(e)}

    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)

    for test_name, result in results.items():
        status = "✅ PASS" if result["success"] else "❌ FAIL"
        duration = f"({result.get('duration', 0):.2f}s)" if "duration" in result else ""
        print(f"{test_name:20} {status} {duration}")
        if "error" in result:
            print(f"                     Error: {result['error']}")

    total_tests = len(results)
    passed_tests = sum(1 for r in results.values() if r["success"])
    print(f"\nTotal: {passed_tests}/{total_tests} tests passed")

if __name__ == "__main__":
    main()
