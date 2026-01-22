#!/usr/bin/env python3
"""
Test script for FastAPI endpoints
"""
import pytest
import httpx
import asyncio
from pathlib import Path
import json

BASE_URL = "http://localhost:8000"
TEST_DATA_DIR = Path("tests/data")

def test_health_check():
    """Test health check endpoint"""
    response = httpx.get(f"{BASE_URL}/health", timeout=10)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    print("✓ Health check passed")

def test_top_products():
    """Test top products endpoint"""
    response = httpx.get(f"{BASE_URL}/api/reports/top-products?limit=5", timeout=10)
    assert response.status_code == 200
    data = response.json()
    assert "products" in data
    assert "total_mentions" in data
    assert "time_period" in data
    print("✓ Top products endpoint passed")

def test_visual_content():
    """Test visual content endpoint"""
    response = httpx.get(f"{BASE_URL}/api/reports/visual-content", timeout=10)
    assert response.status_code == 200
    data = response.json()
    assert "total_images" in data
    assert "images_by_channel" in data
    assert "images_by_category" in data
    print("✓ Visual content endpoint passed")

def test_channel_activity():
    """Test channel activity endpoint"""
    # First get a channel name
    response = httpx.get(f"{BASE_URL}/api/channels", timeout=10)
    if response.status_code == 200:
        channels = response.json()
        if channels:
            channel_name = channels[0]["channel_name"]
            response = httpx.get(
                f"{BASE_URL}/api/channels/{channel_name}/activity?days=7", 
                timeout=10
            )
            assert response.status_code in [200, 404]  # 404 if no data
            print("✓ Channel activity endpoint passed")

def test_message_search():
    """Test message search endpoint"""
    response = httpx.get(
        f"{BASE_URL}/api/search/messages?query=test&limit=5", 
        timeout=10
    )
    assert response.status_code == 200
    data = response.json()
    assert "query" in data
    assert "total_results" in data
    assert "messages" in data
    print("✓ Message search endpoint passed")

def test_api_documentation():
    """Test API documentation endpoints"""
    response = httpx.get(f"{BASE_URL}/docs", timeout=10)
    assert response.status_code == 200
    print("✓ API documentation available")

    response = httpx.get(f"{BASE_URL}/openapi.json", timeout=10)
    assert response.status_code == 200
    data = response.json()
    assert "openapi" in data
    assert "info" in data
    print("✓ OpenAPI schema available")

def run_all_tests():
    """Run all API tests"""
    print("="*60)
    print("RUNNING API TESTS")
    print("="*60)
    
    tests = [
        test_health_check,
        test_top_products,
        test_visual_content,
        test_channel_activity,
        test_message_search,
        test_api_documentation
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")
    
    if failed == 0:
        print("\n✓ All API tests passed!")
        return True
    else:
        print("\n⚠ Some tests failed")
        return False

if __name__ == "__main__":
    # Check if API is running
    try:
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            run_all_tests()
        else:
            print("⚠ API is not running. Start it first with: python run_api.py")
    except httpx.ConnectError:
        print("⚠ API is not running. Start it first with: python run_api.py")