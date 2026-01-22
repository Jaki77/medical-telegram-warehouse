"""
Task 4: Run FastAPI Application
"""
import subprocess
import sys
import time
from pathlib import Path

def check_dependencies():
    """Check if required packages are installed"""
    print("\n" + "="*60)
    print("DEPENDENCY CHECK")
    print("="*60)
    
    try:
        import fastapi
        print("✓ FastAPI installed")
    except:
        print("✗ FastAPI not installed. Run: pip install fastapi")
        return False
    
    try:
        import uvicorn
        print("✓ Uvicorn installed")
    except:
        print("✗ Uvicorn not installed. Run: pip install uvicorn[standard]")
        return False
    
    try:
        import sqlalchemy
        print("✓ SQLAlchemy installed")
    except:
        print("✗ SQLAlchemy not installed. Run: pip install sqlalchemy")
        return False
    
    try:
        import redis
        print("✓ Redis client installed")
    except:
        print("⚠ Redis not installed (optional). Run: pip install redis")
    
    return True

def start_api():
    """Start the FastAPI application"""
    print("\n" + "="*60)
    print("STARTING MEDICAL TELEGRAM ANALYTICS API")
    print("="*60)
    
    # Check if API directory exists
    if not Path("api").exists():
        print("✗ API directory not found. Make sure you're in the project root.")
        return False
    
    # Start the API server
    print("Starting FastAPI server on http://localhost:8000")
    print("Press Ctrl+C to stop")
    print("\nAccess endpoints:")
    print("  • API Documentation: http://localhost:8000/docs")
    print("  • Health Check: http://localhost:8000/health")
    print("  • Top Products: http://localhost:8000/api/reports/top-products")
    print("  • Channel Activity: http://localhost:8000/api/channels/{channel_name}/activity")
    print("  • Message Search: http://localhost:8000/api/search/messages?query=paracetamol")
    print("  • Visual Content: http://localhost:8000/api/reports/visual-content")
    print("\n" + "="*60)
    
    try:
        import uvicorn
        uvicorn.run(
            "api.main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
        return True
    except KeyboardInterrupt:
        print("\nAPI server stopped by user")
        return True
    except Exception as e:
        print(f"✗ Failed to start API: {e}")
        return False

def test_endpoints():
    """Test API endpoints"""
    print("\n" + "="*60)
    print("TESTING API ENDPOINTS")
    print("="*60)
    
    import httpx
    import time
    
    base_url = "http://localhost:8000"
    endpoints = [
        "/health",
        "/api/reports/top-products?limit=5",
        "/api/reports/visual-content",
        "/api/search/messages?query=test&limit=5"
    ]
    
    for endpoint in endpoints:
        try:
            print(f"Testing {endpoint}...")
            response = httpx.get(f"{base_url}{endpoint}", timeout=10)
            
            if response.status_code == 200:
                print(f"✓ {endpoint}: {response.status_code}")
            else:
                print(f"✗ {endpoint}: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"✗ {endpoint}: Error - {e}")
        
        time.sleep(0.5)  # Avoid rate limiting
    
    print("\n" + "="*60)
    print("API TESTING COMPLETE")
    print("="*60)

def main():
    """Main execution function"""
    print("="*60)
    print("TASK 4: ANALYTICAL API WITH FASTAPI")
    print("="*60)
    
    # Check dependencies
    if not check_dependencies():
        print("\n⚠ Please install missing dependencies before continuing.")
        return False
    
    # Create necessary directories
    Path("logs").mkdir(exist_ok=True)
    
    print("\nOptions:")
    print("1. Start API server")
    print("2. Test endpoints (requires running server)")
    print("3. Run with Docker")
    print("4. Exit")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        return start_api()
    elif choice == "2":
        return test_endpoints()
    elif choice == "3":
        print("\nStarting with Docker Compose...")
        subprocess.run(["docker-compose", "-f", "docker-compose.api.yml", "up", "-d"])
        print("Services started. Access API at http://localhost:8000")
        return True
    elif choice == "4":
        print("Exiting.")
        return True
    else:
        print("Invalid choice.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)