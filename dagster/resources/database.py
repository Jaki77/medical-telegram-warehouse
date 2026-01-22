"""
Database resources for Dagster pipeline
"""
import os
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dagster import resource, Field, String

@resource(
    config_schema={
        "host": Field(String, default_value="localhost", is_required=False),
        "port": Field(String, default_value="5432", is_required=False),
        "database": Field(String, default_value="medical_warehouse", is_required=False),
        "username": Field(String, default_value="postgres", is_required=False),
        "password": Field(String, default_value="postgres123", is_required=False),
    }
)
def postgres_resource(context):
    """Resource for PostgreSQL database connection"""
    config = context.resource_config
    
    connection_string = (
        f"postgresql://{config['username']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=300)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    class PostgreSQLConnection:
        def __init__(self):
            self.engine = engine
            self.SessionLocal = SessionLocal
        
        @contextmanager
        def get_session(self) -> Generator[Session, None, None]:
            """Get database session"""
            session = self.SessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        
        def execute_query(self, query: str, params: dict = None):
            """Execute raw SQL query"""
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        
        def check_connection(self) -> bool:
            """Check database connection"""
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            except Exception as e:
                context.log.error(f"Database connection failed: {e}")
                return False
    
    return PostgreSQLConnection()


@resource(
    config_schema={
        "docker_compose_path": Field(String, default_value="docker-compose.yml", is_required=False),
    }
)
def docker_resource(context):
    """Resource for Docker operations"""
    import subprocess
    import time
    from pathlib import Path
    
    class DockerManager:
        def __init__(self, docker_compose_path: str):
            self.docker_compose_path = Path(docker_compose_path)
            self.context = context
        
        def start_services(self, services: list = None):
            """Start Docker services"""
            cmd = ["docker-compose", "-f", str(self.docker_compose_path), "up", "-d"]
            if services:
                cmd.extend(services)
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    self.context.log.info("Docker services started successfully")
                    return True
                else:
                    self.context.log.error(f"Failed to start Docker services: {result.stderr}")
                    return False
            except Exception as e:
                self.context.log.error(f"Docker command failed: {e}")
                return False
        
        def stop_services(self):
            """Stop Docker services"""
            cmd = ["docker-compose", "-f", str(self.docker_compose_path), "down"]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    self.context.log.info("Docker services stopped successfully")
                    return True
                else:
                    self.context.log.error(f"Failed to stop Docker services: {result.stderr}")
                    return False
            except Exception as e:
                self.context.log.error(f"Docker command failed: {e}")
                return False
        
        def check_service_health(self, service_name: str, timeout: int = 30) -> bool:
            """Check if a Docker service is healthy"""
            cmd = ["docker", "ps", "--filter", f"name={service_name}", "--format", "{{.Status}}"]
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if "Up" in result.stdout:
                        self.context.log.info(f"Service {service_name} is running")
                        return True
                    time.sleep(2)
                except Exception as e:
                    self.context.log.error(f"Failed to check service {service_name}: {e}")
            
            self.context.log.error(f"Service {service_name} failed to start within {timeout} seconds")
            return False
    
    return DockerManager(context.resource_config["docker_compose_path"])