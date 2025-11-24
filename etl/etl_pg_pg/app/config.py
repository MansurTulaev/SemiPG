import os
import psycopg2

class DatabaseConfig:
    SOURCE_DB_URL = os.getenv("SOURCE_DB_URL", "postgresql://postgres:password@postgres-source:5432/source_db")
    TARGET_DB_URL = os.getenv("TARGET_DB_URL", "postgresql://postgres:password@postgres-target:5432/target_db")
    
    @staticmethod
    def get_source_connection():
        return psycopg2.connect(DatabaseConfig.SOURCE_DB_URL)
    
    @staticmethod
    def get_target_connection():
        return psycopg2.connect(DatabaseConfig.TARGET_DB_URL)