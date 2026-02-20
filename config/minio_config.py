"""
MinIO configuration for campaign analytics pipeline.
Provides S3-compatible object storage for lakehouse architecture.
"""
import os
from typing import Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class MinIOConfig:
    """MinIO connection and bucket configuration."""
    
    def __init__(self):
        # Connection settings from environment variables
        self.endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
        self.secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        
        # Bucket names
        self.buckets = {
            "bronze": "bronze",
            "silver": "silver",
            "logs": "logs",
            "data_quality": "data-quality"
        }
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark configuration for S3A filesystem access to MinIO.
        
        Returns:
            Dict[str, str]: Spark configuration properties
        """
        protocol = "https" if self.secure else "http"
        
        return {
            # S3A endpoint configuration
            "spark.hadoop.fs.s3a.endpoint": f"{protocol}://{self.endpoint}",
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            
            # Enable path-style access (required for MinIO)
            "spark.hadoop.fs.s3a.path.style.access": "true",
            
            # S3A implementation
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            
            # SSL configuration
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(self.secure).lower(),
            
            # Performance optimizations
            "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100MB chunks
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.block.size": "134217728",  # 128MB
            
            # Connection pooling
            "spark.hadoop.fs.s3a.connection.maximum": "100",
            "spark.hadoop.fs.s3a.threads.max": "64",
        }
    
    def get_s3_path(self, bucket: str, key: str) -> str:
        """
        Build S3A path for Spark operations.
        
        Args:
            bucket: Bucket name (bronze, silver, etc.)
            key: Object key/path within bucket
            
        Returns:
            str: Full S3A URI (e.g., s3a://bronze/campaigns/date=2024-01-01/)
        """
        bucket_name = self.buckets.get(bucket, bucket)
        # Ensure key doesn't start with /
        key = key.lstrip("/")
        return f"s3a://{bucket_name}/{key}"
    
    def get_bucket_name(self, bucket_type: str) -> str:
        """
        Get the actual bucket name for a given type.
        
        Args:
            bucket_type: Type of bucket (bronze, silver, etc.)
            
        Returns:
            str: Actual bucket name
        """
        return self.buckets.get(bucket_type, bucket_type)


# Singleton instance
minio_config = MinIOConfig()


if __name__ == "__main__":
    # Test configuration
    print("MinIO Configuration:")
    print(f"  Endpoint: {minio_config.endpoint}")
    print(f"  Access Key: {minio_config.access_key}")
    print(f"  Buckets: {minio_config.buckets}")
    print(f"\nExample S3 paths:")
    print(f"  Bronze: {minio_config.get_s3_path('bronze', 'campaigns/date=2024-01-01')}")
    print(f"  Silver: {minio_config.get_s3_path('silver', 'campaigns/')}")