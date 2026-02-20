"""
Spark PostgreSQL utilities for campaign analytics pipeline.
"""
from pyspark.sql import SparkSession
import yaml
import os
from dotenv import load_dotenv
from string import Template

# Load environment variables from .env file
load_dotenv()


def load_config():
    """
    Load database configuration with environment variable substitution.
    
    Returns:
        dict: Configuration with env vars substituted
    """
    config_path = os.path.join(os.path.dirname(__file__), '../../config/database.yaml')
    
    with open(config_path, 'r') as f:
        yaml_content = f.read()
    
    # Substitute environment variables (${VAR_NAME} syntax)
    template = Template(yaml_content)
    substituted = template.safe_substitute(os.environ)
    
    # Parse YAML
    config = yaml.safe_load(substituted)
    
    return config


def create_spark_session(app_name="Campaign Analytics"):
    """
    Create Spark session with PostgreSQL support
    
    Args:
        app_name: Name for the Spark application
    
    Returns:
        SparkSession: Configured Spark session
    """
    config = load_config()
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", config['postgres']['jar_path']) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    # Set log level to reduce noise 
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def write_to_postgres(df, table_name, schema, mode="append"):
    """
    Write DataFrame to PostgreSQL
    
    Args:
        df: Spark DataFrame to write
        table_name: Target table name
        schema: Target schema (bronze/silver/gold)
        mode: Write mode (append/overwrite)
    """
    config = load_config()
    jdbc_url = config['postgres']['jdbc_url']
    properties = {
        "user": config['postgres']['user'],
        "password": config['postgres']['password'],
        "driver": config['postgres']['driver']
    }
    
    full_table_name = f"{schema}.{table_name}"
    print(f"✍️  Writing to PostgreSQL: {full_table_name}")
    print(f"   Mode: {mode}")
    print(f"   Rows: {df.count()}")
    
    df.write.jdbc(
        url=jdbc_url,
        table=full_table_name,
        mode=mode,
        properties=properties
    )
    
    print(f"✅ Successfully wrote to {full_table_name}\n")


def read_from_postgres(spark, table_name, schema):
    """Read DataFrame from PostgreSQL"""
    config = load_config()
    jdbc_url = config['postgres']['jdbc_url']
    properties = {
        "user": config['postgres']['user'],
        "password": config['postgres']['password'],
        "driver": config['postgres']['driver']
    }
    
    full_table_name = f"{schema}.{table_name}"
    print(f"📖 Reading from PostgreSQL: {full_table_name}")
    
    df = spark.read.jdbc(
        url=jdbc_url,
        table=full_table_name,
        properties=properties
    )
    
    print(f"✅ Read {df.count()} rows from {full_table_name}\n")
    return df


def get_postgres_properties():
    """
    Get PostgreSQL connection properties for JDBC writes.
    
    Returns:
        dict: Connection properties including url, user, password, driver
    """
    config = load_config()
    return {
        "url": config['postgres']['jdbc_url'],
        "user": config['postgres']['user'],
        "password": config['postgres']['password'],
        "driver": config['postgres']['driver']
    }


def truncate_and_write(df, table_name, schema):
    """
    Truncate table and write new data (preserves table structure and dependencies).
    
    Args:
        df: Spark DataFrame to write
        table_name: Target table name
        schema: Target schema (bronze/silver/gold)
    """
    import psycopg2
    from psycopg2 import sql
    
    config = load_config()
    full_table_name = f"{schema}.{table_name}"
    
    # Connect to Postgres and truncate
    conn = psycopg2.connect(
        host=config['postgres']['host'],
        port=config['postgres']['port'],
        database=config['postgres']['database'],
        user=config['postgres']['user'],
        password=config['postgres']['password']
    )
    
    try:
        cursor = conn.cursor()
        truncate_sql = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
            sql.Identifier(schema, table_name)
        )
        print(f"   Truncating: {full_table_name}")
        cursor.execute(truncate_sql)
        conn.commit()
        cursor.close()
        print(f"   ✅ Truncate successful")
    except Exception as e:
        print(f"   ⚠️  Truncate failed (table may not exist yet): {e}")
    finally:
        conn.close()
    
    # Write data with append mode
    properties = get_postgres_properties()
    print(f"   Writing {df.count()} rows...")
    
    df.write.jdbc(
        url=properties["url"],
        table=full_table_name,
        mode="append",
        properties=properties
    )
    
    print(f"✅ Successfully wrote to {full_table_name}\n")