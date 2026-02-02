from pyspark.sql import SparkSession
import yaml
import os

def load_config():
    """Load database configuration"""
    config_path = os.path.join(os.path.dirname(__file__), '../../config/database.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

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

    # set log level to reduce noise 
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

def get_jdbc_properties():
    """Get JDBC connection properties as dict"""
    config = load_config()
    
    return {
        "url": config['jdbc_url'],
        "user": config['postgres']['user'],
        "password": config['postgres']['password'],
        "driver": config['postgres']['driver']
    }
