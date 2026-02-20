"""
Master Pipeline Runner - Complete Lakehouse ETL
Executes: Bronze (MinIO) → Silver (MinIO + Postgres) → Gold (dbt)
"""
import subprocess
import sys
from datetime import datetime
from pathlib import Path


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(message):
    """Print formatted header"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{message.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}\n")


def print_step(step_num, message):
    """Print formatted step"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}[Step {step_num}] {message}{Colors.END}")


def print_success(message):
    """Print success message"""
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")


def print_error(message):
    """Print error message"""
    print(f"{Colors.RED}❌ {message}{Colors.END}")


def print_warning(message):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")


def run_command(command, description):
    """
    Run a shell command and handle errors
    
    Args:
        command: Command to run (string or list)
        description: Description of what the command does
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n   Running: {description}")
    
    try:
        if isinstance(command, str):
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
        else:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True
            )
        
        if result.stdout:
            print(result.stdout)
        
        print_success(f"{description} completed")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"{description} failed")
        print(f"\n{Colors.RED}Error output:{Colors.END}")
        print(e.stderr if e.stderr else e.stdout)
        return False


def run_bronze_layer():
    """Execute Bronze layer ingestion"""
    print_step(1, "BRONZE LAYER - Ingest to MinIO + Postgres")
    
    scripts = [
        ("python pipeline/bronze/ingest_campaigns.py", "Campaigns ingestion"),
        ("python pipeline/bronze/ingest_performance.py", "Performance ingestion"),
        ("python pipeline/bronze/ingest_advertisers.py", "Advertisers ingestion"),
    ]
    
    for command, description in scripts:
        if not run_command(command, description):
            return False
    
    print_success("Bronze layer complete - Data in MinIO bronze/ bucket")
    return True


def run_silver_layer():
    """Execute Silver layer transformations"""
    print_step(2, "SILVER LAYER - Transform and Validate")
    
    scripts = [
        ("python pipeline/silver/clean_campaigns.py", "Campaigns transformation"),
        ("python pipeline/silver/clean_performance.py", "Performance transformation"),
        ("python pipeline/silver/clean_advertisers.py", "Advertisers transformation"),
    ]
    
    for command, description in scripts:
        if not run_command(command, description):
            return False
    
    print_success("Silver layer complete - Data in MinIO silver/ bucket + Postgres")
    return True


def run_gold_layer():
    """Execute Gold layer dbt models"""
    print_step(3, "GOLD LAYER - dbt Analytics Models")
    
    # Change to dbt directory
    dbt_dir = Path("dbt_project")
    
    if not dbt_dir.exists():
        print_error(f"dbt directory not found: {dbt_dir}")
        return False
    
    commands = [
        (f"cd {dbt_dir} && dbt run", "dbt models execution"),
        (f"cd {dbt_dir} && dbt test", "dbt data quality tests"),
    ]
    
    for command, description in commands:
        if not run_command(command, description):
            return False
    
    print_success("Gold layer complete - Analytics models in Postgres")
    return True


def verify_infrastructure():
    """Verify Docker containers are running"""
    print_step(0, "INFRASTRUCTURE CHECK")
    
    # Check Postgres
    postgres_check = run_command(
        "docker exec campaign_analytics_db pg_isready -U dbt_user",
        "PostgreSQL health check"
    )
    
    # Check MinIO
    minio_check = run_command(
        "docker exec campaign_minio mc alias list local",
        "MinIO health check"
    )
    
    if not (postgres_check and minio_check):
        print_error("Infrastructure check failed!")
        print_warning("Run: docker-compose up -d")
        return False
    
    print_success("All infrastructure ready")
    return True


def print_summary(start_time, success):
    """Print pipeline execution summary"""
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print_header("PIPELINE EXECUTION SUMMARY")
    
    print(f"Start Time:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration:    {duration:.2f} seconds")
    print(f"Status:      {'SUCCESS ✅' if success else 'FAILED ❌'}")
    
    if success:
        print("\n" + "=" * 80)
        print("DATA FLOW COMPLETE:")
        print("=" * 80)
        print("📂 CSV Files")
        print("    ↓")
        print("🟤 Bronze Layer (MinIO + Postgres)")
        print("    - MinIO: s3a://bronze/campaigns/, performance/, advertisers/")
        print("    - Postgres: bronze.raw_*")
        print("    ↓")
        print("⚪ Silver Layer (MinIO + Postgres)")
        print("    - MinIO: s3a://silver/campaigns/, performance/, advertisers/")
        print("    - Postgres: silver.campaigns, performance, advertisers")
        print("    ↓")
        print("🟡 Gold Layer (Postgres)")
        print("    - Postgres: core.*, analytics.*")
        print("=" * 80)
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 LAKEHOUSE PIPELINE SUCCESSFUL! 🎉{Colors.END}\n")
        
        print("Next steps:")
        print("  1. View MinIO data: http://localhost:9001")
        print("  2. Query Postgres:")
        print("     docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics")
        print("  3. View dbt docs:")
        print("     cd dbt_project && dbt docs generate && dbt docs serve")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}Pipeline failed. Check errors above.{Colors.END}\n")


def main():
    """Main pipeline execution"""
    start_time = datetime.now()
    
    print_header("CAMPAIGN ANALYTICS LAKEHOUSE PIPELINE")
    print(f"Timestamp: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Step 0: Verify infrastructure
        if not verify_infrastructure():
            print_summary(start_time, False)
            sys.exit(1)
        
        # Step 1: Bronze layer
        if not run_bronze_layer():
            print_summary(start_time, False)
            sys.exit(1)
        
        # Step 2: Silver layer
        if not run_silver_layer():
            print_summary(start_time, False)
            sys.exit(1)
        
        # Step 3: Gold layer
        if not run_gold_layer():
            print_summary(start_time, False)
            sys.exit(1)
        
        # Success!
        print_summary(start_time, True)
        sys.exit(0)
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Pipeline interrupted by user{Colors.END}")
        print_summary(start_time, False)
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print_summary(start_time, False)
        sys.exit(1)


if __name__ == "__main__":
    main()