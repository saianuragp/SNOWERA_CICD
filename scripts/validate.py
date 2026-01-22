import os
import sys
import subprocess
import snowflake.connector

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

def require_env_vars():
    """Ensure all required environment variables exist"""
    for var in REQUIRED_VARS:
        val = os.getenv(var)
        if not val:
            print(f"❌ Missing env var: {var}")
            sys.exit(1)

def connect_to_snowflake():
    """Create and return a Snowflake connection"""
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        ocsp_fail_open=True
    )
    return conn

def get_changed_sql_files():
    """Return a list of changed SQL files relative to main"""
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True,
        text=True,
        check=True
    )
    return [f for f in result.stdout.splitlines() if f.endswith(".sql")]

def run_sql_file(conn, file_path):
    """Execute a SQL file using the given Snowflake connection"""
    with open(file_path, "r") as f:
        sql_content = f.read()

    try:
        cs = conn.cursor()
        cs.execute(sql_content)
        print(f"✅ Successfully executed {file_path}")
    except Exception as e:
        print(f"❌ Error executing {file_path}: {e}")
        raise
    finally:
        cs.close()

def main():
    # Validate environment variables
    require_env_vars()

    # Connect to Snowflake
    conn = connect_to_snowflake()

    # Detect changed SQL files
    sql_files = get_changed_sql_files()
    if not sql_files:
        print("ℹ️ No SQL changes detected")
        conn.close()
        sys.exit(0)

    print(f"🧪 Validating {len(sql_files)} SQL files in PREPROD")
    for sql_file in sql_files:
        print(f"▶ Executing {sql_file}")
        run_sql_file(conn, sql_file)

    print("✅ Validation completed successfully")
    conn.close()

if __name__ == "__main__":
    main()
