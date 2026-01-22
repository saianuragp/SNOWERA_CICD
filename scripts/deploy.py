import os
import sys
import snowflake.connector
import json

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "CHANGED_SQL_FILES_ARTIFACT"
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
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        insecure_mode=True
    )

def get_sql_files_from_artifact(artifact_path):
    """Read JSON file produced by PREPROD validate workflow"""
    if not os.path.exists(artifact_path):
        print(f"❌ Artifact file not found: {artifact_path}")
        sys.exit(1)
    with open(artifact_path, "r") as f:
        files = json.load(f)
    return files

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
    require_env_vars()
    conn = connect_to_snowflake()

    artifact_path = os.environ["CHANGED_SQL_FILES_ARTIFACT"]
    sql_files = get_sql_files_from_artifact(artifact_path)

    if not sql_files:
        print("ℹ️ No SQL changes detected to deploy")
        conn.close()
        sys.exit(0)

    print(f"🚀 Deploying {len(sql_files)} SQL files to PROD")
    for sql_file in sql_files:
        print(f"▶ Executing {sql_file}")
        run_sql_file(conn, sql_file)

    print("✅ Deployment to PROD completed successfully")
    conn.close()

if __name__ == "__main__":
    main()
