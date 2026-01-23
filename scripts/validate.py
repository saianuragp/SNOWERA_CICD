import os
import sys
import subprocess
import snowflake.connector

# ----------------------------
# Constants
# ----------------------------

MANIFEST_ROLE = "ANU_DEVOPS_FR_PROD_TASK_ETL"
MANIFEST_TABLE = "ANU_DEVOPS_DB_PROD.SNOWERA_DEPLOYMENTS.GITHUB_DEPLOYMENT_LOG"

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

# ----------------------------
# Helpers
# ----------------------------

def require_env_vars():
    for var in REQUIRED_VARS:
        if not os.getenv(var):
            print(f"❌ Missing environment variable: {var}")
            sys.exit(1)


def connect_to_snowflake(role):
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=role,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        insecure_mode=True,
    )


def infer_schema_and_repo():
    repo = os.getenv("GITHUB_REPOSITORY", "")
    # schema = text after first period, repo_name = full repo
    schema = repo.split(".", 1)[1] if "." in repo else "PUBLIC"
    return schema, repo


def get_changed_sql_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.splitlines() if f.endswith(".sql")]


def run_sql_file(conn, file_path):
    with open(file_path, "r") as f:
        sql_content = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql_content)
        print(f"✅ Executed {file_path}")
    finally:
        cur.close()


def insert_manifest_record(conn, repo_name, schema, sql_file):
    sql = f"""
        INSERT INTO {MANIFEST_TABLE}
        (
            repository,
            schema,
            sql_file,
            validated_at,
            deployed_at,
            status
        )
        VALUES
        (
            %s, %s, %s,
            CURRENT_TIMESTAMP(),
            NULL,
            'VALIDATED'
        )
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (repo_name, schema, sql_file))
        print(f"📘 Manifest record inserted for {sql_file}")
    finally:
        cur.close()

# ----------------------------
# Main
# ----------------------------

def main():
    require_env_vars()

    commit_sha = os.getenv("GITHUB_SHA")
    schema, repo_name = infer_schema_and_repo()

    # 1️⃣ Validation phase (role from secrets)
    validation_conn = connect_to_snowflake(os.environ["SNOWFLAKE_ROLE"])

    sql_files = get_changed_sql_files()
    if not sql_files:
        print("ℹ️ No SQL changes detected")
        validation_conn.close()
        return

    print(f"🧪 Validating {len(sql_files)} SQL files")

    for sql_file in sql_files:
        run_sql_file(validation_conn, sql_file)

    validation_conn.close()

    # 2️⃣ Manifest phase (hardcoded role, LAST STEP)
    manifest_conn = connect_to_snowflake(MANIFEST_ROLE)

    for sql_file in sql_files:
        insert_manifest_record(
            manifest_conn,
            repo_name,
            schema,
            os.path.basename(sql_file),
        )

    manifest_conn.close()

    print("✅ Validation completed and manifest recorded")


if __name__ == "__main__":
    main()
