import os
import sys
import snowflake.connector

# ----------------------------
# Constants
# ----------------------------

MANIFEST_ROLE = "ANU_DEVOPS_FR_PROD_TASK_ETL"  # Hardcoded role for manifest updates
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


def connect_to_snowflake(role, database=None):
    db = database if database else os.environ["SNOWFLAKE_DATABASE"]
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=role,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=db,
        insecure_mode=True,
    )


def infer_schema_and_repo():
    repo = os.getenv("GITHUB_REPOSITORY", "")
    schema = repo.split(".", 1)[1] if "." in repo else "PUBLIC"
    return schema, repo


def fetch_validated_sql_files(conn, schema, repo_name):
    """Get all SQL files validated by validate.py for this repo and commit"""
    sql = f"""
        SELECT sql_file
        FROM {MANIFEST_TABLE}
        WHERE repository = %s
          AND schema = %s
          AND status = 'VALIDATED'
        ORDER BY validated_at
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (repo_name, schema))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def run_sql_file(conn, file_path):
    with open(file_path, "r") as f:
        sql_content = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql_content)
        print(f"✅ Executed {file_path}")
    finally:
        cur.close()


def mark_as_deployed(conn, repo_name, commit_sha, sql_file):
    """Update manifest table to mark SQL file as deployed"""
    schema, _ = infer_schema_and_repo()
    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET deployed_at = CURRENT_TIMESTAMP(),
            status = 'DEPLOYED'
        WHERE repository = %s
          AND schema = %s
          AND sql_file = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (repo_name, schema, sql_file))
        print(f"📘 Marked {sql_file} as DEPLOYED")
    finally:
        cur.close()

# ----------------------------
# Main
# ----------------------------

def main():
    require_env_vars()

    commit_sha = os.getenv("GITHUB_SHA")
    schema, repo_name = infer_schema_and_repo()

    # 1️⃣ Deployment phase (role from secrets)
    deploy_conn = connect_to_snowflake(os.environ["SNOWFLAKE_ROLE"])

    sql_files = fetch_validated_sql_files(deploy_conn, schema, repo_name)
    if not sql_files:
        print("ℹ️ No validated SQL files found for deployment")
        deploy_conn.close()
        return

    print(f"🚀 Deploying {len(sql_files)} SQL files to PRODUCTION")

    for sql_file in sql_files:
        run_sql_file(deploy_conn, sql_file)

    deploy_conn.close()

    # 2️⃣ Manifest phase (hardcoded role, LAST STEP)
    manifest_conn = connect_to_snowflake(MANIFEST_ROLE)

    for sql_file in sql_files:
        mark_as_deployed(manifest_conn, repo_name, sql_file)

    manifest_conn.close()

    print("✅ Deployment completed and manifest updated")


if __name__ == "__main__":
    main()
