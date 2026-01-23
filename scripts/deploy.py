import os
import sys
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
# Common helpers
# ----------------------------

def require_env_vars():
    for var in REQUIRED_VARS:
        if not os.getenv(var):
            print(f"❌ Missing env var: {var}")
            sys.exit(1)


def infer_schema():
    """
    Schema name comes from repo name after '.'
    Example: ANUOPS.STG_MASTER_DATA -> STG_MASTER_DATA
    """
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if "." not in repo:
        print("❌ Repo name does not contain schema delimiter '.'")
        sys.exit(1)
    return repo.split(".", 1)[1]


def connect_snowflake(role_override=None):
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=role_override or os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        insecure_mode=True,
    )

# ----------------------------
# Manifest-driven logic
# ----------------------------

# -- commit_sha = %s

def fetch_validated_sql_files(conn):
    commit_sha = os.getenv("GITHUB_SHA")
    database = os.environ["SNOWFLAKE_DATABASE"]
    schema = infer_schema()

    sql = f"""
        SELECT SQL_FILE
        FROM {MANIFEST_TABLE}
        WHERE database = %s
          AND schema = %s
          AND status = 'VALIDATED'
        ORDER BY validated_at
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (database, schema))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def execute_sql_file(conn, sql_file):
    if not os.path.exists(sql_file):
        print(f"❌ SQL file missing in repo: {sql_file}")
        sys.exit(1)

    with open(sql_file, "r") as f:
        sql_content = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql_content)
        print(f"🚀 Deployed {sql_file}")
    except Exception as e:
        print(f"❌ Deployment failed for {sql_file}: {e}")
        raise
    finally:
        cur.close()

# ----------------------------
# Manifest update (LAST STEP)
# Runs with different role
# ----------------------------

def update_manifest_record(sql_file):
    conn = connect_snowflake(role_override=MANIFEST_ROLE)

    commit_sha = os.getenv("GITHUB_SHA")
    database = os.environ["SNOWFLAKE_DATABASE"]
    schema = infer_schema()

    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET
            deployed_at = CURRENT_TIMESTAMP(),
            status = 'DEPLOYED'
        WHERE database = %s
          AND schema = %s
          AND sql_file = %s
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (database, schema, sql_file))
        print(f"📘 Manifest updated for {sql_file}")
    finally:
        cur.close()
        conn.close()

# ----------------------------
# Main
# ----------------------------

def main():
    require_env_vars()

    deploy_conn = connect_snowflake()

    sql_files = fetch_validated_sql_files(deploy_conn)
    if not sql_files:
        print("ℹ️ No validated SQL files found for deployment")
        deploy_conn.close()
        return

    print(f"🚀 Deploying {len(sql_files)} SQL files")

    for sql_file in sql_files:
        execute_sql_file(deploy_conn, sql_file)
        update_manifest_record(sql_file)  # MUST be last & separate role

    deploy_conn.close()
    print("✅ Deployment completed successfully")


if __name__ == "__main__":
    main()
