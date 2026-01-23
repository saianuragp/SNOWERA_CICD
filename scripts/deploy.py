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
# Helpers
# ----------------------------

def deploy_header():
    return r"""
            !
            !
            *
          /   \
        /_______\
       |=       =|
       |         |
       |   ||    |
       |         |
       |         |
       |         |
       |         |
      /  |####|   \
     /   |####|    \
    |  / ^  |  ^ \  |
    | /  (  |  )  \ |
    |/   (  |  )   \|
       ((       ))
       ((   =   ))
       ((   =   ))
       ((       ))
        ((     ))
          (   )
            *
            *
    """

def print_deploy_summary():
    """
    Print a summary of the Snowflake environment for validation.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT", "<not set>")
    user = os.getenv("SNOWFLAKE_USER", "<not set>")
    role = os.getenv("SNOWFLAKE_ROLE", "<not set>")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "<not set>")
    database = os.getenv("SNOWFLAKE_DATABASE", "<not set>")
    schema = infer_schema()
    
    print("\n" + "="*60)
    print("🚀  DEPLOY SUMMARY  🚀")
    print("="*60)
    print(f"Snowflake Account  : {account}")
    print(f"Snowflake User     : {user}")
    print(f"Role               : {role}")
    print(f"Warehouse          : {warehouse}")
    print(f"Database           : {database}")
    print(f"Schema             : {schema}")
    print("="*60 + "\n")

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


def infer_schema():
    """
    Schema derived from repo name after the first '.'
    """
    repo = os.getenv("GITHUB_REPOSITORY", "")
    return repo.split(".", 1)[1] if "." in repo else "PUBLIC"


def get_repository_name():
    """
    Repo name without org
    """
    return os.getenv("GITHUB_REPOSITORY", "").split("/")[-1]


def fetch_validated_sql_files(conn, repository, schema):
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
        cur.execute(sql, (repository, schema))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def run_sql_file(conn, file_path):
    if not os.path.exists(file_path):
        print("=" * 60)
        print(f"❌ SQL file not found in repo: {file_path}")
        print("=" * 60)
        sys.exit(1)

    with open(file_path, "r") as f:
        sql_content = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql_content)
        print("=" * 60)
        print(f"🚀 Deployed {file_path}")
        print("=" * 60)
    finally:
        cur.close()


def mark_as_deployed(conn, repository, schema, sql_file):
    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET
            deployed_at = CURRENT_TIMESTAMP(),
            status = 'DEPLOYED'
        WHERE repository = %s
          AND schema = %s
          AND sql_file = %s
          AND status = 'VALIDATED'
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (repository, schema, sql_file))
        print("=" * 60)
        print(f"📘 Manifest updated for {sql_file}")
        print("=" * 60)
    finally:
        cur.close()

# ----------------------------
# Main
# ----------------------------

def main():
    print(deploy_header())
    require_env_vars()
    print_deploy_summary()

    repository = get_repository_name()
    schema = infer_schema()

    # 1️⃣ Fetch validated SQL (manifest role)
    manifest_conn = connect_to_snowflake(MANIFEST_ROLE)

    sql_files = fetch_validated_sql_files(
        manifest_conn,
        repository,
        schema
    )

    if not sql_files:
        print("ℹ️ No validated SQL files found for deployment")
        manifest_conn.close()
        return

    print(f"🚀 Deploying {len(sql_files)} SQL files to PRODUCTION")

    # 2️⃣ Deploy phase (role from secrets)
    deploy_conn = connect_to_snowflake(os.environ["SNOWFLAKE_ROLE"])

    for sql_file in sql_files:
        run_sql_file(deploy_conn, sql_file)
        mark_as_deployed(
            manifest_conn,
            repository,
            schema,
            sql_file
        )

    deploy_conn.close()
    manifest_conn.close()

    print("✅ Deployment completed successfully")


if __name__ == "__main__":
    main()
