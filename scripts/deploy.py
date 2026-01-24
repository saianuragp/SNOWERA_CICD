import os
import sys
import json
import snowflake.connector

# ---------------- PATH SETUP ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "config", "snowflake_config.json"))

# ---------------- LOAD CONFIG ----------------
def load_config():
    if not os.path.exists(CONFIG_PATH):
        print(f"❌ Config file not found: {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH) as f:
        return json.load(f)

CFG = load_config()

ACCOUNT = CFG["account"]
USER = CFG["user"]
MANIFEST_ROLE = CFG["role"]
MANIFEST_TABLE = CFG["log_table"]
CONFIG_TABLE = CFG["config_table"]

PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
if not PASSWORD:
    print("❌ Missing SNOWFLAKE_PASSWORD secret")
    sys.exit(1)

REPO_NAME = os.getenv("GITHUB_REPOSITORY", "").split("/")[-1]

# ---------------- ASCII ART ----------------
def deploy_header():
    print("=" * 60)
    print("🚀 DEPLOYMENT STARTED 🚀")
    print("=" * 60)
    print(r"""
            !
            !
            *
          /   \
        /_______\
       |=========|
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
            *
    """)

# ---------------- HELPERS ----------------
def connect(role, warehouse, database):
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=role,
        warehouse=warehouse,
        database=database,
        insecure_mode=True,
    )


def get_repo_config(env):
    conn = connect(MANIFEST_ROLE, "ANU_DEVOPS_WH_XS", "ANU_DEVOPS_DB_PROD")

    sql = f"""
        SELECT warehouse,
               {env}_role,
               {env}_database,
               schema_name
        FROM {CONFIG_TABLE}
        WHERE repo_name = %s
          AND is_active = TRUE
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        print(f"❌ No config found for repo {REPO_NAME}")
        sys.exit(1)

    return row  # warehouse, role, database, schema


def fetch_latest_validated_sql_files(schema):
    """
    Fetch only SQL files from the latest validation batch (MAX(validated_at))
    """
    conn = connect(MANIFEST_ROLE, "ANU_DEVOPS_WH_XS", "ANU_DEVOPS_DB_PROD")

    sql = f"""
        SELECT sql_file
        FROM {MANIFEST_TABLE}
        WHERE repo_name = %s
          AND schema_name = %s
          AND status = 'VALIDATED'
          AND validated_at = (
              SELECT MAX(validated_at)
              FROM {MANIFEST_TABLE}
              WHERE repo_name = %s
                AND schema_name = %s
                AND status = 'VALIDATED'
          )
        ORDER BY sql_file
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, REPO_NAME, schema))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [r[0] for r in rows]


def run_sql_file(conn, file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    with open(file_path) as f:
        sql = f.read()

    print("=" * 60)
    print(f"📄 Executing SQL file: {file_path}")
    print("-" * 60)
    print(sql)
    print("=" * 60)
    
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

    print(f"✅ Deployed {file_path}")


def update_manifest_deployed(conn, schema, sql_file):
    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET status = 'DEPLOYED',
            deployed_at = CURRENT_TIMESTAMP(),
            deployed_by = CURRENT_USER(),
            error_message = NULL
        WHERE repo_name = %s
          AND schema_name = %s
          AND sql_file = %s
          AND status = 'VALIDATED'
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, sql_file))
    cur.close()

    print(f"📗 Manifest updated: {sql_file}")


def print_summary(role, warehouse, database, schema):
    print("=" * 60)
    print("🚀 DEPLOY SUMMARY 🚀")
    print("=" * 60)
    print(f"Snowflake Account  : {ACCOUNT}.snowflakecomputing.com")
    print(f"Snowflake User     : {USER}")
    print(f"Role               : {role}")
    print(f"Warehouse          : {warehouse}")
    print(f"Database           : {database}")
    print(f"Schema             : {schema}")
    print(f"Repository         : {REPO_NAME}")
    print("=" * 60)

# ---------------- MAIN ----------------
def main():
    deploy_header()

    warehouse, role, database, schema = get_repo_config("prod")
    print_summary(role, warehouse, database, schema)

    sql_files = fetch_latest_validated_sql_files(schema)
    if not sql_files:
        print("ℹ️ No validated SQL files found for latest run")
        return

    print(f"🚀 Deploying {len(sql_files)} SQL files")

    deploy_conn = connect(role, warehouse, database)

    for f in sql_files:
        try:
            run_sql_file(deploy_conn, f)
        except Exception as e:
            print(f"❌ Deployment failed for {f}: {e}")
            sys.exit(1)

    deploy_conn.close()

    manifest_conn = connect(MANIFEST_ROLE, warehouse, database)
    for f in sql_files:
        update_manifest_deployed(manifest_conn, schema, f)
    manifest_conn.close()

    print("🎉 Deployment completed successfully")

if __name__ == "__main__":
    main()
