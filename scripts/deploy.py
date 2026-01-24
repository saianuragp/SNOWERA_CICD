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
MANIFEST_ROLE = CFG["manifest_role"]
MANIFEST_TABLE = CFG["manifest_table"]
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


def fetch_validated_sql_files(schema):
    conn = connect(MANIFEST_ROLE, "ANU_DEVOPS_WH_XS", "ANU_DEVOPS_DB_PROD")

    sql = f"""
        SELECT sql_file
        FROM {MANIFEST_TABLE}
        WHERE repo_name = %s
          AND schema_name = %s
          AND status = 'VALIDATED'
        ORDER BY validated_at
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema))
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

    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    print(f"✅ Deployed {file_path}")


def update_manifest(conn, schema, sql_file):
    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET status = 'DEPLOYED',
            deployed_at = CURRENT_TIMESTAMP()
        WHERE repo_name = %s
          AND schema_name = %s
          AND sql_file = %s
          AND status = 'VALIDATED'
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, sql_file))
    cur.close()


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

    sql_files = fetch_validated_sql_files(schema)
    if not sql_files:
        print("ℹ️ No validated SQL files found")
        return

    print(f"🚀 Deploying {len(sql_files)} SQL files")

    conn = connect(role, warehouse, database)
    for f in sql_files:
        run_sql_file(conn, f)
    conn.close()

    manifest_conn = connect(MANIFEST_ROLE, warehouse, database)
    for f in sql_files:
        update_manifest(manifest_conn, schema, f)
    manifest_conn.close()

    print("🎉 Deployment completed successfully")

if __name__ == "__main__":
    main()
