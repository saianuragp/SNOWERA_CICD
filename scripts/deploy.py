import os, sys, json
import snowflake.connector

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "..", "config", "config.json")

# ---------------- ASCII ART ----------------
def deploy_header():
    print(r"""
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
    """)

# ---------------- CONFIG ----------------
def load_config():
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
    print("❌ Missing SNOWFLAKE_PASSWORD")
    sys.exit(1)

REPO_NAME = os.getenv("GITHUB_REPOSITORY", "").split("/")[-1]

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
        WHERE repo_name=%s AND is_active=TRUE
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        print("❌ Repo config not found")
        sys.exit(1)

    return row


def fetch_validated_files(conn, schema):
    sql = f"""
        SELECT sql_file
        FROM {MANIFEST_TABLE}
        WHERE repo_name=%s
          AND schema_name=%s
          AND status='VALIDATED'
        ORDER BY validated_at
    """
    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema))
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def run_sql_file(conn, file_path):
    with open(file_path) as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    print(f"🚀 Deployed {file_path}")


def mark_deployed(conn, schema, sql_file):
    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET status='DEPLOYED',
            deployed_at=CURRENT_TIMESTAMP()
        WHERE repo_name=%s AND schema_name=%s AND sql_file=%s
    """
    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, sql_file))
    cur.close()


def print_summary(account, user, role, warehouse, database, schema):
    print("="*60)
    print("🚀 DEPLOY SUMMARY 🚀")
    print("="*60)
    print(f"Account    : {account}")
    print(f"User       : {user}")
    print(f"Role       : {role}")
    print(f"Warehouse  : {warehouse}")
    print(f"Database   : {database}")
    print(f"Schema     : {schema}")
    print(f"Repository : {REPO_NAME}")
    print("="*60)


# ---------------- MAIN ----------------
def main():
    deploy_header()

    warehouse, role, database, schema = get_repo_config("prod")
    print_summary(ACCOUNT, USER, role, warehouse, database, schema)

    manifest_conn = connect(MANIFEST_ROLE, warehouse, database)
    sql_files = fetch_validated_files(manifest_conn, schema)

    if not sql_files:
        print("ℹ️ No validated SQL files found")
        return

    deploy_conn = connect(role, warehouse, database)

    for f in sql_files:
        run_sql_file(deploy_conn, f)
        mark_deployed(manifest_conn, schema, f)

    deploy_conn.close()
    manifest_conn.close()

    print("✅ Deployment completed successfully")


if __name__ == "__main__":
    main()
