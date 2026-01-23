import os, sys, json, subprocess
import snowflake.connector

# ---------------- ASCII ART ----------------
def validate_header():
    print(r"""
    
     *        .  *   
    |-|       *   *
    |-|      _   .  _   
    |-|     |   *    |
    |-|     |~~~~~~~v|
    |-|     |  O o * |
   /___\    |o___O___|
    
    """)

# ---------------- CONFIG ----------------
def load_config():
    with open(".cicd/config.json") as f:
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


def get_changed_sql_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True, text=True, check=True
    )
    return [f for f in result.stdout.splitlines() if f.endswith(".sql")]


def run_sql_file(conn, file_path):
    with open(file_path) as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    print(f"✅ Executed {file_path}")


def insert_manifest(conn, schema, sql_file):
    sql = f"""
        MERGE INTO {MANIFEST_TABLE} t
        USING (SELECT %s repo, %s schema, %s file) s
        ON t.repo_name=s.repo AND t.schema_name=s.schema AND t.sql_file=s.file
        WHEN NOT MATCHED THEN
          INSERT (repo_name,schema_name,sql_file,status,validated_at)
          VALUES (s.repo,s.schema,s.file,'VALIDATED',CURRENT_TIMESTAMP())
    """
    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, sql_file))
    cur.close()


def print_summary(role, warehouse, database, schema):
    print("="*60)
    print("🚀 VALIDATE SUMMARY 🚀")
    print("="*60)
    print("Snowflake Account  : (config)")
    print("Snowflake User     : (config)")
    print(f"Role               : {role}")
    print(f"Warehouse          : {warehouse}")
    print(f"Database           : {database}")
    print(f"Schema             : {schema}")
    print(f"Repository         : {REPO_NAME}")
    print("="*60)


# ---------------- MAIN ----------------
def main():
    validate_header()

    warehouse, role, database, schema = get_repo_config("preprod")
    print_summary(role, warehouse, database, schema)

    sql_files = get_changed_sql_files()
    if not sql_files:
        print("ℹ️ No SQL changes detected")
        return

    conn = connect(role, warehouse, database)

    for f in sql_files:
        run_sql_file(conn, f)

    conn.close()

    manifest_conn = connect(MANIFEST_ROLE, warehouse, database)

    for f in sql_files:
        insert_manifest(manifest_conn, schema, f)

    manifest_conn.close()

    print("✅ Validation completed and manifest updated")


if __name__ == "__main__":
    main()
