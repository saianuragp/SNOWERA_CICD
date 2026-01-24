import os
import sys
import json
import subprocess
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
def validate_header():
    print("=" * 60)
    print("🚀 VALIDATION STARTED 🚀")
    print("=" * 60)
    print(r"""   
         *        .  *   
        |-|       *   *
        |-|      _   .  _   
        |-|     |   *    |
        |-|     |~~~~~~~v|
        |-|     |  O o * |
       /___\    |o___O___|       
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


def get_changed_sql_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True,
        text=True,
        check=True,
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
    """
    Insert or update manifest using validated_at as run discriminator
    """

    sql = f"""
        MERGE INTO {MANIFEST_TABLE} t
        USING (
            SELECT %s AS repo_name,
                   %s AS schema_name,
                   %s AS sql_file
        ) s
        ON t.repo_name = s.repo_name
           AND t.schema_name = s.schema_name
           AND t.sql_file = s.sql_file

        WHEN MATCHED THEN UPDATE SET
            t.status = 'VALIDATED',
            t.validated_at = CURRENT_TIMESTAMP(),
            t.error_message = NULL,
            t.validated_by = CURRENT_USER()

        WHEN NOT MATCHED THEN INSERT
        (
            repo_name,
            schema_name,
            sql_file,
            status,
            validated_at,
            deployed_at,
            error_message,
            validated_by,
            deployed_by,
            created_by,
            created_at
        )
        VALUES
        (
            s.repo_name,
            s.schema_name,
            s.sql_file,
            'VALIDATED',
            CURRENT_TIMESTAMP(),
            NULL,
            NULL,
            CURRENT_USER(),
            NULL,
            CURRENT_USER(),
            CURRENT_TIMESTAMP()
        )
    """

    cur = conn.cursor()
    cur.execute(sql, (REPO_NAME, schema, sql_file))
    cur.close()

    print(f"📘 Manifest recorded: {sql_file}")


def print_summary(role, warehouse, database, schema):
    print("=" * 60)
    print("🚀 VALIDATE SUMMARY 🚀")
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
    validate_header()

    warehouse, role, database, schema = get_repo_config("preprod")
    print_summary(role, warehouse, database, schema)

    sql_files = get_changed_sql_files()
    if not sql_files:
        print("ℹ️ No SQL changes detected")
        return

    print(f"🧪 Validating {len(sql_files)} SQL files")

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
