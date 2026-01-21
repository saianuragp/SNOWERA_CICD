import os
import sys
import subprocess
from pathlib import Path

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",  # Will NOT print
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

def require(var):
    val = os.getenv(var)
    if not val:
        print(f"❌ Missing env var: {var}")
        sys.exit(1)
    return val

def print_env_vars():
    print("🌐 Environment variables (password hidden):")
    for var in REQUIRED_VARS:
        if var == "SNOWFLAKE_PASSWORD":
            print(f"- {var}: ******** (hidden)")
        else:
            print(f"- {var}: {os.getenv(var)}")
    print("---\n")

def infer_environment(role, database):
    key = f"{role}_{database}".lower()
    if "prod" in key:
        return "PROD"
    if "preprod" in key or "pp" in key:
        return "PREPROD"
    if "dev" in key:
        return "DEV"
    return "UNKNOWN"

def get_changed_sql_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.splitlines() if f.endswith(".sql")]

def run_sql(file_path):
    subprocess.run(
        [
            "snow", "sql",
            "-f", file_path,
            "--database", os.environ["SNOWFLAKE_DATABASE"],
            "--warehouse", os.environ["SNOWFLAKE_WAREHOUSE"],
            "--role", os.environ["SNOWFLAKE_ROLE"],
        ],
        check=True,
    )

def write_summary(env, files):
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary:
        return

    with open(summary, "a") as f:
        f.write("## 🧪 Snowflake Validation Summary\n\n")
        f.write(f"- **Environment:** `{env}`\n")
        f.write(f"- **Database:** `{os.environ['SNOWFLAKE_DATABASE']}`\n")
        f.write(f"- **Role:** `{os.environ['SNOWFLAKE_ROLE']}`\n")
        f.write("\n### Executed SQL Files\n")
        for file in files:
            f.write(f"- `{file}`\n")

def main():
    # Ensure all required environment variables exist
    for v in REQUIRED_VARS:
        require(v)

    # Print environment variables (safe)
    print_env_vars()

    env = infer_environment(
        os.environ["SNOWFLAKE_ROLE"],
        os.environ["SNOWFLAKE_DATABASE"],
    )

    if env != "PREPROD":
        print("❌ Validation must run against PREPROD only")
        sys.exit(1)

    sql_files = get_changed_sql_files()

    if not sql_files:
        print("ℹ️ No SQL changes detected")
        sys.exit(0)

    print(f"🧪 Validating {len(sql_files)} SQL files in PREPROD")

    for sql in sql_files:
        print(f"▶ Executing {sql}")
        run_sql(sql)

    write_summary(env, sql_files)
    print("✅ Validation completed successfully")

if __name__ == "__main__":
    main()
