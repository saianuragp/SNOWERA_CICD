import os
import sys
import subprocess
from pathlib import Path

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

def require(var):
    """Ensure required environment variable exists"""
    val = os.getenv(var)
    if not val:
        print(f"❌ Missing env var: {var}")
        sys.exit(1)
    return val

def get_changed_sql_files():
    """Return a list of changed SQL files"""
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.splitlines() if f.endswith(".sql")]

def run_sql(file_path):
    """Run SQL file on Snowflake"""
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

def write_summary(files):
    """Write GitHub Actions summary"""
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary:
        return

    with open(summary, "a") as f:
        f.write("## 🧪 Snowflake Validation Summary\n\n")
        f.write(f"- **Database:** `{os.environ['SNOWFLAKE_DATABASE']}`\n")
        f.write(f"- **Role:** `{os.environ['SNOWFLAKE_ROLE']}`\n")
        f.write("\n### Executed SQL Files\n")
        for file in files:
            f.write(f"- `{file}`\n")

def main():
    # Ensure all required environment variables exist
    for v in REQUIRED_VARS:
        require(v)

    # SQL files to run
    sql_files = get_changed_sql_files()

    if not sql_files:
        print("ℹ️ No SQL changes detected")
        sys.exit(0)

    print(f"🧪 Validating {len(sql_files)} SQL files in PREPROD")

    for sql in sql_files:
        print(f"▶ Executing {sql}")
        run_sql(sql)

    write_summary(sql_files)
    print("✅ Validation completed successfully")

if __name__ == "__main__":
    main()
