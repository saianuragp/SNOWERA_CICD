import os
import sys
import subprocess

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
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

def infer_environment(role, database):
    key = f"{role}_{database}".lower()
    if "prod" in key:
        return "PROD"
    return "UNKNOWN"

def get_changed_sql_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD~1...HEAD"],
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
        f.write("## 🚀 Snowflake Deployment Summary\n\n")
        f.write(f"- **Environment:** `{env}`\n")
        f.write(f"- **Database:** `{os.environ['SNOWFLAKE_DATABASE']}`\n")
        f.write(f"- **Role:** `{os.environ['SNOWFLAKE_ROLE']}`\n")
        f.write("\n### Executed SQL Files\n")
        for file in files:
            f.write(f"- `{file}`\n")

def main():
    for v in REQUIRED_VARS:
        require(v)

    env = infer_environment(
        os.environ["SNOWFLAKE_ROLE"],
        os.environ["SNOWFLAKE_DATABASE"],
    )

    if env != "PROD":
        print("❌ Deployment must run against PROD only")
        sys.exit(1)

    sql_files = get_changed_sql_files()

    if not sql_files:
        print("ℹ️ No SQL changes detected")
        sys.exit(0)

    print(f"🚀 Deploying {len(sql_files)} SQL files to PROD")

    for sql in sql_files:
        print(f"▶ Executing {sql}")
        run_sql(sql)

    write_summary(env, sql_files)
    print("🚀 Deployment completed successfully")

if __name__ == "__main__":
    main()
