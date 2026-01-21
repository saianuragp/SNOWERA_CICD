#!/usr/bin/env python3

import os
import sys
import subprocess
from pathlib import Path

class SnowflakeDeployer:
    def __init__(self):
        self.snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
        self.snowflake_role = os.environ.get('SNOWFLAKE_ROLE')
        self.snowflake_user = os.environ.get('SNOWFLAKE_USER')
        self.github_sha = os.environ.get('GITHUB_SHA', 'unknown')
        self.github_event_before = os.environ.get('GITHUB_EVENT_BEFORE', '')
        
        self.deployment_failed = False
        
    def print_header(self):
        """Print deployment header"""
        print("=" * 60)
        print("Snowflake Context : ")
        print(f"Account: {self.snowflake_account}")
        print(f"Database: {self.snowflake_database}")
        print(f"Role: {self.snowflake_role}")
        print(f"User: {self.snowflake_user}")
        print(f"Commit: {self.github_sha[:8]}")
        print("=" * 60)
        print()
        
    def get_changed_files(self):
        """Get list of changed SQL files from git"""
        print("Identifying changed files...")
        
        try:
            cmd = [
                'git', 'diff', '--name-only',
                self.github_event_before,
                self.github_sha,
                '--', '**/*.sql'
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            changed_files = [
                f.strip() 
                for f in result.stdout.split('\n') 
                if f.strip() and f.endswith('.sql')
            ]
            
            return changed_files
            
        except Exception as e:
            print(f"Error getting changed files: {e}")
            return []
    
    def execute_sql_file(self, file_path):
        """Execute a SQL file using Snow CLI"""
        print("=" * 60)
        print(f"Executing: {file_path}")
        print("=" * 60)
        
        try:
            # Read SQL file
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # Write to temp file
            temp_file = 'temp_deploy.sql'
            with open(temp_file, 'w') as f:
                f.write(sql_content)
            
            # Execute using Snow CLI
            result = subprocess.run(
                ['snow', 'sql', '-f', temp_file, '--temporary-connection'],
                capture_output=True,
                text=True,
                check=False
            )
            
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            if result.returncode == 0:
                print(f"✓ Successfully executed {file_path}")
                print()
                return True
            else:
                error_msg = result.stderr or result.stdout
                print(f"✗ Execution failed:")
                print(error_msg)
                print()
                return False
                
        except Exception as e:
            print(f"✗ Exception occurred: {e}")
            print()
            return False
    
    def deploy(self):
        """Main deployment function"""
        self.print_header()
        
        # Get changed files
        changed_files = self.get_changed_files()
        
        if not changed_files:
            print("No modified SQL files found in this push.")
            return 0
        
        print()
        print("The following files will be deployed:")
        print("-" * 60)
        for file in changed_files:
            print(f"  - {file}")
        print("-" * 60)
        print()
        
        # Deploy each file
        for file_path in changed_files:
            if not os.path.exists(file_path):
                print(f"⚠ Warning: File not found: {file_path}")
                continue
            
            success = self.execute_sql_file(file_path)
            
            if not success:
                self.deployment_failed = True
        
        # Print final summary
        print("=" * 60)
        if self.deployment_failed:
            print("✗ DEPLOYMENT FAILED - One or more SQL scripts failed")
            print("=" * 60)
            return 1
        else:
            print("✓ DEPLOYMENT SUCCESSFUL - All scripts executed successfully")
            print("=" * 60)
            return 0


def main():
    """Main entry point"""
    deployer = SnowflakeDeployer()
    exit_code = deployer.deploy()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
