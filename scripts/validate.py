#!/usr/bin/env python3

import os
import sys
import subprocess
from pathlib import Path

class SnowflakeValidator:
    def __init__(self):
        self.snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
        self.snowflake_role = os.environ.get('SNOWFLAKE_ROLE')
        self.snowflake_user = os.environ.get('SNOWFLAKE_USER')
        self.github_base_ref = os.environ.get('GITHUB_BASE_REF', 'main')
        self.github_head_ref = os.environ.get('GITHUB_HEAD_REF', '')
        
        self.validation_failed = False
      
    def print_header(self):
        """Print validation header"""
        print("=" * 60)
        print("Snowflake Context : ")
        print(f"Account: {self.snowflake_account}")
        print(f"Database: {self.snowflake_database}")
        print(f"Role: {self.snowflake_role}")
        print(f"User: {self.snowflake_user}")
        print(f"Branch: {self.github_head_ref} → {self.github_base_ref}")
        print("=" * 60)
        print()
        
    def get_changed_files(self):
        """Get list of changed SQL files from PR"""
        print("Identifying changed files in PR...")
        
        try:
            # Fetch the base branch
            subprocess.run(
                ['git', 'fetch', 'origin', self.github_base_ref],
                capture_output=True,
                check=False
            )
            
            # Get changed files compared to base branch
            cmd = [
                'git', 'diff', '--name-only',
                f'origin/{self.github_base_ref}...HEAD',
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
    
    
    def validate_sql_file(self, file_path):
        """Validate a SQL file by executing it in PREPROD"""
        print("=" * 60)
        print(f"Validating: {file_path}")
        print("=" * 60)
        
        try:
            # Read SQL file
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            
            # Write to temp file
            temp_file = 'temp_validate.sql'
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
                print(f"✓ Validation passed for {file_path}")
                print()
                return True
            else:
                error_msg = result.stderr or result.stdout
                print(f"✗ Validation failed:")
                print(error_msg)
                print()
                return False
                
        except Exception as e:
            print(f"✗ Exception occurred: {e}")
            print()
            return False
    
    def validate(self):
        """Main validation function"""
        self.print_header()
        
        # Get changed files
        changed_files = self.get_changed_files()
        
        if not changed_files:
            print("No modified SQL files found in this PR.")
            print("✓ Nothing to validate.")
            return 0
        
        print()
        print("The following files will be validated in PREPROD:")
        print("-" * 60)
        for file in changed_files:
            print(f"  - {file}")
        print("-" * 60)
        print()
        
        # Validate each file
        for file_path in changed_files:
            if not os.path.exists(file_path):
                print(f"⚠ Warning: File not found: {file_path}")
                continue
            
            success = self.validate_sql_file(file_path)
            
            if not success:
                self.validation_failed = True
        
        # Print final summary
        print("=" * 60)
        if self.validation_failed:
            print("✗ VALIDATION FAILED - One or more SQL scripts failed")
            print("=" * 60)
            print()
            print("⚠️  Please fix the errors before merging to main")
            return 1
        else:
            print("✓ VALIDATION SUCCESSFUL - All scripts validated in PREPROD")
            print("=" * 60)
            print()
            print("✅ Safe to merge to main for PROD deployment")
            return 0


def main():
    """Main entry point"""
    validator = SnowflakeValidator()
    exit_code = validator.validate()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
