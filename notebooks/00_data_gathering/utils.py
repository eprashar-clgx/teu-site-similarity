from pathlib import Path
import os
from datetime import datetime, timedelta
import subprocess
import time

# Helper function to find root folder
# Downloaded ACS data should be stored in root folder/ data / raw / ACS
def find_project_root(start:Path | None=None)-> Path:
    '''
    Objective is to find a marker such as .toml, .git or a directory that points to root
    '''
    start = start or Path.cwd()
    for p in [start, *start.parents]:
        if (p / ".git").exists() or (p / "pyproject.toml").exists() or (p / "data").exists():
            return p
    raise FileNotFoundError("Project root not found")

# Function to authenticate gcs credentials
def check_and_authenticate(json_path):
    '''
    Function to check google authentication token and re-generate if it is expired/doesn't exist
    '''
    try:
        if not os.path.exists(json_path):
            raise FileNotFoundError("Credentials file not found")
        # Get modification time of the file
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(json_path))
        current_time = datetime.now()

        # Check if the file is older than 24 hours
        if current_time - file_mod_time > timedelta(hours=24):
            print("Credentials file is older than 24 hours. Re-authenticating...")

            # Re-authenticate
            try:
                print(f"Trying reauthentication on gcloud server using shell command...")
                subprocess.run("start cmd /c gcloud auth application-default login", shell=True, check=True)
                print('Login window opened...please complete authentication')
                
                # Poll for file modification
                print("Waiting for credentials file to update...")
                max_wait = 300  # seconds
                check_interval = 2  # seconds
                start_time = datetime.now()

                while (datetime.now() - start_time).total_seconds() < max_wait:
                    new_mod_time = datetime.fromtimestamp(os.path.getmtime(json_path))
                    if new_mod_time > file_mod_time:
                        print("Authentication confirmed! Credentials file updated.")
                        break
                    time.sleep(check_interval)
                else:
                    print("Timed out waiting for credentials file update.")

            except subprocess.CalledProcessError as e:
                print(f"Error during re-authentication: {e}")
            except Exception as e:
                print(f'Authentication failed because of {e}')
        else:
            print("Credentials file is valid.")
    except Exception as e:
        print(f"Error: {e}")