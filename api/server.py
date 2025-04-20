# # dbt_api_server.py
# import subprocess
# import os
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/dbt") # Get from env

# @app.route('/trigger-dbt', methods=['POST'])
# def trigger_dbt():
#     data = request.get_json()
#     if not data or 'command' not in data:
#         return jsonify({"error": "Missing 'command' in request body"}), 400

#     dbt_command = data['command'].split() # e.g., ["run", "--select", "my_model"]
#     allowed_commands = ["run", "test", "seed", "snapshot", "build"] # Basic safety check

#     if not dbt_command or dbt_command[0] not in allowed_commands:
#          return jsonify({"error": f"Invalid or disallowed dbt command: {dbt_command[0]}"}), 400

#     full_command = ["dbt"] + dbt_command

#     try:
#         # Execute dbt command within the project directory
#         print(f"Executing command: {' '.join(full_command)} in {DBT_PROJECT_DIR}")
#         result = subprocess.run(
#             full_command,
#             cwd=DBT_PROJECT_DIR,
#             capture_output=True,
#             text=True,
#             check=False # Don't raise exception on non-zero exit code here
#         )

#         print(f"Command stdout:\n{result.stdout}")
#         print(f"Command stderr:\n{result.stderr}")

#         response = {
#             "command_executed": " ".join(full_command),
#             "return_code": result.returncode,
#             "stdout": result.stdout[-1000:], # Limit output size
#             "stderr": result.stderr[-1000:]
#         }

#         status_code = 200 if result.returncode == 0 else 500 # Internal Server Error on failure
#         return jsonify(response), status_code

#     except FileNotFoundError:
#          print(f"Error: 'dbt' command not found. Is it in the PATH?")
#          return jsonify({"error": "'dbt' command not found in container"}), 500
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

# if __name__ == '__main__':
#      # Listen on all interfaces within the container, port 5000
#      app.run(host='0.0.0.0', port=5000)