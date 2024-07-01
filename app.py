from flask import Flask, request, jsonify, render_template
import os
import subprocess
import threading

app = Flask(__name__)

# Function to read logs
def read_logs():
    if os.path.exists('buzz_sync.log'):
        with open('buzz_sync.log', 'r') as log_file:
            return log_file.readlines()
    return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/update_env', methods=['POST'])
def update_env():
    data = request.json
    primary_key = data.get('primary_key')
    secondary_keys = data.get('secondary_keys', [])

    with open('.env', 'w') as f:
        f.write(f"RD_PRIMARY_API_KEY={primary_key}\n")
        f.write(f"RD_SECONDARY_API_KEYS={','.join(secondary_keys)}\n")

    return jsonify({"success": True})

@app.route('/start_sync', methods=['POST'])
def start_sync():
    def run_sync():
        subprocess.run(['python3', 'buzz_sync.py'])

    threading.Thread(target=run_sync).start()
    return jsonify({"success": True})

@app.route('/logs', methods=['GET'])
def logs():
    log_lines = read_logs()
    return jsonify(log_lines)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
