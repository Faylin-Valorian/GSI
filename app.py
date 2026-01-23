import os
import sys
import json
import pyodbc
import requests
import time
import threading
from datetime import timedelta
from cryptography.fernet import Fernet
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from flask_login import LoginManager, login_required
from sqlalchemy import create_engine

# --- INTERNAL IMPORTS ---
from extensions import db
from models import Users 
from blueprints.auth import auth_bp
from blueprints.StateManagement import state_mgmt_bp
from blueprints.CountyManagement import county_mgmt_bp
from blueprints.UserManagement import user_mgmt_bp

# --- NEW SPLIT BLUEPRINTS ---
from blueprints.MapVisualization import map_viz_bp
from blueprints.CountyWorkflow import workflow_bp
from blueprints.SystemTools import sys_bp

from blueprints.OpenDriveConnection import open_drive_bp
from blueprints.DatabaseCompatibility import db_compat_bp
from blueprints.SetupEDataTable import setup_edata_bp
from blueprints.SetupKeliTables import setup_keli_bp
from blueprints.UnindexedImages import unindexed_bp
from blueprints.AlterDatabaseFields import alter_db_bp
from blueprints.PatchManager import patch_bp
from blueprints.InitialPreparation import initial_prep_bp
from blueprints.InstrumentTypeCorrections import inst_type_bp
from blueprints.InitialKeliLinkup import initial_linkup_bp
from blueprints.EDataErrors import edata_errors_bp
from blueprints.ImportEDataErrors import import_edata_errors_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-key-change-in-prod'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=2)

# Configure Upload Folder for County Badge Images
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'images')
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# --- ENCRYPTION HELPER ---
KEY_FILE = 'secret.key'
def load_key():
    if not os.path.exists(KEY_FILE):
        key = Fernet.generate_key()
        with open(KEY_FILE, 'wb') as key_file: key_file.write(key)
    return open(KEY_FILE, 'rb').read()

# Initialize cipher (global)
cipher = Fernet(load_key())

def encrypt_password(password): return cipher.encrypt(password.encode()).decode()
def decrypt_password(encrypted): return cipher.decrypt(encrypted.encode()).decode()

# --- DB CONFIG LOAD HELPERS ---
def load_db_config():
    config_path = os.path.join(app.root_path, 'db_config.json')
    if not os.path.exists(config_path): return None
    with open(config_path, 'r') as f: return json.load(f)

def get_db_uri(cfg):
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"DRIVER={driver};SERVER={cfg['server']};DATABASE={cfg['database']};UID={cfg['user']};PWD={decrypt_password(cfg['password'])}"
    return f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(conn_str)}"

# --- APP INIT & AUTO-RESET LOGIC ---
db_config = None
try:
    db_config = load_db_config()
    if db_config:
        app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri(db_config)
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        db.init_app(app)
except Exception as e:
    print(f" >>> CONFIG/KEY ERROR: {e}")
    print(" >>> Resetting configuration to force setup...")
    if os.path.exists('db_config.json'): os.remove('db_config.json')
    if os.path.exists('secret.key'): os.remove('secret.key')
    cipher = Fernet(load_key())
    db_config = None

if not db_config:
    print(" >>> NO DATABASE CONFIG FOUND (or reset). Redirecting to setup.")

# --- INITIALIZE LOGIN MANAGER ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'

@login_manager.user_loader
def load_user(user_id):
    if 'SQLALCHEMY_DATABASE_URI' not in app.config:
        return None
    return db.session.get(Users, int(user_id))

# --- REGISTER BLUEPRINTS ---
app.register_blueprint(auth_bp)
app.register_blueprint(state_mgmt_bp)
app.register_blueprint(county_mgmt_bp)
app.register_blueprint(user_mgmt_bp)

# New Split Blueprints (Replaces geo_bp)
app.register_blueprint(map_viz_bp)
app.register_blueprint(workflow_bp)
app.register_blueprint(sys_bp)

app.register_blueprint(open_drive_bp)
app.register_blueprint(db_compat_bp)
app.register_blueprint(setup_edata_bp)
app.register_blueprint(setup_keli_bp)
app.register_blueprint(unindexed_bp)
app.register_blueprint(alter_db_bp)
app.register_blueprint(patch_bp)
app.register_blueprint(initial_prep_bp)
app.register_blueprint(inst_type_bp)
app.register_blueprint(edata_errors_bp)
app.register_blueprint(initial_linkup_bp) 
app.register_blueprint(import_edata_errors_bp)

# --- MIDDLEWARE: FORCE SETUP ---
@app.before_request
def check_db_config():
    config_path = os.path.join(app.root_path, 'db_config.json')
    is_configured = os.path.exists(config_path)
    allowed_endpoints = ['first_time_setup', 'restart_app']
    if request.endpoint and ('static' in request.endpoint or request.endpoint in allowed_endpoints):
        return
    if not is_configured:
        return redirect(url_for('first_time_setup'))

# --- ROUTES ---
@app.route('/')
@login_required
def dashboard():
    return render_template('dashboard.html')

@app.route('/setup', methods=['GET', 'POST'])
def first_time_setup():
    if request.method == 'POST':
        server = request.form.get('server')
        database = request.form.get('database')
        user = request.form.get('user')
        password = request.form.get('password')
        try:
            test_uri = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            eng = create_engine(test_uri)
            with eng.connect() as conn: pass
        except Exception as e:
            flash(f"Connection Failed: {e}")
            return render_template('setup.html', default_server=server, default_db=database, default_user=user)

        with open('db_config.json', 'w') as f:
            json.dump({"server": server, "database": database, "user": user, "password": encrypt_password(password)}, f)
        return """<div style="text-align:center;padding:50px;background:#222;color:#fff;"><h1>Saved!</h1><form action="/restart" method="POST"><button type="submit">Restart App</button></form></div>"""
    return render_template('setup.html')

@app.route('/restart', methods=['POST'])
def restart_app():
    def restart_job():
        time.sleep(1) 
        print(" >>> RESTARTING APPLICATION...")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Thread(target=restart_job).start()
    return """
    <html><head><title>Restarting...</title><meta http-equiv="refresh" content="5;url=/"><style>body{background-color:#222;color:#fff;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;}.loader{border:4px solid #333;border-top:4px solid #3498db;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;margin:20px auto;}@keyframes spin{0%{transform:rotate(0deg);}100%{transform:rotate(360deg);}}</style></head><body><div style="text-align:center;"><h1>Restarting System</h1><div class="loader"></div><p>Please wait...</p></div></body></html>
    """

@app.route('/keep-alive', methods=['POST'])
@login_required
def keep_alive(): return jsonify({'status': 'active'})

if __name__ == '__main__':
    if not db_config and not os.environ.get("WERKZEUG_RUN_MAIN"):
        print(" !! WARNING: Database not configured. Go to /setup !!")
    app.run(host='0.0.0.0', port=5000, debug=True)