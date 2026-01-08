import os
import sys
import json
import pyodbc
import requests
from datetime import timedelta
from cryptography.fernet import Fernet
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from flask_login import LoginManager, login_required
from sqlalchemy import create_engine

# --- INTERNAL IMPORTS ---
from extensions import db
from models import Users 
from blueprints.auth import auth_bp
from blueprints.UserManagement import user_mgmt_bp
from blueprints.geospatial import geo_bp
from blueprints.OpenDriveConnection import open_drive_bp
from blueprints.DatabaseCompatibility import db_compat_bp
from blueprints.SetupDatabaseProcedures import setup_procedures_bp
from blueprints.SetupEDataTable import setup_edata_bp
from blueprints.SetupKeliTables import setup_keli_bp
from blueprints.UnindexedImages import unindexed_bp
from blueprints.SchemaTools import schema_bp
from blueprints.PatchManager import patch_bp
from blueprints.DataCleanup import cleanup_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-key-change-in-prod'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=2)

# --- INITIALIZE LOGIN MANAGER (Moved Up) ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(Users, int(user_id))

# --- REGISTER BLUEPRINTS ---
app.register_blueprint(auth_bp)
app.register_blueprint(user_mgmt_bp)
app.register_blueprint(geo_bp)
app.register_blueprint(open_drive_bp)
app.register_blueprint(db_compat_bp)
app.register_blueprint(setup_procedures_bp)
app.register_blueprint(setup_edata_bp)
app.register_blueprint(setup_keli_bp)
app.register_blueprint(unindexed_bp)
app.register_blueprint(schema_bp)
app.register_blueprint(patch_bp)
app.register_blueprint(cleanup_bp)

# --- DB CONFIG LOAD ---
def load_db_config():
    config_path = os.path.join(app.root_path, 'db_config.json')
    if not os.path.exists(config_path): return None
    with open(config_path, 'r') as f: return json.load(f)

def get_db_uri(cfg):
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"DRIVER={driver};SERVER={cfg['server']};DATABASE={cfg['database']};UID={cfg['user']};PWD={decrypt_password(cfg['password'])}"
    return f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(conn_str)}"

# --- ENCRYPTION HELPER ---
KEY_FILE = 'secret.key'
def load_key():
    if not os.path.exists(KEY_FILE):
        key = Fernet.generate_key()
        with open(KEY_FILE, 'wb') as key_file: key_file.write(key)
    return open(KEY_FILE, 'rb').read()

cipher = Fernet(load_key())

def encrypt_password(password): return cipher.encrypt(password.encode()).decode()
def decrypt_password(encrypted): return cipher.decrypt(encrypted.encode()).decode()

# --- APP INIT ---
db_config = load_db_config()
if db_config:
    app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri(db_config)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
else:
    print(" >>> NO DATABASE CONFIG FOUND. Redirecting to setup.")

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
        
        # Test Connection
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
    print(" >>> RESTARTING APPLICATION...")
    os.execv(sys.executable, [sys.executable] + sys.argv)

@app.route('/keep-alive', methods=['POST'])
@login_required
def keep_alive(): return jsonify({'status': 'active'})

if __name__ == '__main__':
    if not db_config and not os.environ.get("WERKZEUG_RUN_MAIN"):
        print(" !! WARNING: Database not configured. Go to /setup !!")
    app.run(host='0.0.0.0', port=5000, debug=True)