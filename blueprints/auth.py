from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_login import login_user, logout_user, login_required, current_user
from extensions import db
from models import Users
import random

auth_bp = Blueprint('auth', __name__)
ALLOWED_REGISTRATION_DOMAIN = 'sutterfieldtechnologies.net'

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        u = Users.query.filter_by(username=username).first()
        
        if u and u.check_password(password):
            if u.is_locked:
                flash("Account is LOCKED.")
                return render_template('login.html')
            if not u.is_verified:
                session['pending_user_id'] = u.id
                return redirect(url_for('auth.verify_code'))
                
            login_user(u)
            if u.is_temporary_password:
                return redirect(url_for('auth.force_password_change'))
            return redirect(url_for('dashboard'))
        flash("Invalid credentials")
    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')

        if email.split('@')[-1] != ALLOWED_REGISTRATION_DOMAIN:
            flash(f"Restricted to {ALLOWED_REGISTRATION_DOMAIN}")
            return render_template('register.html')

        if Users.query.filter((Users.username == username) | (Users.email == email)).first():
            flash("User already exists")
            return render_template('register.html')

        role = 'admin' if Users.query.count() == 0 else 'user'
        code = str(random.randint(100000, 999999))
        
        # --- DEBUG OUTPUT FOR DEVELOPMENT ---
        print(f" --- DEBUG TOKEN: {code} --- ")
        
        u = Users(username=username, email=email, is_verified=False, verification_code=code, role=role)
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        
        session['pending_user_id'] = u.id
        return redirect(url_for('auth.verify_code'))
    return render_template('register.html')

@auth_bp.route('/verify', methods=['GET', 'POST'])
def verify_code():
    if request.method == 'POST':
        code = request.form.get('code')
        uid = session.get('pending_user_id')
        if not uid: return redirect(url_for('auth.login'))

        u = db.session.get(Users, uid)
        if u and u.verification_code == code:
            u.is_verified = True
            u.verification_code = None
            db.session.commit()
            flash("Verified!")
            return redirect(url_for('auth.login'))
        flash("Invalid Code.")
    return render_template('verify.html')

@auth_bp.route('/change-password', methods=['GET', 'POST'])
@login_required
def force_password_change():
    if not current_user.is_temporary_password: return redirect(url_for('dashboard'))
    if request.method == 'POST':
        if request.form.get('new_password') != request.form.get('confirm_password'):
            flash("Mismatch")
            return render_template('change_password.html')
        current_user.set_password(request.form.get('new_password'))
        current_user.is_temporary_password = False
        db.session.commit()
        return redirect(url_for('dashboard'))
    return render_template('change_password.html')