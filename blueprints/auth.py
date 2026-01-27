from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required, current_user
from models import Users
from extensions import db

auth_bp = Blueprint('auth', __name__)

# [GSI_BLOCK: auth_login]
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))  # FIXED: index -> dashboard
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = Users.query.filter_by(username=username).first()
        
        if user and check_password_hash(user.password_hash, password):
            if not user.is_active:
                flash('Account is inactive. Please contact administrator.', 'danger')
                return render_template('login.html')
                
            login_user(user)
            return redirect(url_for('dashboard'))  # FIXED: index -> dashboard
        else:
            flash('Invalid username or password', 'danger')
            
    return render_template('login.html')
# [GSI_END: auth_login]

# [GSI_BLOCK: auth_register]
@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))  # FIXED: index -> dashboard
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        
        if password != confirm_password:
            flash('Passwords do not match', 'danger')
            return render_template('register.html')
            
        if Users.query.filter_by(username=username).first():
            flash('Username already exists', 'danger')
            return render_template('register.html')
            
        new_user = Users(
            username=username,
            password_hash=generate_password_hash(password),
            role='user', 
            is_active=False # Pending admin approval
        )
        
        db.session.add(new_user)
        db.session.commit()
        
        flash('Registration successful! Please wait for admin approval.', 'success')
        return redirect(url_for('auth.login'))
        
    return render_template('register.html')
# [GSI_END: auth_register]

# [GSI_BLOCK: auth_logout]
@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
# [GSI_END: auth_logout]

# [GSI_BLOCK: auth_verify]
@auth_bp.route('/verify-password', methods=['POST'])
@login_required
def verify_password():
    data = request.json
    password = data.get('password')
    
    if check_password_hash(current_user.password_hash, password):
        return jsonify({'success': True})
    return jsonify({'success': False})
# [GSI_END: auth_verify]

# [GSI_BLOCK: auth_change_pw]
@auth_bp.route('/change-password', methods=['POST'])
@login_required
def change_password():
    data = request.json
    current_pw = data.get('current_password')
    new_pw = data.get('new_password')
    
    if not check_password_hash(current_user.password_hash, current_pw):
        return jsonify({'success': False, 'message': 'Incorrect current password'})
        
    current_user.password_hash = generate_password_hash(new_pw)
    db.session.commit()
    
    return jsonify({'success': True, 'message': 'Password updated successfully'})
# [GSI_END: auth_change_pw]