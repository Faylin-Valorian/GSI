from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from extensions import db
from models import Users
from utils import format_error
import random, string

user_mgmt_bp = Blueprint('user_mgmt', __name__)

# --- LIST USERS ---
@user_mgmt_bp.route('/api/users', methods=['GET'])
@login_required
def get_users_api():
    if current_user.role != 'admin': return jsonify({'error': 'Unauthorized'}), 403
    return jsonify([{
        'id': u.id, 
        'username': u.username, 
        'email': u.email, 
        'role': u.role, 
        'is_verified': u.is_verified, 
        'is_locked': u.is_locked,
        'current_working_county_id': u.current_working_county_id
    } for u in Users.query.all()])

# --- UPDATE DETAILS (NEW) ---
@user_mgmt_bp.route('/api/admin/user/<int:id>/update', methods=['POST'])
@login_required
def update_user_details(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    
    try:
        u = db.session.get(Users, id)
        if not u: return jsonify({'success': False, 'message': 'User not found'})

        data = request.json
        new_username = data.get('username', '').strip()
        new_email = data.get('email', '').strip()

        # Validation: Check for duplicates if changing
        if new_username and new_username != u.username:
            if Users.query.filter_by(username=new_username).first():
                return jsonify({'success': False, 'message': 'Username already taken.'})
            u.username = new_username

        if new_email and new_email != u.email:
            if Users.query.filter_by(email=new_email).first():
                return jsonify({'success': False, 'message': 'Email already taken.'})
            u.email = new_email

        db.session.commit()
        return jsonify({'success': True})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- RESET PASSWORD ---
@user_mgmt_bp.route('/api/admin/user/<int:id>/reset-password', methods=['POST'])
@login_required
def reset_user_password(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    
    try:
        u = db.session.get(Users, id)
        if not u: return jsonify({'success': False, 'message': 'User not found'})
        
        # Generate random 8-char password
        temp_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        u.set_password(temp_pass)
        u.is_temporary_password = True # Flag them to change it on login
        db.session.commit()
        
        return jsonify({'success': True, 'temp_pass': temp_pass})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- TOGGLE ROLE ---
@user_mgmt_bp.route('/api/admin/user/<int:id>/toggle-role', methods=['POST'])
@login_required
def toggle_user_role(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    if current_user.id == id: return jsonify({'success': False, 'message': 'Cannot demote yourself.'})
    try:
        u = db.session.get(Users, id)
        if u:
            u.role = 'user' if u.role == 'admin' else 'admin'
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'User not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- TOGGLE LOCK ---
@user_mgmt_bp.route('/api/admin/user/<int:id>/toggle-lock', methods=['POST'])
@login_required
def toggle_user_lock(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    if current_user.id == id: return jsonify({'success': False, 'message': 'Cannot lock your own account.'})
    try:
        u = db.session.get(Users, id)
        if u:
            u.is_locked = not u.is_locked
            if u.is_locked: u.current_working_county_id = None
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'User not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- DELETE USER ---
@user_mgmt_bp.route('/api/admin/user/<int:id>/delete', methods=['POST'])
@login_required
def delete_user(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    if current_user.id == id: return jsonify({'success': False, 'message': 'Cannot delete yourself.'})
    try:
        u = db.session.get(Users, id)
        if u:
            db.session.delete(u)
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'User not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})