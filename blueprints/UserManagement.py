import random
import string
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from extensions import db
from models import Users
from utils import format_error

user_mgmt_bp = Blueprint('user_mgmt', __name__)

@user_mgmt_bp.route('/api/admin/users/list', methods=['GET'])
@login_required
def list_users():
    if current_user.role != 'admin': return jsonify([])
    
    users = Users.query.all()
    return jsonify([{
        'id': u.id,
        'username': u.username,
        'email': u.email,
        'role': u.role,
        'status': 'active' if not u.is_locked else 'locked'
    } for u in users])

@user_mgmt_bp.route('/api/admin/users/edit', methods=['POST'])
@login_required
def edit_user():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    try:
        u = db.session.get(Users, data.get('id'))
        if not u: return jsonify({'success': False, 'message': 'User not found'})
        
        # Check uniqueness
        new_user = data.get('username')
        new_email = data.get('email')
        
        if new_user and new_user != u.username:
            if Users.query.filter_by(username=new_user).first():
                return jsonify({'success': False, 'message': 'Username taken'})
            u.username = new_user
            
        if new_email and new_email != u.email:
            if Users.query.filter_by(email=new_email).first():
                return jsonify({'success': False, 'message': 'Email taken'})
            u.email = new_email
            
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@user_mgmt_bp.route('/api/admin/users/reset-password', methods=['POST'])
@login_required
def reset_user_password():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    try:
        u = db.session.get(Users, data.get('id'))
        if not u: return jsonify({'success': False, 'message': 'User not found'})
        
        temp_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        u.set_password(temp_pass)
        u.is_temporary_password = True
        db.session.commit()
        
        return jsonify({'success': True, 'temp_password': temp_pass})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})