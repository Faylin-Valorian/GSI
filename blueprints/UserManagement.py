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
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    try:
        u = db.session.get(Users, data.get('id'))
        if u:
            # Basic uniqueness check
            if data.get('username') != u.username and Users.query.filter_by(username=data.get('username')).first():
                 return jsonify({'success': False, 'message': 'Username taken'})
            
            u.username = data.get('username')
            u.email = data.get('email')
            db.session.commit()
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    return jsonify({'success': False, 'message': 'User not found'})

@user_mgmt_bp.route('/api/admin/user/<int:id>/toggle-role', methods=['POST'])
@login_required
def toggle_user_role(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    if current_user.id == id: return jsonify({'success': False, 'message': 'Cannot demote yourself.'})
    u = db.session.get(Users, id)
    if u:
        u.role = 'user' if u.role == 'admin' else 'admin'
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'User not found'})

@user_mgmt_bp.route('/api/admin/user/<int:id>/delete', methods=['POST'])
@login_required
def delete_user(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    if current_user.id == id: return jsonify({'success': False, 'message': 'Cannot delete yourself.'})
    u = db.session.get(Users, id)
    if u:
        db.session.delete(u)
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'User not found'})

@user_mgmt_bp.route('/api/admin/users/reset-password', methods=['POST'])
@login_required
def reset_user_password():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    u = db.session.get(Users, data.get('id'))
    if u:
        temp = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        u.set_password(temp)
        u.is_temporary_password = True
        db.session.commit()
        return jsonify({'success': True, 'temp_password': temp})
    return jsonify({'success': False})