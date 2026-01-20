from flask import Blueprint, jsonify, render_template_string, request
from flask_login import login_required, current_user
from extensions import db
from models import Users
from utils import format_error
import random, string

user_mgmt_bp = Blueprint('user_mgmt', __name__)

@user_mgmt_bp.route('/api/admin/users/window')
@login_required
def user_window():
    if current_user.role != 'admin': return "Unauthorized", 403
    
    users = Users.query.all()
    
    html = """
    <div class="modal fade" id="accountsModal" tabindex="-1">
        <div class="modal-dialog modal-xl modal-dialog-centered">
            <div class="modal-content custom-panel">
                <div class="modal-header border-secondary">
                    <h5 class="modal-title"><i class="bi bi-people me-2"></i>User Accounts</h5>
                    <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <div class="table-responsive">
                        <table class="table table-hover align-middle table-dark">
                            <thead><tr><th>User</th><th>Email</th><th>Role</th><th>Status</th><th class="text-end">Actions</th></tr></thead>
                            <tbody>
                                {% for u in users %}
                                <tr>
                                    <td class="fw-bold">{{ u.username }}</td>
                                    <td>{{ u.email }}</td>
                                    <td><span class="badge {{ 'bg-warning text-dark' if u.role=='admin' else 'bg-secondary' }}">{{ u.role }}</span></td>
                                    <td>{{ 'Locked' if u.is_locked else 'Active' }}</td>
                                    <td class="text-end">
                                        <button class="btn btn-sm btn-outline-primary me-1" title="Edit User" 
                                            onclick="openUserEditModal({{ u.id }}, '{{ u.username }}', '{{ u.email }}')">
                                            <i class="bi bi-pencil-fill"></i>
                                        </button>
                                        <button class="btn btn-sm {{ 'btn-outline-warning' if u.role=='admin' else 'btn-outline-secondary' }} me-1" title="Toggle Admin Role"
                                                onclick="toggleUserRole({{ u.id }})">
                                            <i class="bi bi-shield-lock-fill"></i>
                                        </button>
                                        <button class="btn btn-sm btn-outline-danger" title="Delete User"
                                                onclick="deleteUser({{ u.id }}, '{{ u.username }}')">
                                            <i class="bi bi-trash-fill"></i>
                                        </button>
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="modal fade" id="userEditModal" tabindex="-1" style="z-index: 1100;">
        <div class="modal-dialog modal-dialog-centered">
            <div class="modal-content custom-panel border-primary">
                <div class="modal-header border-secondary">
                    <h5 class="modal-title"><i class="bi bi-person-gear me-2"></i>Edit User</h5>
                    <button type="button" class="btn-close btn-close-white" onclick="closeUserEditModal()"></button>
                </div>
                <div class="modal-body">
                    <input type="hidden" id="editUserId">
                    
                    <div class="mb-3">
                        <label class="form-label text-muted small">Username</label>
                        <input type="text" id="editUsername" class="form-control text-light bg-dark border-secondary">
                    </div>
                    <div class="mb-3">
                        <label class="form-label text-muted small">Email Address</label>
                        <input type="email" id="editEmail" class="form-control text-light bg-dark border-secondary">
                    </div>
                    
                    <div class="d-grid mb-4">
                        <button class="btn btn-primary btn-sm" onclick="submitUserEdit()">Save Changes</button>
                    </div>

                    <hr class="border-secondary">

                    <h6 class="text-warning small text-uppercase fw-bold mb-3"><i class="bi bi-key-fill me-2"></i>Security</h6>
                    <div class="d-flex justify-content-between align-items-center">
                        <span class="text-muted small">Reset Password to temporary value</span>
                        <button class="btn btn-outline-warning btn-sm" onclick="resetUserPassword()">Generate New Password</button>
                    </div>
                    
                    <div id="resetPasswordResult" class="mt-3 d-none">
                        <div class="alert alert-warning mb-0 py-2">
                            <small class="d-block text-dark fw-bold mb-1">Temporary Password:</small>
                            <div class="input-group input-group-sm">
                                <input type="text" id="tempPassDisplay" class="form-control font-monospace text-center" readonly>
                                <button class="btn btn-dark" onclick="navigator.clipboard.writeText(document.getElementById('tempPassDisplay').value)">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // --- ACTION LOGIC ---
        var userEditModalObj;

        function toggleUserRole(id) {
            if(!confirm("Change user role?")) return;
            fetch(`/api/admin/user/${id}/toggle-role`, {method:'POST'}).then(r=>r.json()).then(d=>{ 
                if(d.success) refreshUsers(); 
                else alert(d.message);
            });
        }
        function deleteUser(id, name) {
            if(!confirm(`Delete ${name} permanently?`)) return;
            fetch(`/api/admin/user/${id}/delete`, {method:'POST'}).then(r=>r.json()).then(d=>{ 
                if(d.success) refreshUsers(); 
                else alert(d.message);
            });
        }

        // --- EDIT MODAL LOGIC ---
        function openUserEditModal(id, username, email) {
            // Hide main modal
            bootstrap.Modal.getInstance(document.getElementById('accountsModal')).hide();
            
            document.getElementById('editUserId').value = id;
            document.getElementById('editUsername').value = username;
            document.getElementById('editEmail').value = email;
            document.getElementById('resetPasswordResult').classList.add('d-none');
            
            userEditModalObj = new bootstrap.Modal(document.getElementById('userEditModal'));
            userEditModalObj.show();
        }

        function closeUserEditModal() {
            if(userEditModalObj) userEditModalObj.hide();
            setTimeout(openUserManager, 300); // Re-open list
        }

        function submitUserEdit() {
            const id = document.getElementById('editUserId').value;
            fetch(`/api/admin/user/${id}/update`, {
                method: 'POST', 
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    username: document.getElementById('editUsername').value,
                    email: document.getElementById('editEmail').value
                })
            }).then(r=>r.json()).then(d=>{
                if(d.success) { alert("Updated!"); closeUserEditModal(); }
                else alert(d.message);
            });
        }

        function resetUserPassword() {
            if(!confirm("Reset this user's password?")) return;
            const id = document.getElementById('editUserId').value;
            fetch(`/api/admin/user/${id}/reset-password`, {method:'POST'})
            .then(r=>r.json()).then(d=>{
                if(d.success) {
                    document.getElementById('tempPassDisplay').value = d.temp_pass;
                    document.getElementById('resetPasswordResult').classList.remove('d-none');
                } else alert(d.message);
            });
        }

        function refreshUsers() {
            // Re-fetch the whole blueprint modal to refresh list
            const modal = bootstrap.Modal.getInstance(document.getElementById('accountsModal'));
            modal.hide();
            setTimeout(openUserManager, 300);
        }
    </script>
    """
    return render_template_string(html, users=users)

# ... (Keep existing API Routes below: get_users_api, update, reset, etc.) ...
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
        if new_username and new_username != u.username:
            if Users.query.filter_by(username=new_username).first(): return jsonify({'success': False, 'message': 'Username already taken.'})
            u.username = new_username
        if new_email and new_email != u.email:
            if Users.query.filter_by(email=new_email).first(): return jsonify({'success': False, 'message': 'Email already taken.'})
            u.email = new_email
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

@user_mgmt_bp.route('/api/admin/user/<int:id>/reset-password', methods=['POST'])
@login_required
def reset_user_password(id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    try:
        u = db.session.get(Users, id)
        if not u: return jsonify({'success': False, 'message': 'User not found'})
        temp_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        u.set_password(temp_pass)
        u.is_temporary_password = True
        db.session.commit()
        return jsonify({'success': True, 'temp_pass': temp_pass})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

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