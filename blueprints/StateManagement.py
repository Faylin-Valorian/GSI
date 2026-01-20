from flask import Blueprint, jsonify, render_template_string
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates
from utils import format_error

state_mgmt_bp = Blueprint('state_mgmt', __name__)

@state_mgmt_bp.route('/api/admin/states/window')
@login_required
def state_window():
    if current_user.role != 'admin': return "Unauthorized", 403
    
    states = IndexingStates.query.order_by(IndexingStates.state_name).all()
    
    html = """
    <div class="modal fade" id="statesModal" tabindex="-1">
        <div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable">
            <div class="modal-content custom-panel">
                <div class="modal-header border-secondary">
                    <h5 class="modal-title"><i class="bi bi-map me-2"></i>Enable States</h5>
                    <button class="btn btn-sm btn-outline-warning ms-3" onclick="openSeedConfirm()">
                        <i class="bi bi-database-add me-1"></i>Initialize Data
                    </button>
                    <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <table class="table table-hover align-middle table-dark">
                        <thead><tr><th>State</th><th>FIPS</th><th>Status</th><th class="text-end">Actions</th></tr></thead>
                        <tbody>
                            {% for s in states %}
                            <tr>
                                <td>{{ s.state_name }}</td>
                                <td>{{ s.fips_code }}</td>
                                <td class="{{ 'text-success' if s.is_enabled else 'text-muted' }}">
                                    {{ 'Enabled' if s.is_enabled else 'Disabled' }}
                                </td>
                                <td class="text-end">
                                    <button class="btn btn-sm {{ 'btn-warning' if s.is_enabled else 'btn-success' }}" 
                                            onclick="toggleState({{ s.id }}, this)">
                                        {{ 'Disable' if s.is_enabled else 'Enable' }}
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

    <div class="modal fade" id="seedConfirmModal" tabindex="-1" style="z-index: 1060;">
        <div class="modal-dialog modal-dialog-centered">
            <div class="modal-content custom-panel border-warning">
                <div class="modal-header border-secondary bg-warning text-dark">
                    <h5 class="modal-title"><i class="bi bi-exclamation-triangle-fill me-2"></i>Initialize Database</h5>
                </div>
                <div class="modal-body">
                    <p>This will scan the map files and populate the database with all US States and Counties.</p>
                    <p class="small text-muted">This process may take a few seconds. Existing data will not be duplicated.</p>
                    <div id="seedLoader" class="text-center d-none mt-3">
                        <div class="spinner-border text-warning" role="status"></div>
                        <div class="small mt-1">Processing...</div>
                    </div>
                </div>
                <div class="modal-footer border-secondary">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" class="btn btn-warning fw-bold" onclick="executeSeeding()">Yes, Initialize</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        // --- STATE LOGIC ---
        function toggleState(id, btn) {
            btn.disabled = true;
            fetch(`/api/admin/state/${id}/toggle`, {method:'POST'})
            .then(r=>r.json()).then(d=>{
                if(d.success) {
                    // Refresh this window
                    const modal = bootstrap.Modal.getInstance(document.getElementById('statesModal'));
                    modal.hide();
                    setTimeout(openStatesManager, 300);
                } else {
                    alert(d.message);
                    btn.disabled = false;
                }
            });
        }

        // --- SEEDING LOGIC ---
        var seedModalObj;
        function openSeedConfirm() {
            // Hide main modal temporarily
            const mainModal = bootstrap.Modal.getInstance(document.getElementById('statesModal'));
            if(mainModal) mainModal.hide();
            
            seedModalObj = new bootstrap.Modal(document.getElementById('seedConfirmModal'));
            seedModalObj.show();
        }

        function executeSeeding() {
            document.getElementById('seedLoader').classList.remove('d-none');
            
            fetch('/api/admin/seed', {method:'POST'})
            .then(r => r.json())
            .then(d => {
                document.getElementById('seedLoader').classList.add('d-none');
                seedModalObj.hide();
                
                if(d.success){ 
                    alert(d.message);
                    window.location.reload(); // Full reload to update maps and grids
                } else { 
                    alert("Seeding failed: " + d.message);
                    // Re-open states modal if failed
                    setTimeout(openStatesManager, 500);
                }
            })
            .catch(e => { 
                alert("Error: "+e);
                seedModalObj.hide();
            });
        }
    </script>
    """
    return render_template_string(html, states=states)

@state_mgmt_bp.route('/api/admin/state/<int:id>/toggle', methods=['POST'])
@login_required
def toggle_state(id):
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        s = db.session.get(IndexingStates, id)
        if s: 
            s.is_enabled = not s.is_enabled
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'State not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})