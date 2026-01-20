from flask import Blueprint, jsonify, render_template_string
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates, IndexingCounties
from utils import format_error

county_mgmt_bp = Blueprint('county_mgmt', __name__)

@county_mgmt_bp.route('/api/admin/counties/window')
@login_required
def county_window():
    if current_user.role != 'admin': return "Unauthorized", 403
    
    states = IndexingStates.query.filter_by(is_enabled=True).order_by(IndexingStates.state_name).all()
    
    html = """
    <div class="modal fade" id="countiesModal" tabindex="-1">
        <div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable">
            <div class="modal-content custom-panel">
                <div class="modal-header border-secondary">
                    <h5 class="modal-title"><i class="bi bi-geo-alt me-2"></i>Enable Counties</h5>
                    <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <div class="row g-2 mb-3">
                        <div class="col-md-4">
                            <label class="small text-muted">Filter by State</label>
                            <select id="countyStateSelect" class="form-select bg-dark text-light border-secondary" onchange="loadCountiesForTable()">
                                <option value="">Select State...</option>
                                {% for s in states %}
                                <option value="{{ s.fips_code }}">{{ s.state_name }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div class="col-md-8">
                             <label class="small text-muted">Search by Name</label>
                             <input type="text" id="countySearch" class="form-control bg-dark text-light border-secondary" placeholder="Type to search..." onkeyup="filterCountyTable()">
                        </div>
                    </div>
                    
                    <div class="table-responsive">
                        <table class="table table-hover align-middle table-dark table-sm">
                            <thead>
                                <tr>
                                    <th>County</th>
                                    <th class="text-center">Map Visibility</th>
                                    <th class="text-center">Global Status</th>
                                    <th class="text-end">Actions</th>
                                </tr>
                            </thead>
                            <tbody id="countiesTableBody">
                                <tr><td colspan="4" class="text-center text-muted py-4">Select a state to load counties</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <script>
        function loadCountiesForTable() {
            const fips = document.getElementById('countyStateSelect').value;
            if(!fips) return;
            
            document.getElementById('countiesTableBody').innerHTML = '<tr><td colspan="4" class="text-center py-4"><span class="spinner-border spinner-border-sm"></span> Loading...</td></tr>';
            
            fetch(`/api/admin/counties/list/${fips}`)
            .then(r=>r.json())
            .then(data => {
                let html = '';
                data.forEach(c => {
                    // RESTORED: Both Buttons (Active & Visible)
                    
                    // Button 1: Visibility (Eye) - Shows/Hides on Map
                    const visClass = c.is_enabled ? 'btn-outline-warning' : 'btn-outline-secondary';
                    const visIcon = c.is_enabled ? 'bi-eye-fill' : 'bi-eye-slash';
                    const visBtn = `<button class="btn btn-sm ${visClass} me-1" title="Toggle Map Visibility" onclick="toggleCountyVis(${c.id})"><i class="bi ${visIcon}"></i></button>`;
                        
                    // Button 2: Active (Power) - Allows/Blocks User Access
                    const actClass = c.is_active ? 'btn-success' : 'btn-danger';
                    const actText = c.is_active ? 'Active' : 'Inactive';
                    const actBtn = `<button class="btn btn-sm ${actClass} me-1" style="min-width: 80px;" title="Toggle User Access" onclick="toggleCountyActive(${c.id})">${actText}</button>`;
                    
                    // Status Badges
                    const visBadge = c.is_enabled ? '<span class="text-warning">Visible</span>' : '<span class="text-muted">Hidden</span>';
                    const actBadge = c.is_active ? '<span class="text-success">Active</span>' : '<span class="text-danger">Inactive</span>';

                    html += `<tr>
                        <td>${c.name}</td>
                        <td class="text-center">${visBadge}</td>
                        <td class="text-center">${actBadge}</td>
                        <td class="text-end">${actBtn} ${visBtn}</td>
                    </tr>`;
                });
                document.getElementById('countiesTableBody').innerHTML = html;
            });
        }

        function filterCountyTable() {
            const term = document.getElementById('countySearch').value.toLowerCase();
            const rows = document.querySelectorAll('#countiesTableBody tr');
            rows.forEach(r => {
                const txt = r.innerText.toLowerCase();
                r.style.display = txt.includes(term) ? '' : 'none';
            });
        }

        function toggleCountyVis(id) {
            fetch(`/api/admin/county/${id}/toggle`, {method:'POST'}).then(r=>r.json()).then(d=>{ if(d.success) loadCountiesForTable(); });
        }
        function toggleCountyActive(id) {
            fetch(`/api/admin/county/${id}/set-global-active`, {method:'POST'}).then(r=>r.json()).then(d=>{ if(d.success) loadCountiesForTable(); });
        }
    </script>
    """
    return render_template_string(html, states=states)

@county_mgmt_bp.route('/api/admin/counties/list/<state_fips>')
@login_required
def list_counties(state_fips):
    c_list = IndexingCounties.query.filter_by(state_fips=state_fips).order_by(IndexingCounties.county_name).all()
    return jsonify([{
        'id': c.id, 
        'name': c.county_name, 
        'is_active': c.is_active, 
        'is_enabled': c.is_enabled
    } for c in c_list])