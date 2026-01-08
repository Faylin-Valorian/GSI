import os
from flask import Blueprint, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from utils import format_error  # <--- IMPORTED

# Matches your app.py registration: setup_procedures_bp
setup_procedures_bp = Blueprint('setup_procedures', __name__)

@setup_procedures_bp.route('/api/tools/setup-procedures', methods=['POST'])
@login_required
def setup_procedures():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'})

    # Define path to SQL folder
    sql_folder = os.path.join(current_app.root_path, 'sql', 'procedures')
    
    if not os.path.exists(sql_folder):
        # Friendly message (no debug toggle needed for logical checks)
        return jsonify({'success': False, 'message': 'SQL Procedures folder not found.'})

    processed = 0
    errors = []

    try:
        # Get list of .sql files
        files = [f for f in os.listdir(sql_folder) if f.endswith('.sql')]
        
        if not files:
            return jsonify({'success': False, 'message': 'No .sql files found.'})

        # Iterate and Execute
        for filename in files:
            file_path = os.path.join(sql_folder, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    
                    # Execute the SQL script
                    db.session.execute(text(sql_content))
                    db.session.commit()
                    processed += 1

            except Exception as e:
                db.session.rollback()
                # --- NEW ERROR HANDLING ---
                # Returns "System Error: ..." to user, or "[DEBUG] ..." to you
                msg = format_error(e)
                errors.append(f"{filename}: {msg}")

        # Summary
        if processed == 0 and errors:
            return jsonify({'success': False, 'message': f"Failed. {errors[0]}"})
        
        msg = f"Executed {processed} procedures."
        if errors:
            msg += f" ({len(errors)} failed)"
            
        return jsonify({'success': True, 'message': msg})

    except Exception as e:
        # Global folder access errors
        msg = format_error(e)
        return jsonify({'success': False, 'message': msg})