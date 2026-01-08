from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from utils import format_error

db_compat_bp = Blueprint('db_compat', __name__)

@db_compat_bp.route('/api/tools/db-compatibility', methods=['POST'])
@login_required
def update_compatibility():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'})

    data = request.json
    target_level = data.get('target_level')
    # Check if the user has already said "Yes" to the warning
    is_confirmed = data.get('confirmed', False)

    if not target_level:
        return jsonify({'success': False, 'message': 'Target level required'})

    try:
        # Use AUTOCOMMIT connection for ALTER DATABASE
        with db.engine.connect() as connection:
            connection = connection.execution_options(isolation_level="AUTOCOMMIT")

            # 1. Get Current Database Name & Level
            # We query sys.databases to get the actual integer value of the current level
            result = connection.execute(text("SELECT name, compatibility_level FROM sys.databases WHERE name = DB_NAME()"))
            row = result.fetchone()
            
            if not row:
                raise Exception("Could not determine current database info")

            current_db = row[0]
            current_level = int(row[1])

            # 2. Validate Input
            try:
                new_level = int(target_level)
            except ValueError:
                return jsonify({'success': False, 'message': 'Level must be an integer (e.g., 150)'})

            # --- RULE 1: ABSOLUTE MINIMUM ---
            if new_level < 130:
                return jsonify({'success': False, 'message': 'Security Block: Compatibility Level cannot be lower than 130.'})

            # --- RULE 2: DOWNGRADE CONFIRMATION ---
            # If lowering the level AND the user hasn't confirmed yet...
            if new_level < current_level and not is_confirmed:
                return jsonify({
                    'success': False, 
                    'requires_confirmation': True, 
                    'message': f"Warning: You are downgrading from level {current_level} to {new_level}. This may break features. Are you sure?"
                })

            # 3. Execute Change
            # If we get here, either level >= current OR is_confirmed is True
            sql = f"ALTER DATABASE [{current_db}] SET COMPATIBILITY_LEVEL = {new_level}"
            connection.execute(text(sql))

        return jsonify({'success': True, 'message': f'Compatibility Level set to {new_level}'})

    except Exception as e:
        msg = format_error(e)
        return jsonify({'success': False, 'message': msg})