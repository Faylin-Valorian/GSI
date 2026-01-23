import os
from flask import current_app
from werkzeug.utils import secure_filename

def format_error(e):
    """Standardized error formatting."""
    return str(e)

def ensure_folders(state_name, county_name=None):
    """Creates the standard directory structure in /data."""
    root = os.path.join(current_app.root_path, 'data')
    
    # Ensure State Folder
    s_clean = secure_filename(state_name)
    state_path = os.path.join(root, s_clean)
    if not os.path.exists(state_path):
        os.makedirs(state_path)
        
    # Ensure County Folders if provided
    if county_name:
        c_clean = secure_filename(county_name)
        county_path = os.path.join(state_path, c_clean)
        if not os.path.exists(county_path):
            os.makedirs(county_path)
            
        standard_subfolders = [
            'eData Files',
            'Keli Files',
            'Images',
            'eData Errors',
            'Keli Errors'
        ]
        
        for folder in standard_subfolders:
            path = os.path.join(county_path, folder)
            if not os.path.exists(path):
                os.makedirs(path)