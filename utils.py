from flask import session
import re

def format_error(e):
    """
    Returns a detailed error if Debug Mode is ON.
    Returns a sanitized, friendly error if Debug Mode is OFF.
    """
    # 1. DEBUG MODE: Return everything (Raw Python/SQL error)
    if session.get('debug_mode'):
        return f"[DEBUG] {str(e)}"
    
    # 2. USER MODE: Translate technical jargon to English
    
    # Convert error to string
    raw_msg = str(getattr(e, 'orig', e))
    msg_lower = raw_msg.lower()

    # --- SPECIFIC WINDOWS/NET USE CLEANING ---
    # Detects "System error 67 has occurred. The network name cannot be found."
    # and returns just "The network name cannot be found."
    windows_error_match = re.search(r"system error \d+ has occurred\.\s*(.*)", raw_msg, re.IGNORECASE)
    if windows_error_match:
        friendly_part = windows_error_match.group(1).strip()
        return f"Connection Failed: {friendly_part}"

    # --- CATEGORY A: USER FIXABLE ERRORS ---
    if "access is denied" in msg_lower or "error code 5" in msg_lower:
        return "Access Denied: The server cannot read the specified file or folder. Check permissions."
    
    if "the network path was not found" in msg_lower or "error code 53" in msg_lower:
        return "Connection Failed: Could not find the network path. Check if the drive is connected."
    
    if "no headers found" in msg_lower or "empty file" in msg_lower:
        return "File Error: The selected file appears to be empty or missing headers."

    if "invalid level" in msg_lower:
        return "Input Error: Please enter a valid compatibility level (e.g., 150)."

    # --- CATEGORY B: SYSTEM/IT ERRORS ---
    if "alter database statement not allowed" in msg_lower:
        return "Configuration Error: The database setting could not be updated due to a transaction restriction. Please contact IT Support."
    
    if "complexity error" in msg_lower or "8632" in msg_lower:
        return "Processing Error: The file structure is too complex for the database. Please contact IT Support."
    
    if "login failed" in msg_lower:
        return "Authentication Error: The server could not connect to the database. Please contact IT Support."
    
    if "syntax error" in msg_lower or "42000" in raw_msg:
        return "System Error: An internal database command failed. Please contact IT Support."

    # --- FALLBACK ---
    return "An unexpected error occurred. Please try again or contact IT Support."