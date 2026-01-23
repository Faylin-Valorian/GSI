from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from datetime import datetime

class Users(db.Model, UserMixin):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(50), default='user')
    
    # EXACT MATCH TO SQL:
    is_verified = db.Column(db.Boolean, default=False)
    verification_code = db.Column(db.String(6), nullable=True)
    is_locked = db.Column(db.Boolean, default=False)
    is_temporary_password = db.Column(db.Boolean, default=False)
    current_working_county_id = db.Column(db.Integer, db.ForeignKey('indexing_counties.id'), nullable=True)

    # REMOVED: status (Does not exist in DB)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class IndexingStates(db.Model):
    __tablename__ = 'indexing_states'
    id = db.Column(db.Integer, primary_key=True)
    state_name = db.Column(db.String(100), nullable=False)
    state_abbr = db.Column(db.String(5), nullable=True)
    fips_code = db.Column(db.String(10), unique=True, nullable=False)
    is_enabled = db.Column(db.Boolean, default=False)
    is_locked = db.Column(db.Boolean, default=False)

class IndexingCounties(db.Model):
    __tablename__ = 'indexing_counties'
    id = db.Column(db.Integer, primary_key=True)
    county_name = db.Column(db.String(100), nullable=False)
    geo_id = db.Column(db.String(50), unique=True, nullable=False) # Matches SQL 'geo_id'
    state_fips = db.Column(db.String(10), nullable=False)
    is_active = db.Column(db.Boolean, default=False)
    is_enabled = db.Column(db.Boolean, default=False)
    is_locked = db.Column(db.Boolean, default=False)
    notes = db.Column(db.Text, nullable=True)

class CountyImages(db.Model):
    __tablename__ = 'county_images'
    id = db.Column(db.Integer, primary_key=True)
    county_id = db.Column(db.Integer, db.ForeignKey('indexing_counties.id'))
    image_path = db.Column(db.String(255))

class UnindexedImages(db.Model):
    __tablename__ = 'unindexed_images'
    id = db.Column(db.Integer, primary_key=True)
    county_id = db.Column(db.Integer, db.ForeignKey('indexing_counties.id'), nullable=False)
    full_path = db.Column(db.String(4000), nullable=False)
    book_name = db.Column(db.String(255))
    page_name = db.Column(db.String(255))
    require_indexing = db.Column(db.Boolean, default=False)
    scanned_at = db.Column(db.DateTime, default=datetime.utcnow)

class GenericDataImport(db.Model):
    __tablename__ = 'GenericDataImport'
    id = db.Column(db.Integer, primary_key=True)
    # Managed dynamically