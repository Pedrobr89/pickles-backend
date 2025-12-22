from app.core.database import db
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import secrets

class User(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    github_id = db.Column(db.String(100), unique=True, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_admin = db.Column(db.Boolean, default=False)
    
    # Subscription fields
    stripe_customer_id = db.Column(db.String(120), nullable=True)
    subscription_status = db.Column(db.String(50), default='free') # free, active, past_due, canceled
    
    # Usage limit tracking (simplified)
    # Em um cenário real, isso poderia estar em uma tabela separada de logs de uso ou Redis
    daily_queries = db.Column(db.Integer, default=0)
    last_query_date = db.Column(db.Date, nullable=True)
    
    # Password reset fields
    reset_token = db.Column(db.String(100), nullable=True)
    reset_token_expiry = db.Column(db.DateTime, nullable=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def generate_reset_token(self):
        """Generates a secure reset token valid for 1 hour"""
        self.reset_token = secrets.token_urlsafe(32)
        self.reset_token_expiry = datetime.utcnow() + timedelta(hours=1)
        return self.reset_token
    
    def verify_reset_token(self, token):
        """Verifies if the provided token is valid and not expired"""
        if not self.reset_token or not self.reset_token_expiry:
            return False
        if self.reset_token != token:
            return False
        if datetime.utcnow() > self.reset_token_expiry:
            return False
        return True
    
    def clear_reset_token(self):
        """Clears the reset token after successful password reset"""
        self.reset_token = None
        self.reset_token_expiry = None

    def to_dict(self):
        return {
            'id': self.id,
            'email': self.email,
            'subscription_status': self.subscription_status,
            'created_at': self.created_at.isoformat()
        }
