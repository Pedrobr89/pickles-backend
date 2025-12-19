from app.core.database import db
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

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

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def to_dict(self):
        return {
            'id': self.id,
            'email': self.email,
            'subscription_status': self.subscription_status,
            'created_at': self.created_at.isoformat()
        }
