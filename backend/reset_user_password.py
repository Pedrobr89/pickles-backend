"""
Script para redefinir senha de um usuário
"""

import sys
import os
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.database import db
from app.models.user_model import User
from flask import Flask

def reset_password(email, new_password):
    """Reset user password"""
    
    # Create Flask app
    app = Flask(__name__)
    
    # Get absolute path to database
    backend_dir = Path(__file__).parent
    db_path = backend_dir / 'users.db'
    
    # Get database URL from environment or use absolute path
    db_url = os.environ.get('DATABASE_URL', f'sqlite:///{db_path}')
    app.config['SQLALCHEMY_DATABASE_URI'] = db_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    db.init_app(app)
    
    with app.app_context():
        # Find user
        user = User.query.filter_by(email=email).first()
        
        if not user:
            print(f"❌ Usuário {email} não encontrado no banco de dados.")
            print(f"   Banco de dados: {db_url}")
            return False
        
        # Reset password
        user.set_password(new_password)
        db.session.commit()
        
        print(f"✅ Senha redefinida com sucesso!")
        print(f"   Email: {email}")
        print(f"   Nova senha: {new_password}")
        return True

if __name__ == '__main__':
    # Load .env if exists
    from dotenv import load_dotenv
    load_dotenv()
    
    # Reset password for pedronastari@yahoo.com
    email = 'pedronastari@yahoo.com'
    new_password = 'Pickles@2024'  # Nova senha
    
    print(f"Redefinindo senha para {email}...")
    reset_password(email, new_password)
