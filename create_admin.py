import sys
import os
sys.path.append(os.getcwd())
from backend.server import create_app
from backend.app.core.database import db
from backend.app.models.user_model import User

app = create_app('development')
with app.app_context():
    email = input("Email do admin: ")
    password = input("Senha do admin: ")
    
    user = User.query.filter_by(email=email).first()
    if user:
        user.is_admin = True
        user.set_password(password)
        print(f"Usuário {email} atualizado para Admin.")
    else:
        user = User(email=email, is_admin=True)
        user.set_password(password)
        db.session.add(user)
        print(f"Admin {email} criado com sucesso.")
    
    db.session.commit()
