from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from app.models.user_model import User
from app.core.database import db
from werkzeug.security import generate_password_hash

user_bp = Blueprint('user', __name__)

@user_bp.route('/profile', methods=['PUT'])
@login_required
def update_profile():
    data = request.get_json()
    
    # In a real app we might allow changing email, 
    # but that requires re-verification. Keeping it simple.
    
    # For now, maybe just auxiliary fields if we had them (name, company)
    # current_user.name = data.get('name', current_user.name)
    
    db.session.commit()
    return jsonify({'message': 'Perfil atualizado com sucesso'})

@user_bp.route('/password', methods=['PUT'])
@login_required
def change_password():
    data = request.get_json()
    current_password = data.get('current_password')
    new_password = data.get('new_password')
    
    if not current_user.check_password(current_password):
        return jsonify({'error': 'Senha atual incorreta'}), 400
        
    current_user.set_password(new_password)
    db.session.commit()
    
    return jsonify({'message': 'Senha alterada com sucesso'})

@user_bp.route('/settings', methods=['GET'])
@login_required
def get_settings():
    return jsonify(current_user.to_dict())
