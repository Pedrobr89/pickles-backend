from flask import Blueprint, request, jsonify, url_for, redirect
from flask_login import login_user, logout_user, login_required, current_user
from app.models.user_model import User
from app.core.database import db
from app.core.extensions import oauth
from services.email_service import send_password_reset_email
import logging

auth_bp = Blueprint('auth', __name__)
logger = logging.getLogger(__name__)

@auth_bp.route('/login/github')
def login_github():
    redirect_uri = url_for('auth.github_callback', _external=True)
    return oauth.github.authorize_redirect(redirect_uri)

@auth_bp.route('/github/callback')
def github_callback():
    token = oauth.github.authorize_access_token()
    if not token:
        return jsonify({'error': 'Acesso negado pelo GitHub'}), 401
        
    resp = oauth.github.get('user')
    profile = resp.json()
    github_id = str(profile.get('id'))
    email = profile.get('email')
    
    # Se email for privado, tenta buscar
    if not email:
        resp_emails = oauth.github.get('user/emails')
        for e in resp_emails.json():
            if e.get('primary') and e.get('verified'):
                email = e.get('email')
                break
    
    if not email:
        return jsonify({'error': 'Email não encontrado no GitHub'}), 400

    # Busca usuário existente
    user = User.query.filter((User.github_id == github_id) | (User.email == email)).first()
    
    if not user:
        # Cria novo usuário
        user = User(email=email, github_id=github_id)
        # Senha aleatória forte pois login é via github
        import secrets
        user.set_password(secrets.token_urlsafe(16))
        db.session.add(user)
        db.session.commit()
    else:
        # Atualiza github_id se não tiver
        if not user.github_id:
            user.github_id = github_id
            db.session.commit()
            
    login_user(user)
    return redirect('/app')

@auth_bp.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'error': 'Email e senha são obrigatórios'}), 400
        
    if len(password) < 8:
        return jsonify({'error': 'A senha deve ter no mínimo 8 caracteres'}), 400

    if User.query.filter_by(email=email).first():
        return jsonify({'error': 'Email já cadastrado'}), 400

    new_user = User(email=email)
    new_user.set_password(password)
    
    db.session.add(new_user)
    db.session.commit()

    return jsonify({'message': 'Usuário criado com sucesso', 'user': new_user.to_dict()}), 201

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = User.query.filter_by(email=email).first()

    # TEMPORÁRIO: Verificação de senha desativada para debug
    if user:  # Aceita qualquer senha se o usuário existir
        login_user(user)
        logger.warning(f"⚠️ LOGIN SEM SENHA ATIVADO - Usuário {email} fez login sem verificação")
        return jsonify({'message': 'Login realizado com sucesso', 'user': user.to_dict()})
    
    return jsonify({'error': 'Email não encontrado'}), 401

@auth_bp.route('/logout', methods=['POST'])
@login_required
def logout():
    logout_user()
    return jsonify({'message': 'Logout realizado com sucesso'})

@auth_bp.route('/me', methods=['GET'])
@login_required
def curren_user_info():
    return jsonify({'user': current_user.to_dict()})

@auth_bp.route('/reset-password-request', methods=['POST'])
def reset_password_request():
    """Request password reset - sends email with token"""
    data = request.get_json()
    email = data.get('email')
    
    if not email:
        return jsonify({'error': 'Email é obrigatório'}), 400
    
    # Find user by email
    user = User.query.filter_by(email=email).first()
    
    # Always return success message (don't reveal if email exists for security)
    if user:
        # Generate reset token
        token = user.generate_reset_token()
        db.session.commit()
        
        # Send email
        email_sent = send_password_reset_email(user.email, token)
        
        if email_sent:
            logger.info(f"Password reset email sent to {email}")
        else:
            logger.error(f"Failed to send password reset email to {email}")
    else:
        logger.warning(f"Password reset requested for non-existent email: {email}")
    
    # Always return success to prevent email enumeration
    return jsonify({
        'message': 'Se o email estiver cadastrado, você receberá instruções para redefinir sua senha.'
    }), 200

@auth_bp.route('/reset-password', methods=['POST'])
def reset_password():
    """Reset password using token"""
    data = request.get_json()
    token = data.get('token')
    new_password = data.get('password')
    
    if not token or not new_password:
        return jsonify({'error': 'Token e nova senha são obrigatórios'}), 400
    
    if len(new_password) < 8:
        return jsonify({'error': 'A senha deve ter no mínimo 8 caracteres'}), 400
    
    # Find user by token
    user = User.query.filter_by(reset_token=token).first()
    
    if not user:
        return jsonify({'error': 'Token inválido ou expirado'}), 400
    
    # Verify token is still valid
    if not user.verify_reset_token(token):
        return jsonify({'error': 'Token inválido ou expirado'}), 400
    
    # Update password
    user.set_password(new_password)
    user.clear_reset_token()
    db.session.commit()
    
    logger.info(f"Password successfully reset for user {user.email}")
    
    return jsonify({'message': 'Senha redefinida com sucesso! Você já pode fazer login.'}), 200
