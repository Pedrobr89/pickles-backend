from flask import Blueprint, request, jsonify, url_for, redirect
from flask_login import login_user, logout_user, login_required, current_user
from app.models.user_model import User
from app.core.database import db
from app.core.extensions import oauth

auth_bp = Blueprint('auth', __name__)

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

    if user and user.check_password(password):
        login_user(user)
        return jsonify({'message': 'Login realizado com sucesso', 'user': user.to_dict()})
    
    return jsonify({'error': 'Credenciais inválidas'}), 401

@auth_bp.route('/logout', methods=['POST'])
@login_required
def logout():
    logout_user()
    return jsonify({'message': 'Logout realizado com sucesso'})

@auth_bp.route('/me', methods=['GET'])
@login_required
def curren_user_info():
    return jsonify({'user': current_user.to_dict()})
