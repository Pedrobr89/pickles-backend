from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from app.models.user_model import User
from app.core.database import db
from datetime import datetime, date

admin_bp = Blueprint('admin', __name__)

def check_admin():
    if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        return False
    return True

@admin_bp.route('/stats', methods=['GET'])
@login_required
def get_stats():
    if not check_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    
    total_users = User.query.count()
    
    # Novos hoje
    today = datetime.utcnow().date()
    # SQLite function for date might vary, but for SQLAlchemy with Python objects:
    new_users_today = User.query.filter(db.func.date(User.created_at) == today).count()
    
    # Assinantes ativos (status != 'free')
    active_subs = User.query.filter(User.subscription_status != 'free').count()
    
    # Placeholder for MRR - would need a Payment/Subscription model
    mrr = active_subs * 97.0 # Assuming Pro plan price
    
    return jsonify({
        'total_users': total_users,
        'new_users_today': new_users_today,
        'active_subs': active_subs,
        'mrr': mrr
    })

@admin_bp.route('/users', methods=['GET'])
@login_required
def list_users():
    if not check_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    
    # Simple pagination
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    users_paginated = User.query.order_by(User.created_at.desc()).paginate(page=page, per_page=per_page)
    
    users_list = []
    for u in users_paginated.items:
        users_list.append({
            'id': u.id,
            'email': u.email,
            'created_at': u.created_at.strftime('%Y-%m-%d %H:%M'),
            'plan': u.subscription_status.capitalize(),
            'usage': u.daily_queries,
            'status': 'Active', # Placeholder
            'is_admin': u.is_admin
        })
        
    return jsonify({
        'users': users_list,
        'total': users_paginated.total,
        'pages': users_paginated.pages,
        'current_page': page
    })
