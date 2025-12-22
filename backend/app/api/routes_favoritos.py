"""
API Routes para Favoritos B2G
Gerencia favoritos de licitações do usuário
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import sqlite3
import json
from datetime import datetime
from pathlib import Path

bp = Blueprint('favoritos', __name__, url_prefix='/api/favoritos')

# Path do banco de dados
DB_PATH = Path(__file__).parent.parent.parent / 'users.db'


def get_db_connection():
    """Cria conexão com banco de dados"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def require_auth(f):
    """Decorator para exigir autenticação (simplificado)"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # TODO: Implementar autenticação real
        # Por enquanto, usar usuario_id = 1 para testes
        return f(*args, **kwargs)
    return decorated_function


def get_current_user_id():
    """Retorna ID do usuário atual (mock)"""
    # TODO: Pegar do session/token
    return 1


@bp.route('/licitacao', methods=['POST'])
@require_auth
def adicionar_favorito():
    """Adiciona licitação aos favoritos"""
    try:
        data = request.get_json()
        usuario_id = get_current_user_id()
        
        licitacao_id = data.get('licitacao_id')
        licitacao_data = data.get('licitacao_data', {})
        notas = data.get('notas', '')
        
        if not licitacao_id:
            return jsonify({'erro': 'licitacao_id é obrigatório'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO licitacoes_favoritas 
                (usuario_id, licitacao_id, licitacao_data, notas)
                VALUES (?, ?, ?, ?)
            ''', (
                usuario_id,
                licitacao_id,
                json.dumps(licitacao_data),
                notas
            ))
            
            conn.commit()
            favorito_id = cursor.lastrowid
            
            return jsonify({
                'sucesso': True,
                'mensagem': 'Licitação adicionada aos favoritos',
                'favorito_id': favorito_id
            }), 201
            
        except sqlite3.IntegrityError:
            return jsonify({'erro': 'Licitação já está nos favoritos'}), 409
        finally:
            conn.close()
            
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@bp.route('/licitacao/<licitacao_id>', methods=['DELETE'])
@require_auth
def remover_favorito(licitacao_id):
    """Remove licitação dos favoritos"""
    try:
        usuario_id = get_current_user_id()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM licitacoes_favoritas
            WHERE usuario_id = ? AND licitacao_id = ?
        ''', (usuario_id, licitacao_id))
        
        conn.commit()
        rows_deleted = cursor.rowcount
        conn.close()
        
        if rows_deleted > 0:
            return jsonify({
                'sucesso': True,
                'mensagem': 'Favorito removido'
            })
        else:
            return jsonify({'erro': 'Favorito não encontrado'}), 404
            
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@bp.route('/licitacoes', methods=['GET'])
@require_auth
def listar_favoritos():
    """Lista todos os favoritos do usuário"""
    try:
        usuario_id = get_current_user_id()
        pagina = int(request.args.get('pagina', 1))
        por_pagina = int(request.args.get('por_pagina', 20))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Contar total
        cursor.execute('''
            SELECT COUNT(*) as total
            FROM licitacoes_favoritas
            WHERE usuario_id = ?
        ''', (usuario_id,))
        
        total = cursor.fetchone()['total']
        
        # Buscar favoritos paginados
        offset = (pagina - 1) * por_pagina
        
        cursor.execute('''
            SELECT 
                id,
                licitacao_id,
                licitacao_data,
                notas,
                criado_em,
                atualizado_em
            FROM licitacoes_favoritas
            WHERE usuario_id = ?
            ORDER BY criado_em DESC
            LIMIT ? OFFSET ?
        ''', (usuario_id, por_pagina, offset))
        
        favoritos = []
        for row in cursor.fetchall():
            try:
                licitacao_data = json.loads(row['licitacao_data']) if row['licitacao_data'] else {}
            except:
                licitacao_data = {}
                
            favoritos.append({
                'id': row['id'],
                'licitacao_id': row['licitacao_id'],
                'licitacao_data': licitacao_data,
                'notas': row['notas'],
                'criado_em': row['criado_em'],
                'atualizado_em': row['atualizado_em']
            })
        
        conn.close()
        
        return jsonify({
            'favoritos': favoritos,
            'total': total,
            'pagina': pagina,
            'por_pagina': por_pagina,
            'total_paginas': (total + por_pagina - 1) // por_pagina
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@bp.route('/licitacao/<licitacao_id>/verificar', methods=['GET'])
@require_auth
def verificar_favorito(licitacao_id):
    """Verifica se licitação está favoritada"""
    try:
        usuario_id = get_current_user_id()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id FROM licitacoes_favoritas
            WHERE usuario_id = ? AND licitacao_id = ?
        ''', (usuario_id, licitacao_id))
        
        favorito = cursor.fetchone()
        conn.close()
        
        return jsonify({
            'favoritado': favorito is not None,
            'favorito_id': favorito['id'] if favorito else None
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@bp.route('/licitacao/<favorito_id>/notas', methods=['PUT'])
@require_auth
def atualizar_notas(favorito_id):
    """Atualiza notas de um favorito"""
    try:
        data = request.get_json()
        usuario_id = get_current_user_id()
        notas = data.get('notas', '')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE licitacoes_favoritas
            SET notas = ?, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = ? AND usuario_id = ?
        ''', (notas, favorito_id, usuario_id))
        
        conn.commit()
        rows_updated = cursor.rowcount
        conn.close()
        
        if rows_updated > 0:
            return jsonify({
                'sucesso': True,
                'mensagem': 'Notas atualizadas'
            })
        else:
            return jsonify({'erro': 'Favorito não encontrado'}), 404
            
    except Exception as e:
        return jsonify({'erro': str(e)}), 500
