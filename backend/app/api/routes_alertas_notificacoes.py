"""
API Routes para Sistema de Alertas e Notificações B2G (Sprint 3)
"""

from flask import Blueprint, jsonify, request
import logging
import sqlite3
from services.services_alertas_b2g import AlertasB2GService
from services.services_notificacoes_b2g import NotificacoesB2GService
from utils.utils_error_handler import handle_errors

logger = logging.getLogger(__name__)
alertas_bp = Blueprint('alertas', __name__)
notificacoes_bp = Blueprint('notificacoes', __name__)


def _get_db_connection():
    """Helper para obter conexão com BD"""
    try:
        db_path = 'backend/database/users.db'
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar BD: {e}")
        return None


def _get_current_user_id():
    """
    TODO: Implementar autenticação real
    Por enquanto retorna ID fixo para desenvolvimento
    """
    return 1


# ==========================================
# ROTAS DE ALERTAS
# ==========================================

@alertas_bp.route('/', methods=['GET'])
@handle_errors
def listar_todos_alertas():
    """Lista todos os alertas do usuário"""
    usuario_id = _get_current_user_id()
    apenas_ativos = request.args.get('apenas_ativos', 'true').lower() == 'true'
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        alertas = service.listar_alertas(usuario_id, apenas_ativos)
        
        return jsonify({
            'sucesso': True,
            'total': len(alertas),
            'alertas': alertas
        })
    finally:
        db.close()


@alertas_bp.route('/', methods=['POST'])
@handle_errors
def criar_alerta():
    """Cria um novo alerta personalizado"""
    usuario_id = _get_current_user_id()
    data = request.get_json() or {}
    
    nome = data.get('nome')
    criterios = data.get('criterios', {})
    frequencia = data.get('frequencia', 'imediato')
    canais = data.get('canais', ['in_app', 'email'])
    
    if not nome:
        return jsonify({'erro': 'Nome do alerta é obrigatório'}), 400
    
    if not criterios:
        return jsonify({'erro': 'Critérios são obrigatórios'}), 400
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        alerta = service.criar_alerta(
            usuario_id=usuario_id,
            nome=nome,
            criterios=criterios,
            frequencia=frequencia,
            canais=canais
        )
        
        if not alerta:
            return jsonify({'erro': 'Erro ao criar alerta'}), 500
        
        return jsonify({
            'sucesso': True,
            'alerta': alerta
        }), 201
    finally:
        db.close()


@alertas_bp.route('/<int:alerta_id>', methods=['GET'])
@handle_errors
def obter_alerta(alerta_id: int):
    """Obtém detalhes de um alerta específico"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        alertas = service.listar_alertas(usuario_id, apenas_ativos=False)
        
        alerta = next((a for a in alertas if a['id'] == alerta_id), None)
        
        if not alerta:
            return jsonify({'erro': 'Alerta não encontrado'}), 404
        
        return jsonify({
            'sucesso': True,
            'alerta': alerta
        })
    finally:
        db.close()


@alertas_bp.route('/<int:alerta_id>', methods=['PUT'])
@handle_errors
def atualizar_alerta(alerta_id: int):
    """Atualiza um alerta existente"""
    usuario_id = _get_current_user_id()
    data = request.get_json() or {}
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        sucesso = service.atualizar_alerta(alerta_id, usuario_id, data)
        
        if not sucesso:
            return jsonify({'erro': 'Alerta não encontrado ou sem permissão'}), 404
        
        return jsonify({
            'sucesso': True,
            'mensagem': 'Alerta atualizado com sucesso'
        })
    finally:
        db.close()


@alertas_bp.route('/<int:alerta_id>', methods=['DELETE'])
@handle_errors
def deletar_alerta(alerta_id: int):
    """Deleta um alerta"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        sucesso = service.deletar_alerta(alerta_id, usuario_id)
        
        if not sucesso:
            return jsonify({'erro': 'Alerta não encontrado ou sem permissão'}), 404
        
        return jsonify({
            'sucesso': True,
            'mensagem': 'Alerta deletado com sucesso'
        })
    finally:
        db.close()


@alertas_bp.route('/verificar', methods=['POST'])
@handle_errors
def verificar_alertas():
    """Verifica alertas e retorna matches"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = AlertasB2GService(db)
        matches = service.verificar_alertas(usuario_id)
        
        return jsonify({
            'sucesso': True,
            'total_alertas_verificados': len(matches),
            'matches': matches
        })
    finally:
        db.close()


# ==========================================
# ROTAS DE NOTIFICAÇÕES
# ==========================================

@notificacoes_bp.route('/', methods=['GET'])
@handle_errors
def listar_notificacoes():
    """Lista notificações do usuário"""
    usuario_id = _get_current_user_id()
    
    apenas_nao_lidas = request.args.get('apenas_nao_lidas', 'false').lower() == 'true'
    limite = min(int(request.args.get('limite', 50)), 100)
    offset = max(int(request.args.get('offset', 0)), 0)
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        resultado = service.listar_notificacoes(
            usuario_id=usuario_id,
            apenas_nao_lidas=apenas_nao_lidas,
            limite=limite,
            offset=offset
        )
        
        return jsonify({
            'sucesso': True,
            **resultado
        })
    finally:
        db.close()


@notificacoes_bp.route('/', methods=['POST'])
@handle_errors
def criar_notificacao():
    """Cria uma nova notificação"""
    usuario_id = _get_current_user_id()
    data = request.get_json() or {}
    
    tipo = data.get('tipo')
    titulo = data.get('titulo')
    mensagem = data.get('mensagem')
    dados = data.get('dados')
    link = data.get('link')
    
    if not tipo or not titulo or not mensagem:
        return jsonify({'erro': 'tipo, titulo e mensagem são obrigatórios'}), 400
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        notificacao = service.criar_notificacao(
            usuario_id=usuario_id,
            tipo=tipo,
            titulo=titulo,
            mensagem=mensagem,
            dados=dados,
            link=link
        )
        
        if not notificacao:
            return jsonify({'erro': 'Erro ao criar notificação'}), 500
        
        return jsonify({
            'sucesso': True,
            'notificacao': notificacao
        }), 201
    finally:
        db.close()


@notificacoes_bp.route('/<int:notificacao_id>/marcar-lida', methods=['PUT', 'PATCH'])
@handle_errors
def marcar_notificacao_lida(notificacao_id: int):
    """Marca notificação como lida"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        sucesso = service.marcar_como_lida(notificacao_id, usuario_id)
        
        if not sucesso:
            return jsonify({'erro': 'Notificação não encontrada'}), 404
        
        return jsonify({
            'sucesso': True,
            'mensagem': 'Notificação marcada como lida'
        })
    finally:
        db.close()


@notificacoes_bp.route('/marcar-todas-lidas', methods=['PUT', 'PATCH'])
@handle_errors
def marcar_todas_lidas():
    """Marca todas notificações como lidas"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        total = service.marcar_todas_como_lidas(usuario_id)
        
        return jsonify({
            'sucesso': True,
            'total_marcadas': total,
            'mensagem': f'{total} notificações marcadas como lidas'
        })
    finally:
        db.close()


@notificacoes_bp.route('/<int:notificacao_id>', methods=['DELETE'])
@handle_errors
def deletar_notificacao(notificacao_id: int):
    """Deleta uma notificação"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        sucesso = service.deletar_notificacao(notificacao_id, usuario_id)
        
        if not sucesso:
            return jsonify({'erro': 'Notificação não encontrada'}), 404
        
        return jsonify({
            'sucesso': True,
            'mensagem': 'Notificação deletada'
        })
    finally:
        db.close()


@notificacoes_bp.route('/resumo', methods=['GET'])
@handle_errors
def resumo_notificacoes():
    """Retorna resumo de notificações do usuário"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão com banco'}), 500
    
    try:
        service = NotificacoesB2GService(db)
        resultado = service.listar_notificacoes(usuario_id, limite=1)
        
        return jsonify({
            'sucesso': True,
            'total': resultado.get('total', 0),
            'nao_lidas': resultado.get('nao_lidas', 0)
        })
    finally:
        db.close()


# ==========================================
# HEALTH CHECKS
# ==========================================

@alertas_bp.route('/health', methods=['GET'])
def health_check_alertas():
    """Health check do serviço de alertas"""
    return jsonify({
        'sucesso': True,
        'servico': 'Alertas B2G',
        'status': 'ativo'
    })


@notificacoes_bp.route('/health', methods=['GET'])
def health_check_notificacoes():
    """Health check do serviço de notificações"""
    return jsonify({
        'sucesso': True,
        'servico': 'Notificações B2G',
        'status': 'ativo'
    })
