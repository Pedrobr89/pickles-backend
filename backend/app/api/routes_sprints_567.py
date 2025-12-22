"""
API Routes Consolidada para Sprints 5, 6 e 7
Parcerias, Cache e Integrações
"""

from flask import Blueprint, jsonify, request
import logging
from services.services_parcerias_b2g import ParceriasB2GService
from services.services_cache_performance import CacheB2GService
from services.services_integracoes_b2g import IntegracoesB2GService
from utils.utils_error_handler import handle_errors
import sqlite3

logger = logging.getLogger(__name__)
parcerias_bp = Blueprint('parcerias', __name__)
cache_bp = Blueprint('cache_api', __name__)
integracoes_bp = Blueprint('integracoes_b2g', __name__)


def _get_db():
    """Helper BD"""
    try:
        return sqlite3.connect('backend/database/users.db')
    except:
        return None


def _get_user_id():
    """TODO: Auth real"""
    return 1


# ==================== PARCERIAS ====================

@parcerias_bp.route('/buscar-parceiros', methods=['POST'])
@handle_errors
def buscar_parceiros():
    """Busca parceiros complementares"""
    data = request.get_json() or {}
    
    empresa_cnpj = data.get('cnpj')
    licitacao = data.get('licitacao', {})
    limite = int(data.get('limite', 10))
    
    if not empresa_cnpj or not licitacao:
        return jsonify({'erro': 'cnpj e licitacao são obrigatórios'}), 400
    
    service = ParceriasB2GService()
    parceiros = service.buscar_parceiros_complementares(empresa_cnpj, licitacao, limite)
    
    return jsonify({
        'sucesso': True,
        'total': len(parceiros),
        'parceiros': parceiros
    })


@parcerias_bp.route('/analisar-consorcio', methods=['POST'])
@handle_errors
def analisar_consorcio():
    """Analisa viabilidade de consórcio"""
    data = request.get_json() or {}
    
    empresas = data.get('empresas', [])
    licitacao = data.get('licitacao', {})
    
    if not empresas or not licitacao:
        return jsonify({'erro': 'empresas e licitacao são obrigatórios'}), 400
    
    service = ParceriasB2GService()
    analise = service.analisar_viabilidade_consorcio(empresas, licitacao)
    
    return jsonify({
        'sucesso': True,
        'analise': analise
    })


# ==================== CACHE ====================

@cache_bp.route('/invalidar', methods=['POST'])
@handle_errors
def invalidar_cache():
    """Invalida cache"""
    data = request.get_json() or {}
    padrao = data.get('padrao')
    
    db = _get_db()
    service = CacheB2GService(db)
    total = service.invalidar_cache(padrao)
    
    if db:
        db.close()
    
    return jsonify({
        'sucesso': True,
        'total_invalidado': total
    })


@cache_bp.route('/limpar-expirado', methods=['POST'])
@handle_errors
def limpar_expirado():
    """Limpa cache expirado"""
    db = _get_db()
    service = CacheB2GService(db)
    total = service.limpar_cache_expirado()
    
    if db:
        db.close()
    
    return jsonify({
        'sucesso': True,
        'total_removido': total
    })


# ==================== INTEGRAÇÕES ====================

@integracoes_bp.route('/webhooks', methods=['POST'])
@handle_errors
def registrar_webhook():
    """Registra webhook"""
    usuario_id = _get_user_id()
    data = request.get_json() or {}
    
    url = data.get('url')
    eventos = data.get('eventos', [])
    secret = data.get('secret')
    
    if not url or not eventos:
        return jsonify({'erro': 'url e eventos são obrigatórios'}), 400
    
    db = _get_db()
    if not db:
        return jsonify({'erro': 'Erro de conexão'}), 500
    
    try:
        service = IntegracoesB2GService(db)
        webhook = service.registrar_webhook(usuario_id, url, eventos, secret)
        
        return jsonify({
            'sucesso': True,
            'webhook': webhook
        }), 201
    finally:
        db.close()


@integracoes_bp.route('/pncp/sincronizar', methods=['POST'])
@handle_errors
def sincronizar_pncp():
    """Sincroniza dados do PNCP"""
    data = request.get_json() or {}
    filtros = data.get('filtros')
    
    service = IntegracoesB2GService()
    resultado = service.sincronizar_pncp_realtime(filtros)
    
    return jsonify({
        'sucesso': resultado.get('sucesso', False),
        **resultado
    })


# ==================== HEALTH CHECKS ====================

@parcerias_bp.route('/health', methods=['GET'])
def health_parcerias():
    return jsonify({'sucesso': True, 'servico': 'Parcerias B2G', 'status': 'ativo'})


@cache_bp.route('/health', methods=['GET'])
def health_cache():
    return jsonify({'sucesso': True, 'servico': 'Cache B2G', 'status': 'ativo'})


@integracoes_bp.route('/health', methods=['GET'])
def health_integracoes():
    return jsonify({'sucesso': True, 'servico': 'Integrações B2G', 'status': 'ativo'})
