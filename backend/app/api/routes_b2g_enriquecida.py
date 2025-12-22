"""
Novas rotas da API para Sprint 2 - Dados Contextuais e Relatórios IA
"""

from flask import Blueprint, jsonify, request
import logging
from services.services_dados_contextuais import DadosContextuaisService
from services.services_relatorio_ia import RelatorioIAService
from utils.utils_error_handler import handle_errors

logger = logging.getLogger(__name__)
b2g_enriquecida_bp = Blueprint('b2g_enriquecida', __name__)


@b2g_enriquecida_bp.route('/licitacao/enriquecer', methods=['POST'])
@handle_errors
def rota_enriquecer_licitacao():
    """
    Enriquece uma licita\u00e7\u00e3o com dados contextuais
    
    Body:
    {
        "licitacao": {...}
    }
    
    Returns:
        Licitação enriquecida com histórico do órgão, análise de concorrência, etc.
    """
    data = request.get_json() or {}
    licitacao = data.get('licitacao')
    
    if not licitacao:
        return jsonify({'erro': 'licitacao é obrigatória'}), 400
    
    service = DadosContextuaisService()
    licitacao_enriquecida = service.enriquecer_licitacao(licitacao)
    
    return jsonify({
        'sucesso': True,
        'licitacao_enriquecida': licitacao_enriquecida
    })


@b2g_enriquecida_bp.route('/relatorio/executivo', methods=['POST'])
@handle_errors
def rota_relatorio_executivo():
    """
    Gera relatório executivo com IA para análise de licitações
    
    Body:
    {
        "empresa_data": {...},
        "licitacoes": [...],
        "match_scores": [...]
    }
    
    Returns:
        Relatório executivo completo com insights e recomendações
    """
    data = request.get_json() or {}
    
    empresa_data = data.get('empresa_data', {})
    licitacoes = data.get('licitacoes', [])
    match_scores = data.get('match_scores', [])
    
    if not empresa_data:
        return jsonify({'erro': 'empresa_data é obrigatória'}), 400
    
    service = RelatorioIAService()
    relatorio = service.gerar_relatorio_executivo(empresa_data, licitacoes, match_scores)
    
    return jsonify({
        'sucesso': True,
        'relatorio': relatorio
    })


@b2g_enriquecida_bp.route('/licitacao/sugestoes', methods=['POST'])
@handle_errors
def rota_sugestoes_acao():
    """
    Gera sugestões de ação baseadas em IA para uma licitação
    
    Body:
    {
        "licitacao": {...},
        "match_data": {...},
        "empresa_data": {...}
    }
    
   

 Returns:
        Lista de sugestões priorizadas
    """
    data = request.get_json() or {}
    
    licitacao = data.get('licitacao', {})
    match_data = data.get('match_data', {})
    empresa_data = data.get('empresa_data', {})
    
    if not licitacao:
        return jsonify({'erro': 'licitacao é obrigatória'}), 400
    
    service = RelatorioIAService()
    sugestoes = service.gerar_sugestoes_acao(licitacao, match_data, empresa_data)
    
    return jsonify({
        'sucesso': True,
        'sugestoes': sugestoes
    })


@b2g_enriquecida_bp.route('/licitacoes/enriquecer-batch', methods=['POST'])
@handle_errors
def rota_enriquecer_licitacoes_batch():
    """
    Enriquece múltiplas licitações em lote
    
    Body:
    {
        "licitacoes": [...]
    }
    
    Returns:
        Array com licitações enriquecidas
    """
    data = request.get_json() or {}
    licitacoes = data.get('licitacoes', [])
    
    if not licitacoes:
        return jsonify({'erro': 'licitacoes é obrigatória'}), 400
    
    service = DadosContextuaisService()
    licitacoes_enriquecidas = []
    
    for lic in licitacoes[:100]:  # Limitar a 100 por request
        try:
            enriquecida = service.enriquecer_licitacao(lic)
            licitacoes_enriquecidas.append(enriquecida)
        except Exception as e:
            logger.error(f"Erro ao enriquecer licitação: {e}")
            licitacoes_enriquecidas.append(lic)  # Retorna original em caso de erro
    
    return jsonify({
        'sucesso': True,
        'total': len(licitacoes_enriquecidas),
        'licitacoes': licitacoes_enriquecidas
    })


@b2g_enriquecida_bp.route('/historico/orgao/<string:orgao_nome>', methods=['GET'])
@handle_errors
def rota_historico_orgao(orgao_nome: str):
    """
    Obtém histórico detalhado de um órgão
    
    Args:
        orgao_nome: Nome do órgão
        
    Returns:
        Histórico completo do órgão
    """
    service = DadosContextuais Service()
    historico = service._obter_historico_orgao(orgao_nome)
    
    return jsonify({
        'sucesso': True,
        'orgao': orgao_nome,
        'historico': historico
    })


@b2g_enriquecida_bp.route('/checklist/documentos/<string:modalidade>', methods=['GET'])
@handle_errors
def rota_checklist_documentos(modalidade: str):
    """
    Obtém checklist de documentos necessários para uma modalidade
    
    Args:
        modalidade: Modalidade da licitação
        
    Returns:
        Lista de documentos necessários
    """
    service = DadosContextuaisService()
    checklist = service._gerar_checklist_documentos(modalidade)
    
    return jsonify({
        'sucesso': True,
        'modalidade': modalidade,
        'documentos': checklist
    })


@b2g_enriquecida_bp.route('/health', methods=['GET'])
def rota_health_check():
    """Health check do serviço"""
    return jsonify({
        'sucesso': True,
        'servico': 'B2G Enriquecimento Sprint 2',
        'status': 'ativo'
    })
