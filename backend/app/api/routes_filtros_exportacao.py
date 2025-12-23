"""
API Routes para Filtros Avançados, Mapas e Exportação (Sprint 4)
"""

from flask import Blueprint, jsonify, request, send_file
import logging
from io import BytesIO
from services.services_filtros_avancados import FiltrosAvancadosService
from services.services_exportacao_b2g import ExportacaoB2GService
from utils.utils_error_handler import handle_errors
import sqlite3

logger = logging.getLogger(__name__)
filtros_bp = Blueprint('filtros_avancados', __name__)
mapa_bp = Blueprint('mapa', __name__)
export_bp = Blueprint('exportacao', __name__)


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
    """TODO: Implementar autenticação real"""
    return 1


# ==========================================
# ROTAS DE FILTROS AVANÇADOS
# ==========================================

@filtros_bp.route('/aplicar', methods=['POST'])
@handle_errors
def aplicar_filtros():
    """Aplica filtros avançados em licitações"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    filtros = data.get('filtros', {})
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = FiltrosAvancadosService()
    resultado = service.aplicar_filtros(licitacoes, filtros)
    
    return jsonify({
        'sucesso': True,
        'total_original': len(licitacoes),
        'total_filtrado': len(resultado),
        'licitacoes': resultado
    })


@filtros_bp.route('/salvos', methods=['GET'])
@handle_errors
def listar_filtros_salvos():
    """Lista filtros salvos do usuário"""
    usuario_id = _get_current_user_id()
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão'}), 500
    
    try:
        service = FiltrosAvancadosService(db)
        filtros = service.listar_filtros_salvos(usuario_id)
        
        return jsonify({
            'sucesso': True,
            'total': len(filtros),
            'filtros': filtros
        })
    finally:
        db.close()


@filtros_bp.route('/salvos', methods=['POST'])
@handle_errors
def salvar_filtro():
    """Salva um conjunto de filtros"""
    usuario_id = _get_current_user_id()
    data = request.get_json() or {}
    
    nome = data.get('nome')
    filtros = data.get('filtros', {})
    
    if not nome:
        return jsonify({'erro': 'Nome é obrigatório'}), 400
    
    db = _get_db_connection()
    if not db:
        return jsonify({'erro': 'Erro de conexão'}), 500
    
    try:
        service = FiltrosAvancadosService(db)
        filtro_salvo = service.salvar_filtro(usuario_id, nome, filtros)
        
        if not filtro_salvo:
            return jsonify({'erro': 'Erro ao salvar filtro'}), 500
        
        return jsonify({
            'sucesso': True,
            'filtro': filtro_salvo
        }), 201
    finally:
        db.close()


# ==========================================
# ROTAS DE MAPA
# ==========================================

@mapa_bp.route('/dados', methods=['POST'])
@handle_errors
def gerar_dados_mapa():
    """Gera dados formatados para exibição em mapa"""
    data = request.get_json() or {}
    licitacoes = data.get('licitacoes', [])
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = FiltrosAvancadosService()
    dados_mapa = service.gerar_dados_mapa(licitacoes)
    
    return jsonify({
        'sucesso': True,
        **dados_mapa
    })


@mapa_bp.route('/filtro-geografico', methods=['POST'])
@handle_errors
def filtro_geografico():
    """Aplica filtro geográfico específico"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    config_geo = data.get('geografico', {})
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = FiltrosAvancadosService()
    resultado = service._filtrar_geografico(licitacoes, config_geo)
    
    return jsonify({
        'sucesso': True,
        'total_filtrado': len(resultado),
        'licitacoes': resultado
    })


# ==========================================
# ROTAS DE EXPORTAÇÃO
# ==========================================

@export_bp.route('/excel', methods=['POST'])
@handle_errors
def exportar_excel():
    """Exporta licitações para Excel"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    incluir_match = data.get('incluir_match', True)
    incluir_contexto = data.get('incluir_contexto', False)
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = ExportacaoB2GService()
    excel_bytes = service.exportar_excel(licitacoes, incluir_match, incluir_contexto)
    
    if not excel_bytes:
        return jsonify({'erro': 'Erro ao gerar Excel'}), 500
    
    # Enviar arquivo
    return send_file(
        BytesIO(excel_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'licitacoes_b2g_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )


@export_bp.route('/csv', methods=['POST'])
@handle_errors
def exportar_csv():
    """Exporta licitações para CSV"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    separador = data.get('separador', ';')
    incluir_match = data.get('incluir_match', True)
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = ExportacaoB2GService()
    csv_str = service.exportar_csv(licitacoes, separador, incluir_match)
    
    if not csv_str:
        return jsonify({'erro': 'Erro ao gerar CSV'}), 500
    
    return send_file(
        BytesIO(csv_str.encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'licitacoes_b2g_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    )


@export_bp.route('/pdf', methods=['POST'])
@handle_errors
def exportar_pdf():
    """Exporta licitações para PDF"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    titulo = data.get('titulo', 'Relatório de Licitações B2G')
    incluir_resumo = data.get('incluir_resumo', True)
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = ExportacaoB2GService()
    pdf_bytes = service.exportar_pdf(licitacoes, titulo, incluir_resumo)
    
    if not pdf_bytes:
        return jsonify({'erro': 'Erro ao gerar PDF'}), 500
    
    return send_file(
        BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'relatorio_b2g_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
    )


@export_bp.route('/relatorio-detalhado', methods=['POST'])
@handle_errors
def gerar_relatorio_detalhado():
    """Gera relatório detalhado em JSON"""
    data = request.get_json() or {}
    
    licitacoes = data.get('licitacoes', [])
    empresa_data = data.get('empresa_data', {})
    match_scores = data.get('match_scores', [])
    
    if not licitacoes:
        return jsonify({'erro': 'Lista de licitações é obrigatória'}), 400
    
    service = ExportacaoB2GService()
    relatorio = service.gerar_relatorio_detalhado(licitacoes, empresa_data, match_scores)
    
    return jsonify({
        'sucesso': True,
        'relatorio': relatorio
    })


# ==========================================
# HEALTH CHECKS
# ==========================================

@filtros_bp.route('/health', methods=['GET'])
def health_check_filtros():
    """Health check do serviço de filtros"""
    return jsonify({
        'sucesso': True,
        'servico': 'Filtros Avançados B2G',
        'status': 'ativo'
    })


@mapa_bp.route('/health', methods=['GET'])
def health_check_mapa():
    """Health check do serviço de mapa"""
    return jsonify({
        'sucesso': True,
        'servico': 'Mapa Interativo B2G',
        'status': 'ativo'
    })


@export_bp.route('/health', methods=['GET'])
def health_check_export():
    """Health check do serviço de exportação"""
    return jsonify({
        'sucesso': True,
        'servico': 'Exportação B2G',
        'status': 'ativo'
    })
