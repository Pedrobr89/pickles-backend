"""
Rotas da API para consultas de CNPJ, palavra-chave e sócios
"""

from flask import Blueprint, request, jsonify, make_response
import logging
from utils.utils_error_handler import handle_errors, ValidationError, NotFoundError
from utils.utils_serializer import serializar_dataframe
from services.services_cnpj_service import consultar_cnpj_simples_enriquecida, buscar_por_palavra_chave, buscar_empresas_por_socio

logger = logging.getLogger(__name__)
consultas_bp = Blueprint('consultas', __name__)

@consultas_bp.route('/cnpj/<string:cnpj>', methods=['GET'])
@handle_errors
def api_consulta_cnpj(cnpj):
    cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
    if len(cnpj_limpo) != 14:
        raise ValidationError("CNPJ deve conter 14 dígitos")
    light = str(request.args.get('light','')).lower() in ['1','true','yes']
    resultado = consultar_cnpj_simples_enriquecida(cnpj_limpo, light=light)
    if not resultado:
        raise NotFoundError(f"CNPJ {cnpj} não encontrado")
    return jsonify(resultado)

@consultas_bp.route('/palavra_chave', methods=['GET'])
@handle_errors
def api_consulta_palavra_chave():
    """
    Busca empresas por palavra-chave (razão social ou nome fantasia)
    """
    termo = request.args.get('termo', '').strip()
    uf = request.args.get('uf')
    municipio = request.args.get('municipio')

    # Valida termo
    if not termo or len(termo) < 4:
        raise ValidationError("O termo de busca deve ter pelo menos 4 caracteres")

    # Valida paginação
    try:
        page = max(int(request.args.get('page', '1')), 1)
        page_size = max(min(int(request.args.get('page_size', '50')), 500), 1)
    except ValueError:
        raise ValidationError("Parâmetros de paginação inválidos")

    logger.info(f"Busca palavra-chave: '{termo}' (UF: {uf}, Mun: {municipio})")

    # Executa busca
    df = buscar_por_palavra_chave(
        termo=termo,
        uf=uf,
        municipio=municipio,
        page=page,
        page_size=page_size
    )

    if df is None or df.empty:
        raise NotFoundError(f"Nenhuma empresa encontrada para o termo '{termo}'")

    return jsonify(serializar_dataframe(df))

@consultas_bp.route('/socio', methods=['GET'])
@handle_errors
def api_consulta_socio():
    """
    Busca empresas por nome de sócio
    """
    nome_socio = request.args.get('nome')
    if not nome_socio:
        raise ValidationError("O parâmetro 'nome' é obrigatório")

    if len(nome_socio.strip()) < 3:
        raise ValidationError("O nome do sócio deve ter pelo menos 3 caracteres")

    logger.info(f"Busca por sócio: '{nome_socio}'")

    # Executa busca
    df = buscar_empresas_por_socio(nome_socio.strip())
    if df is None or df.empty:
        raise NotFoundError(f"Nenhuma empresa encontrada para o sócio '{nome_socio}'")

    return jsonify(serializar_dataframe(df))
 
