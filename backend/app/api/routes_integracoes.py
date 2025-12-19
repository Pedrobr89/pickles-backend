"""
Rotas da API para integrações com autarquias públicas
"""

from flask import Blueprint, request, jsonify
import logging
import asyncio
from utils.utils_error_handler import handle_errors, ValidationError, NotFoundError
from services.compat import get_proxy_config, set_proxy_config, requests_kwargs
from services.services_integracao_service import PNCPIntegration
from services.services_analise_service import get_cnpjs_por_cnae, get_cnpjs_por_uf, get_cnpjs_por_municipio
from services.services_cache_service import cache
import os

logger = logging.getLogger(__name__)
integracoes_bp = Blueprint('integracoes', __name__)

# Instância global do orquestrador
_orquestrador = None

def get_orquestrador():
    """Retorna instância singleton do orquestrador"""
    global _orquestrador
    if _orquestrador is None:
        try:
            from orquestrador_integracoes import OrquestradorIntegracoes
            _orquestrador = OrquestradorIntegracoes()
        except Exception as e:
            raise InternalServerError(f"Orquestrador indisponível: {e}")
    return _orquestrador

@integracoes_bp.route('/analise-360/<string:cnpj>', methods=['GET'])
@handle_errors
def api_analise_360(cnpj):
    """
    Análise 360° de uma empresa com dados de múltiplas fontes
    """
    cnpj_limpo = ''.join(filter(str.isdigit, cnpj))

    if len(cnpj_limpo) != 14:
        raise ValidationError("CNPJ deve conter 14 dígitos")

    incluir_historico = request.args.get('incluir_historico', 'true').lower() == 'true'

    logger.info(f"Análise 360° solicitada para {cnpj_limpo}")

    orquestrador = get_orquestrador()
    resultado = asyncio.run(
        orquestrador.analise_completa_empresa(
            cnpj_limpo,
            incluir_historico=incluir_historico
        )
    )

    if 'erro' in resultado:
        return jsonify(resultado), 502

    return jsonify(resultado)

@integracoes_bp.route('/licitacoes/cnpj/<string:cnpj>', methods=['GET'])
@handle_errors
def api_licitacoes_cnpj(cnpj):
    """
    Consulta dados de licitações para um CNPJ específico
    """
    cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
    if len(cnpj_limpo) != 14:
        raise ValidationError("CNPJ deve conter 14 dígitos")

    cliente = PNCPIntegration()
    perfil = cliente.analisar_perfil_licitacoes(cnpj_limpo)
    return jsonify(perfil)

@integracoes_bp.route('/pncp/feed', methods=['GET'])
@handle_errors
def api_licitacoes_feed():
    pagina = int(request.args.get('pagina', '1'))
    tamanho = int(request.args.get('tamanhoPagina', '50'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'modalidade': request.args.get('modalidade'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_editais(pagina=pagina, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

@integracoes_bp.route('/config/proxy', methods=['GET','POST','DELETE'])
@handle_errors
def config_proxy():
    if request.method == 'GET':
        cfg = get_proxy_config()
        return jsonify({ 'config': cfg })
    elif request.method == 'POST':
        data = request.get_json() or {}
        url = str(data.get('url') or '').strip()
        ignore_ssl = bool(data.get('ignore_ssl', False))
        set_proxy_config(url, ignore_ssl)
        return jsonify({ 'ok': True, 'config': { 'url': url, 'ignore_ssl': ignore_ssl } })
    else:
        cache.delete('proxy_config')
        return jsonify({ 'ok': True })

@integracoes_bp.route('/diagnostico/pncp', methods=['GET'])
@handle_errors
def diagnostico_pncp():
    import requests
    cliente = PNCPIntegration()
    kw = requests_kwargs(timeout=15)
    test_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = { 'pagina': 1, 'tamanhoPagina': 1 }
    try:
        r = requests.get(test_url, params=params, **kw)
        ok = r.ok
        status = r.status_code
        body = r.text[:200]
    except Exception as e:
        return jsonify({ 'ok': False, 'erro': str(e), 'proxies': kw.get('proxies'), 'verify': kw.get('verify') }), 200
    return jsonify({ 'ok': ok, 'status': status, 'proxies': kw.get('proxies'), 'verify': kw.get('verify'), 'preview': body }), 200

@integracoes_bp.route('/config/proxy/auto', methods=['POST'])
@handle_errors
def config_proxy_auto():
    https = os.environ.get('HTTPS_PROXY') or ''
    http = os.environ.get('HTTP_PROXY') or ''
    url = https or http
    if not url:
        return jsonify({ 'ok': False, 'erro': 'Variáveis de ambiente HTTP_PROXY/HTTPS_PROXY não definidas' }), 200
    set_proxy_config(url, True)
    return jsonify({ 'ok': True, 'config': { 'url': url, 'ignore_ssl': True } })

@integracoes_bp.route('/licitacoes/setorial', methods=['POST'])
@handle_errors
def api_licitacoes_setorial():
    """
    Análise de licitações para um setor específico
    """
    dados = request.json or {}
    cnaes = dados.get('cnae_codes') or []
    uf = dados.get('uf')
    municipio = dados.get('municipio')
    limite = int(dados.get('limite', 30))

    if not cnaes and not uf and not municipio:
        raise ValidationError("Informe ao menos 'cnae_codes' ou 'uf'/'municipio'")

    candidatos = set()
    try:
        if cnaes:
            candidatos |= set(get_cnpjs_por_cnae(cnaes))
        if uf:
            candidatos &= set(get_cnpjs_por_uf(uf)) if candidatos else set(get_cnpjs_por_uf(uf))
        if municipio:
            candidatos &= set(get_cnpjs_por_municipio(str(municipio))) if candidatos else set(get_cnpjs_por_municipio(str(municipio)))
    except Exception:
        pass

    lista = sorted(list(candidatos))[:limite]
    cliente = PNCPIntegration()
    resultados = []

    for c in lista:
        try:
            resultados.append(cliente.analisar_perfil_licitacoes(c))
        except Exception:
            continue

    if not resultados:
        raise NotFoundError("Nenhum dado de licitações encontrado para o conjunto")

    # Processa resultados
    import pandas as pd
    df = pd.DataFrame(resultados)

    total_contratos = int(df['total_contratos'].sum()) if 'total_contratos' in df.columns else 0
    valor_total = float(df['valor_total'].sum()) if 'valor_total' in df.columns else 0.0
    score_medio = float(df['score_experiencia'].apply(lambda s: s.get('score',0)).mean()) if 'score_experiencia' in df.columns else 0.0

    anos = sorted(set([a for xs in df['anos_atuacao'].dropna().tolist() for a in (xs or [])])) if 'anos_atuacao' in df.columns else []

    orgaos = []
    try:
        from collections import Counter
        cont = Counter()
        for xs in df['orgaos_contratantes'].dropna().tolist():
            for o in xs or []:
                cont[o] += 1
        orgaos = [k for k,_ in cont.most_common(10)]
    except Exception:
        orgaos = []

    resumo = {
        'total_empresas_avaliadas': len(df),
        'total_contratos': total_contratos,
        'valor_total_contratos': valor_total,
        'score_experiencia_medio': score_medio,
        'anos_atuacao_conjunto': anos,
        'orgaos_mais_frequentes': orgaos,
        'amostra_cnpjs': lista
    }

    return jsonify(resumo)
