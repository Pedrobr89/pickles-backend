"""
Rotas da API para análises setoriais
"""

from flask import Blueprint, jsonify, request, make_response
from dataclasses import asdict
import logging
import os
import requests
import duckdb
from pyarrow import parquet as pq
from core.config import Config
from services.services_cnpj_service import buscar_por_palavra_chave
from services.services_cnpj_service import consultar_cnpj_completo, verificar_divida_pgfn
import numpy as np
import time
import pandas as pd
from pathlib import Path
from utils.utils_error_handler import handle_errors, ValidationError, NotFoundError
from utils.utils_serializer import serializar_dataframe, serializar_dados_graficos
from services.services_analise_service import executar_analise_setorial, sugerir_cnaes, scoring_compatibilidade, scoring_ranking, ranking_empresas_por_prestador

from flask import jsonify
from services.services_analise_service import analisar_licitacoes_por_cnpj
from services.services_integracao_service import PNCPIntegration
from services.services_cache_service import cache
try:
    from server import limiter as _limiter
except Exception:
    _limiter = None
def _limit(rule: str):
    def wrapper(fn):
        return fn if _limiter is None else _limiter.limit(rule)(fn)
    return wrapper
from fpdf import FPDF

logger = logging.getLogger(__name__)
analises_bp = Blueprint('analises', __name__)
scoring_bp = Blueprint('scoring', __name__)

def _gerar_relatorio_ai(contexto: str, fallback_text: str) -> str:
    api_key = os.environ.get('AI_API_KEY')
    base_url = os.environ.get('AI_API_BASE', 'https://api.openai.com/v1')
    model = os.environ.get('AI_MODEL', 'gpt-4o-mini')
    if not api_key:
        return fallback_text
    try:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Você é um analista de mercado B2B. Produza um relatório executivo conciso em português com insights acionáveis e próximos passos."},
                {"role": "user", "content": contexto}
            ],
            "temperature": 0.2
        }
        resp = requests.post(f"{base_url}/chat/completions", json=payload, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, timeout=20)
        if resp.ok:
            data = resp.json()
            txt = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content") or ""
            return txt.strip() or fallback_text
        return fallback_text
    except Exception:
        return fallback_text

def _chat_ai(messages: list, fallback_text: str) -> str:
    api_key = os.environ.get('AI_API_KEY')
    base_url = os.environ.get('AI_API_BASE', 'https://api.openai.com/v1')
    model = os.environ.get('AI_MODEL', 'gpt-4o-mini')
    if not api_key:
        return fallback_text
    try:
        payload = {
            "model": model,
            "messages": messages,
            "temperature": 0.2
        }
        resp = requests.post(f"{base_url}/chat/completions", json=payload, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, timeout=20)
        if resp.ok:
            data = resp.json()
            txt = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content") or ""
            return txt.strip() or fallback_text
        return fallback_text
    except Exception:
        return fallback_text

def _embed_text(txt: str) -> np.ndarray:
    api_key = os.environ.get('AI_API_KEY')
    base_url = os.environ.get('AI_API_BASE', 'https://api.openai.com/v1')
    model = os.environ.get('AI_EMBED_MODEL', 'text-embedding-3-large')
    s = str(txt or '').strip()
    if not s:
        return np.zeros((256,), dtype=np.float32)
    if not api_key:
        dim = 256
        v = np.zeros((dim,), dtype=np.float32)
        for i, ch in enumerate(s.encode('utf-8')):
            v[i % dim] += float(ch)
        n = np.linalg.norm(v) or 1.0
        return (v / n).astype(np.float32)
    try:
        payload = {"input": s, "model": model}
        resp = requests.post(f"{base_url}/embeddings", json=payload, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, timeout=20)
        if not resp.ok:
            dim = 256
            v = np.zeros((dim,), dtype=np.float32)
            for i, ch in enumerate(s.encode('utf-8')):
                v[i % dim] += float(ch)
            n = np.linalg.norm(v) or 1.0
            return (v / n).astype(np.float32)
        data = resp.json()
        vec = np.array(data.get('data', [{}])[0].get('embedding') or [], dtype=np.float32)
        n = np.linalg.norm(vec) or 1.0
        return (vec / n).astype(np.float32)
    except Exception:
        dim = 256
        v = np.zeros((dim,), dtype=np.float32)
        for i, ch in enumerate(s.encode('utf-8')):
            v[i % dim] += float(ch)
        n = np.linalg.norm(v) or 1.0
        return (v / n).astype(np.float32)

def _rag_search(query: str, top_k: int = 5) -> list:
    idx = cache.get('semantic_index') or {}
    items = idx.get('items') or []
    if not items:
        return []
    q = _embed_text(query)
    scores = []
    for it in items:
        vb = it.get('vector') or b''
        dim = int(it.get('dim') or 0) or q.shape[0]
        v = np.frombuffer(vb, dtype=np.float32)
        if v.shape[0] != dim:
            continue
        s = float(np.dot(q[:dim], v[:dim]))
        scores.append((s, it))
    scores.sort(key=lambda x: x[0], reverse=True)
    return [dict(id=it.get('id'), text=it.get('text'), meta=it.get('meta'), score=round(s, 4)) for s, it in scores[:max(1, top_k)]]

def _index_append_items(new_items: list):
    try:
        idx = cache.get('semantic_index') or {}
        items = (idx.get('items') or []) + (new_items or [])
        cache.set('semantic_index', { 'items': items, 'built_at': int(time.time()) }, expire=3600)
        return len(items)
    except Exception:
        cache.set('semantic_index', { 'items': new_items or [], 'built_at': int(time.time()) }, expire=3600)
        return len(new_items or [])

def _schema_tables() -> dict:
    out = {}
    for nome, caminho in Config.ARQUIVOS_PARQUET.items():
        try:
            pf = pq.ParquetFile(str(caminho))
            cols = [c.name for c in pf.schema]
        except Exception:
            cols = []
        out[nome] = { 'path': str(caminho), 'columns': cols }
    return out

def _vector_backend() -> str:
    return str(os.environ.get('VECTOR_DB_BACKEND') or 'cache').strip().lower()

def _qdrant_upsert(items: list) -> bool:
    try:
        base = os.environ.get('QDRANT_URL')
        coll = os.environ.get('QDRANT_COLLECTION', 'semantic_index')
        if not base:
            return False
        points = []
        for it in items:
            v = np.frombuffer(it.get('vector') or b'', dtype=np.float32).tolist()
            points.append({
                'id': it.get('id'),
                'vector': v,
                'payload': { 'text': it.get('text'), 'meta': it.get('meta') }
            })
        payload = { 'points': points }
        url = base.rstrip('/') + f"/collections/{coll}/points?wait=true"
        resp = requests.put(url, json=payload, timeout=20)
        return bool(resp.ok)
    except Exception:
        return False

def _qdrant_search(query: str, top_k: int = 5) -> list:
    try:
        base = os.environ.get('QDRANT_URL')
        coll = os.environ.get('QDRANT_COLLECTION', 'semantic_index')
        if not base:
            return []
        qv = _embed_text(query).astype(np.float32).tolist()
        payload = { 'vector': qv, 'limit': max(1, int(top_k)), 'with_payload': True }
        url = base.rstrip('/') + f"/collections/{coll}/points/search"
        resp = requests.post(url, json=payload, timeout=20)
        if not resp.ok:
            return []
        data = resp.json()
        hits = data.get('result') or []
        out = []
        for h in hits:
            pl = h.get('payload') or {}
            out.append({ 'id': h.get('id'), 'text': (pl.get('text') or ''), 'meta': pl.get('meta') or {}, 'score': float(h.get('score') or 0.0) })
        return out
    except Exception:
        return []

def _vector_upsert(items: list, mode: str = 'append') -> dict:
    be = _vector_backend()
    if be == 'qdrant':
        ok = _qdrant_upsert(items)
        if ok:
            return { 'ok': True, 'backend': 'qdrant', 'total_indexed': len(items) }
    # cache backend (default/fallback)
    if mode == 'append':
        total = _index_append_items(items)
        return { 'ok': True, 'backend': 'cache', 'mode': 'append', 'total_indexed': len(items), 'total_items': total }
    else:
        idx = { 'items': items, 'built_at': int(time.time()) }
        cache.set('semantic_index', idx, expire=3600)
        return { 'ok': True, 'backend': 'cache', 'mode': 'replace', 'total_indexed': len(items) }

def _vector_search(query: str, top_k: int = 5) -> list:
    be = _vector_backend()
    if be == 'qdrant':
        res = _qdrant_search(query, top_k=top_k)
        if res:
            return res
    # cache backend (default/fallback)
    return _rag_search(query, top_k=top_k)

@analises_bp.route('/licitacoes/<string:cnpj>', methods=['GET'])
@handle_errors
def rota_licitacoes_por_cnpj(cnpj):
    resultado = analisar_licitacoes_por_cnpj(cnpj)
    return jsonify(resultado)

@analises_bp.route('/pncp/editais', methods=['GET'])
@handle_errors
def rota_pncp_editais():
    from flask import request
    pagina = int(request.args.get('pagina','1'))
    tamanho = int(request.args.get('tamanhoPagina','10'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'modalidade': request.args.get('modalidade'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao'),
        'dataInicial': request.args.get('dataInicial'),
        'dataFinal': request.args.get('dataFinal')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_editais(pagina=pagina, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

@analises_bp.route('/pncp/itens', methods=['GET'])
@handle_errors
def rota_pncp_itens():
    from flask import request
    pagina = int(request.args.get('pagina','1'))
    tamanho = int(request.args.get('tamanhoPagina','10'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'modalidade': request.args.get('modalidade'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao'),
        'dataInicial': request.args.get('dataInicial'),
        'dataFinal': request.args.get('dataFinal'),
        'uf': request.args.get('uf') or request.args.get('ufSigla'),
        'municipioNome': request.args.get('municipioNome'),
        'codigoMunicipioIbge': request.args.get('codigoMunicipioIbge'),
        'cnpj': request.args.get('cnpj'),
        'codigoUnidadeAdministrativa': request.args.get('codigoUnidadeAdministrativa'),
        'idUsuario': request.args.get('idUsuario')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_itens(pagina=pagina, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

@analises_bp.route('/pncp/contratos', methods=['GET'])
@handle_errors
def rota_pncp_contratos():
    from flask import request
    pagina = int(request.args.get('pagina','1'))
    tamanho = int(request.args.get('tamanhoPagina','10'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'modalidade': request.args.get('modalidade'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao'),
        'dataInicial': request.args.get('dataInicial'),
        'dataFinal': request.args.get('dataFinal'),
        'uf': request.args.get('uf') or request.args.get('ufSigla'),
        'municipioNome': request.args.get('municipioNome'),
        'codigoMunicipioIbge': request.args.get('codigoMunicipioIbge'),
        'cnpj': request.args.get('cnpj'),
        'codigoUnidadeAdministrativa': request.args.get('codigoUnidadeAdministrativa'),
        'idUsuario': request.args.get('idUsuario')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_contratos(pagina=pagina, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

@analises_bp.route('/pncp/editais/todos', methods=['GET'])
@handle_errors
def rota_pncp_editais_todos():
    from flask import request
    dias = int(request.args.get('dias','30'))
    tamanho = int(request.args.get('tamanhoPagina','200'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_editais_todos(dias=dias, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

@analises_bp.route('/pncp/serie', methods=['GET'])
@handle_errors
def rota_pncp_serie():
    from flask import request
    dias = request.args.get('dias')
    tamanho = int(request.args.get('tamanhoPagina','200'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao'),
        'dataInicial': request.args.get('dataInicial'),
        'dataFinal': request.args.get('dataFinal'),
        'uf': request.args.get('uf') or request.args.get('ufSigla'),
        'municipioNome': request.args.get('municipioNome'),
        'codigoMunicipioIbge': request.args.get('codigoMunicipioIbge'),
        'cnpj': request.args.get('cnpj'),
        'codigoUnidadeAdministrativa': request.args.get('codigoUnidadeAdministrativa'),
        'idUsuario': request.args.get('idUsuario')
    }
    cliente = PNCPIntegration()
    if dias and dias.isdigit():
        base = cliente.listar_editais_todos(dias=int(dias), tamanho=tamanho, filtros=filtros)
        itens = base.get('data') or []
    else:
        base = cliente.listar_editais_todos(dias=30, tamanho=tamanho, filtros=filtros)
        itens = base.get('data') or []
    def _d(x):
        s = str(x or '')
        if not s:
            return ''
        import re
        m = re.match(r"(\d{4})-?(\d{2})-?(\d{2})", s)
        if m:
            return f"{m.group(1)}{m.group(2)}{m.group(3)}"
        return ''
    def _v(x):
        try:
            return float(x or 0)
        except Exception:
            return 0.0
    by_day = {}
    by_modal = {}
    by_uf = {}
    total_v = 0.0
    for it in itens:
        d = _d(it.get('dataPublicacaoPncp') or it.get('dataPublicacao'))
        if not d:
            continue
        by_day.setdefault(d, {'date': d, 'count': 0, 'valor': 0.0})
        by_day[d]['count'] += 1
        val = _v(it.get('valorTotalEstimado') or it.get('valorTotal'))
        by_day[d]['valor'] += val
        total_v += val
        mod = str(it.get('codigoModalidadeContratacao') or it.get('modalidade') or '')
        uf = str(it.get('uf') or it.get('ufSigla') or '')
        if mod:
            by_modal[mod] = by_modal.get(mod, 0) + 1
        if uf:
            by_uf[uf] = by_uf.get(uf, 0) + 1
    series = sorted(by_day.values(), key=lambda x: x['date'])
    ma7 = []
    w = []
    for s in series:
        w.append(s['count'])
        if len(w) > 7:
            w.pop(0)
        ma7.append(sum(w) / len(w))
    top_modal = sorted([{ 'label': k, 'count': v } for k, v in by_modal.items()], key=lambda x: x['count'], reverse=True)[:10]
    top_uf = sorted([{ 'label': k, 'count': v } for k, v in by_uf.items()], key=lambda x: x['count'], reverse=True)[:10]
    out = {
        'total': len(itens),
        'totalValor': total_v,
        'series': series,
        'mediaMovel7': ma7,
        'topModalidades': top_modal,
        'topUFs': top_uf
    }
    return jsonify(out)

@analises_bp.route('/pncp/raw/<string:tipo>', methods=['GET'])
@handle_errors
def rota_pncp_raw(tipo: str):
    pagina = int(request.args.get('pagina','1'))
    tamanho = int(request.args.get('tamanhoPagina','10'))
    filtros = {
        'situacao': request.args.get('situacao'),
        'modalidade': request.args.get('modalidade'),
        'orgao': request.args.get('orgao'),
        'unidadeGestora': request.args.get('unidadeGestora'),
        'palavraChave': request.args.get('palavraChave'),
        'codigoModalidadeContratacao': request.args.get('codigoModalidadeContratacao'),
        'dataInicial': request.args.get('dataInicial'),
        'dataFinal': request.args.get('dataFinal'),
        'uf': request.args.get('uf') or request.args.get('ufSigla'),
        'municipioNome': request.args.get('municipioNome'),
        'codigoMunicipioIbge': request.args.get('codigoMunicipioIbge'),
        'cnpj': request.args.get('cnpj'),
        'codigoUnidadeAdministrativa': request.args.get('codigoUnidadeAdministrativa'),
        'idUsuario': request.args.get('idUsuario')
    }
    cliente = PNCPIntegration()
    resultado = cliente.listar_raw(tipo, pagina=pagina, tamanho=tamanho, filtros=filtros)
    return jsonify(resultado)

# --- KPIs Globais da Base ---
@analises_bp.route('/kpis/base', methods=['GET'])
@handle_errors
def rota_kpis_base():
    """
    KPIs globais da base de dados (RFB) lendo diretamente os arquivos Parquet.
    Retorna contagens e tops por UF e CNAE, além do status dos arquivos.
    """
    import pyarrow.parquet as pq
    from pathlib import Path

    def _num_rows(fp: Path) -> int:
        try:
            pf = pq.ParquetFile(str(fp))
            return sum(r.num_rows for r in pf.metadata.row_group_metadata) if pf.metadata else 0
        except Exception:
            try:
                pf = pq.ParquetFile(str(fp))
                return int(pf.metadata.num_rows or 0)
            except Exception:
                return 0

    def _size_mb(fp: Path) -> float:
        try:
            return round(fp.stat().st_size / (1024 * 1024), 2)
        except Exception:
            return 0.0

    # Status de arquivos principais
    arquivos = {
        nome: {
            'path': str(caminho),
            'existe': Path(caminho).exists(),
            'rows': _num_rows(caminho) if Path(caminho).exists() else 0,
            'size_mb': _size_mb(caminho) if Path(caminho).exists() else 0.0
        }
        for nome, caminho in Config.ARQUIVOS_PARQUET.items()
        if nome in ['empresas','estabelecimentos','socios']
    }

    total_empresas = (arquivos.get('empresas') or {}).get('rows', 0)
    total_estabelecimentos = (arquivos.get('estabelecimentos') or {}).get('rows', 0)
    total_socios = (arquivos.get('socios') or {}).get('rows', 0)

    # Tops usando DuckDB para performance
    top_uf = []
    top_cnae = []
    top_porte = []
    situacoes = []
    
    try:
        est_path = str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')
        emp_path = str(Config.ARQUIVOS_PARQUET['empresas']).replace('\\','/')
        
        # UF
        q1 = (
            f"SELECT uf as label, COUNT(*) as count FROM read_parquet('{est_path}') "
            f"WHERE uf IS NOT NULL GROUP BY uf ORDER BY count DESC LIMIT 10"
        )
        df_uf = duckdb.sql(q1).to_df()
        top_uf = [{'label': str(r['label']), 'count': int(r['count'])} for _, r in df_uf.iterrows()]
        
        # CNAE principal com descrição
        try:
            # Carregar tabela de CNAEs para obter descrições
            cnae_path = str(Config.ARQUIVOS_PARQUET.get('cnaes', '')).replace('\\','/')
            if Path(cnae_path).exists():
                q2 = (
                    f"SELECT e.cnae_fiscal_principal AS cnae, c.descricao, COUNT(*) as count "
                    f"FROM read_parquet('{est_path}') e "
                    f"LEFT JOIN read_parquet('{cnae_path}') c ON e.cnae_fiscal_principal = c.codigo "
                    f"WHERE e.cnae_fiscal_principal IS NOT NULL "
                    f"GROUP BY e.cnae_fiscal_principal, c.descricao "
                    f"ORDER BY count DESC LIMIT 15"
                )
            else:
                q2 = (
                    f"SELECT coalesce(cnae_fiscal_principal, cnae_fiscal) AS cnae, COUNT(*) as count "
                    f"FROM read_parquet('{est_path}') "
                    f"WHERE cnae_fiscal_principal IS NOT NULL "
                    f"GROUP BY cnae ORDER BY count DESC LIMIT 15"
                )
            df_cnae = duckdb.sql(q2).to_df()
            top_cnae = [
                {
                    'label': f"{str(r.get('descricao', ''))[:40] if 'descricao' in r and r['descricao'] else str(r['cnae']).zfill(7)}", 
                    'cnae': str(r['cnae']).zfill(7),
                    'count': int(r['count'])
                } 
                for _, r in df_cnae.iterrows()
            ]
        except Exception as e:
            logger.warning(f"Erro ao buscar CNAEs: {e}")
            top_cnae = []
        
        # Situação cadastral
        try:
            q3 = (
                f"SELECT situacao_cadastral as sit, COUNT(*) as count FROM read_parquet('{est_path}') "
                f"GROUP BY situacao_cadastral ORDER BY count DESC"
            )
            df_sit = duckdb.sql(q3).to_df()
            def _nome_sit(code):
                m = {'01':'Nula','1':'Nula','02':'Ativa','2':'Ativa','03':'Suspensa','3':'Suspensa','04':'Inapta','4':'Inapta','05':'Baixada','5':'Baixada','08':'Baixada','8':'Baixada'}
                return m.get(str(code), str(code))
            situacoes = [{'label': _nome_sit(r['sit']), 'count': int(r['count'])} for _, r in df_sit.iterrows()]
        except Exception as e:
            logger.warning(f"Erro ao buscar situações: {e}")
            situacoes = []
        
        # Porte de empresa
        try:
            if Path(emp_path).exists():
                q4 = (
                    f"SELECT porte_da_empresa as porte, COUNT(*) as count "
                    f"FROM read_parquet('{emp_path}') "
                    f"WHERE porte_da_empresa IS NOT NULL "
                    f"GROUP BY porte_da_empresa ORDER BY count DESC"
                )
                df_porte = duckdb.sql(q4).to_df()
                def _nome_porte(code):
                    m = {
                        '00': 'Não Informado',
                        '01': 'Microempresa',
                        '02': 'Pequena',
                        '03': 'Média',
                        '04': 'Grande',
                        '05': 'Demais',
                        '1': 'Microempresa',
                        '2': 'Pequena',
                        '3': 'Média',
                        '4': 'Grande',
                        '5': 'Demais'
                    }
                    return m.get(str(code), f'Porte {code}')
                top_porte = [{'label': _nome_porte(r['porte']), 'count': int(r['count'])} for _, r in df_porte.iterrows()]
        except Exception as e:
            logger.warning(f"Erro ao buscar portes: {e}")
            top_porte = []

        # Séries Temporais (Entradas e Saídas)
        cards = {}
        try:
            # Entradas (últimos 24 meses aprox, filtro > 2023)
            q_ent = (
                f"SELECT SUBSTR(data_de_inicio_atividade, 1, 6) as mes, COUNT(*) as count "
                f"FROM read_parquet('{est_path}') "
                f"WHERE data_de_inicio_atividade >= '20230101' AND data_de_inicio_atividade IS NOT NULL "
                f"GROUP BY 1 ORDER BY 1"
            )
            df_ent = duckdb.sql(q_ent).to_df()
            entradas_mensais = [{'label': f"{str(r['mes'])[:4]}-{str(r['mes'])[4:]}", 'count': int(r['count'])} for _, r in df_ent.iterrows()]
            
            # Saídas (Baixadas = '08', '8')
            q_sai = (
                f"SELECT SUBSTR(data_situacao_cadastral, 1, 6) as mes, COUNT(*) as count "
                f"FROM read_parquet('{est_path}') "
                f"WHERE situacao_cadastral IN ('08', '8') "
                f"AND data_situacao_cadastral >= '20230101' AND data_situacao_cadastral IS NOT NULL "
                f"GROUP BY 1 ORDER BY 1"
            )
            df_sai = duckdb.sql(q_sai).to_df()
            saidas_mensais = [{'label': f"{str(r['mes'])[:4]}-{str(r['mes'])[4:]}", 'count': int(r['count'])} for _, r in df_sai.iterrows()]
            
            cards['entradas_mensais'] = entradas_mensais
            cards['saidas_mensais'] = saidas_mensais
            
            # Totais recentes para cards
            if entradas_mensais:
                cards['entradas_mes_vigente'] = entradas_mensais[-1]['count']
                cards['entradas_mes_vigente_label'] = entradas_mensais[-1]['label']
            if len(entradas_mensais) > 1:
                cards['entradas_mes_anterior'] = entradas_mensais[-2]['count']
                cards['entradas_mes_anterior_label'] = entradas_mensais[-2]['label']
            
            # Calcular total ativas no backend para facilitar
            total_ativas_calc = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{est_path}') WHERE situacao_cadastral IN ('02','2')").fetchone()[0]
            cards['total_ativas'] = int(total_ativas_calc)
                
        except Exception as e:
            logger.warning(f"Erro ao buscar series: {e}")
            cards['entradas_mensais'] = []
            cards['saidas_mensais'] = []

    except Exception as e:
        logger.error(f"Erro geral em kpis/base: {e}")
        top_uf = []
        top_cnae = []
        top_porte = []
        situacoes = []
        cards = {}

    out = {
        'timestamp': int(time.time()),
        'arquivos': arquivos,
        'resumo': {
            'total_empresas': int(total_empresas),
            'total_estabelecimentos': int(total_estabelecimentos),
            'total_socios': int(total_socios)
        },
        'top': {
            'uf': top_uf,
            'cnae_fiscal': top_cnae,
            'cnae_principal': top_cnae,  # Alias para compatibilidade
            'situacao_cadastral': situacoes,
            'porte_da_empresa': top_porte,
            'porte': top_porte  # Alias para compatibilidade
        },
        'cards': cards
    }
    return jsonify(out)

@analises_bp.route('/scoring/compatibilidade', methods=['POST'])
def rota_scoring_compatibilidade():
    data = request.get_json() or {}
    cnpj = data.get('cnpj') or ''
    edital = data.get('edital_data') or {}
    resultado = scoring_compatibilidade(cnpj, edital)
    return jsonify(resultado)

@analises_bp.route('/scoring/ranking', methods=['POST'])
def rota_scoring_ranking():
    data = request.get_json() or {}
    cnpj = data.get('cnpj') or ''
    filtros = data.get('filtros') or {}
    limite = int(data.get('limite') or 50)
    resultado = scoring_ranking(cnpj, filtros, limite)
    return jsonify(resultado)

@analises_bp.route('/compat/empresas', methods=['POST'])
def rota_compat_empresas():
    data = request.get_json() or {}
    cnpj = data.get('cnpj_prestador') or data.get('cnpj') or ''
    filtros = data.get('filtros') or data
    limite = int(data.get('limite') or 50)
    resultado = ranking_empresas_por_prestador(cnpj, filtros, limite)
    try:
        itens = resultado.get('resultados') or []
        top = []
        for it in itens:
            emp = it.get('empresa') or {}
            top.append({
                'cnpj': emp.get('cnpj'),
                'razao_social': emp.get('razao_social') or emp.get('razao_social_nome_empresarial'),
                'uf': emp.get('uf'),
                'municipio': emp.get('municipio'),
                'score_total': it.get('compatibilidade') or it.get('score_base') or 0
            })
        resultado['top_empresas'] = top
    except Exception:
        resultado['top_empresas'] = []
    try:
        dash = resultado.get('dashboard') or {}
        resumo_kpis = f"Total: {dash.get('total_leads', 0)} | Média: {dash.get('media_score', 0)} | Máx: {dash.get('max_score', 0)} | Top UF: {dash.get('top_uf', '—')} | Potenciais (A/M/B): {dash.get('potencial_alto', 0)}/{dash.get('potencial_medio', 0)}/{dash.get('potencial_baixo', 0)}"
        amostra = "; ".join([f"{(t.get('razao_social') or '—')[:40]} ({t.get('uf') or '—'}) - Score {int(t.get('score_total') or 0)}" for t in (resultado.get('top_empresas') or [])[:10]])
        contexto = f"KPIs de Leads: {resumo_kpis}. Amostra de empresas: {amostra}. Objetivo: criar um relatório executivo com insights, oportunidades por UF e próximos passos de prospecção.".strip()
        resultado['relatorio_executivo'] = _gerar_relatorio_ai(contexto, resultado.get('relatorio_executivo') or '')
    except Exception:
        pass
    return jsonify(resultado)

@scoring_bp.route('/compatibilidade', methods=['POST'])
def scoring_compatibilidade_root():
    data = request.get_json() or {}
    cnpj = data.get('cnpj') or ''
    edital = data.get('edital_data') or {}
    resultado = scoring_compatibilidade(cnpj, edital)
    return jsonify(resultado)

@scoring_bp.route('/ranking', methods=['POST'])
def scoring_ranking_root():
    data = request.get_json() or {}
    cnpj = data.get('cnpj') or ''
    filtros = data.get('filtros') or {}
    limite = int(data.get('limite') or 50)
    resultado = scoring_ranking(cnpj, filtros, limite)
    return jsonify(resultado)

@analises_bp.route('/scoring/batch', methods=['POST'])
def rota_scoring_batch():
    data = request.get_json() or {}
    cnpj = data.get('cnpj') or ''
    editais = data.get('editais') or []
    info = scoring_compatibilidade(cnpj, {})  # garante formato
    # Reusa motor para cada edital
    from services.services_analise_service import ScoringEngine, _map_cnpj_enriquecido_to_scoring, consultar_cnpj_simples_enriquecida
    cnpj_info = consultar_cnpj_simples_enriquecida(cnpj)
    if not cnpj_info:
        return jsonify({'erro':'CNPJ não encontrado'}), 404
    cnpj_data = _map_cnpj_enriquecido_to_scoring(cnpj_info)
    engine = ScoringEngine()
    resultados = []
    for ed in editais:
        try:
            r = engine.calcular_score(cnpj_data, ed)
            resultados.append(getattr(r, '__dict__', r))
        except Exception as e:
            resultados.append({'edital_id': ed.get('numeroControlePNCP'), 'error': str(e)})
    resultados_validos = [r for r in resultados if 'error' not in r]
    resultados_validos.sort(key=lambda x: x['score_total'], reverse=True)
    return jsonify({ 'cnpj': cnpj, 'total_avaliados': len(editais), 'total_validos': len(resultados_validos), 'resultados': resultados_validos })

@scoring_bp.route('/batch', methods=['POST'])
def scoring_batch_root():
    return rota_scoring_batch()

@analises_bp.route('/scoring/test', methods=['GET'])
def rota_scoring_test():
    logger.info("[scoring] rota_scoring_test chamada")
    return jsonify({ 'ok': True, 'msg': 'Scoring API ativa' })

# --- NL2SQL (consulta natural -> SQL seguro em DuckDB) ---
@analises_bp.route('/nl2sql', methods=['POST'])
@handle_errors
def api_nl2sql():
    from flask import request
    pergunta = str((request.get_json() or {}).get('pergunta') or '').strip().lower()
    limite = int((request.get_json() or {}).get('limite') or 100)
    limite = max(1, min(limite, 500))

    if not pergunta:
        raise ValidationError('Informe "pergunta"')

    tabela = 'empresas'
    select_cols = ['cnpj', 'razao_social', 'nome_fantasia', 'uf', 'municipio', 'cnae_principal']
    where = []
    order = ''
    aggregate = ''

    import re
    # Filtros básicos
    m_uf = re.search(r"\b(em|no)\s([a-z]{2})\b", pergunta)
    if m_uf:
        uf = m_uf.group(2).upper()
        where.append(f"uf = '{uf}'")
        tabela = 'estabelecimentos'
    m_mun = re.search(r"municipio\s([a-z\s\-]+)", pergunta)
    if m_mun:
        mun = m_mun.group(1).strip().replace("'", "")
        where.append(f"LOWER(municipio) LIKE LOWER('%{mun}%')")
        tabela = 'estabelecimentos'
    m_cnae = re.search(r"cnae\s(\d{4,7})", pergunta)
    if m_cnae:
        cnae = m_cnae.group(1)
        where.append(f"CAST(cnae_principal AS VARCHAR) LIKE '{cnae}%'")
        tabela = 'estabelecimentos'

    # Métricas
    if any(w in pergunta for w in ['quantas', 'qtd', 'quantidade', 'conta']):
        aggregate = 'COUNT(*) AS total'
    elif 'top' in pergunta:
        m_top = re.search(r"top\s(\d{1,3})", pergunta)
        n = int(m_top.group(1)) if m_top else 10
        order = 'ORDER BY capital_social DESC'
        limite = max(1, min(n, 100))

    # Monta SQL
    where_sql = (' WHERE ' + ' AND '.join(where)) if where else ''
    if aggregate:
        sql = f"SELECT {aggregate} FROM {tabela}{where_sql}"
    else:
        # Fallback para agregação segura quando não conhecemos o esquema exato
        sql = f"SELECT COUNT(*) AS total FROM {tabela}{where_sql}"

    try:
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW empresas AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['empresas']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW estabelecimentos AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW socios AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['socios']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW simples AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['simples']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW cnaes AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['cnaes']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW municipios AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['municipios']).replace('\\','/')}')")
        df = con.execute(sql).df()
        con.close()
        cols = [str(c) for c in (list(df.columns) if df is not None else [])]
        rows = (df.astype(str).values.tolist() if df is not None else [])
        return jsonify({ 'sql': sql, 'columns': cols, 'rows': rows })
    except Exception as e:
        return jsonify({ 'erro': 'falha ao executar consulta', 'detalhes': str(e), 'sql': sql }), 500

@analises_bp.route('/players/lista', methods=['GET'])
@handle_errors
def rota_players_lista():
    pagina = int(request.args.get('pagina', 1))
    termo = request.args.get('busca', '').strip()
    uf = request.args.get('uf', '').strip().upper()
    porte = request.args.get('porte', '').strip()
    situacao = request.args.get('situacao', '').strip()
    idade_range = request.args.get('idade', '').strip()
    ordenacao = request.args.get('ordem', 'score_desc').strip()
    
    limite = 20
    offset = (pagina - 1) * limite
    
    try:
        est_path = str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')
        emp_path = str(Config.ARQUIVOS_PARQUET['empresas']).replace('\\','/')
        soc_path = str(Config.ARQUIVOS_PARQUET['socios']).replace('\\','/')
        mun_path = str(Config.ARQUIVOS_PARQUET.get('municipios', '')).replace('\\','/')
        
        # Filtros
        condicoes = []
        
        # 1. Filtro de Situação
        if situacao:
             # Mapeamento simples de badgets do front para situação cadastral ou lógica
             if situacao.lower() == 'ativa':
                 condicoes.append("e.situacao_cadastral = '02'")
             else:
                 # Outros status (ex: baixada, suspensa) ou tipos (exportadora - requires more logic)
                 # Para simplificar, vamos assumir que o usuário busca ativas por padrão, a menos que especifique
                 condicoes.append("e.situacao_cadastral = '02'")
        else:
             condicoes.append("e.situacao_cadastral = '02'")

        # 2. Busca Textual (Nome ou CNAE)
        if termo:
            safe_termo = termo.replace("'", "")
            # Check if it looks like a CNAE (digits)
            import re
            cnae_digits = ''.join(filter(str.isdigit, safe_termo))
            if len(cnae_digits) >= 4:
                condicoes.append(f"(CAST(e.cnae_fiscal_principal AS VARCHAR) LIKE '{cnae_digits}%')")
            elif len(safe_termo) >= 3:
                condicoes.append(f"(emp.razao_social_nome_empresarial ILIKE '%{safe_termo}%' OR e.nome_fantasia ILIKE '%{safe_termo}%')")

        # 3. UF
        if uf:
            condicoes.append(f"e.uf = '{uf}'")

        # 4. Porte
        if porte:
            # Porte: 00-NI, 01-ME, 03-EPP, 05-Demais
            # Front sends: "MEI", "Microempresa", "Pequeno Porte", etc.
            # Map front values to codes if possible, or use text comparison match if column is text
            # Usually parquet stores '01', '03' etc. or decoded text?
            # Based on previous code, it seems to store codes or text. Let's assume text search or code map.
            # safer to try permissive match
            p = porte.lower()
            if 'micro' in p: condicoes.append("emp.porte_da_empresa IN ('01','1','00','Microempresa')")
            elif 'pequeno' in p: condicoes.append("emp.porte_da_empresa IN ('03','3','02','2','Pequeno Porte','EPP')")
            elif 'medio' in p or 'médio' in p: condicoes.append("emp.porte_da_empresa IN ('05','5')") # Aproximação
            elif 'grande' in p: condicoes.append("emp.porte_da_empresa IN ('05','5')") # Aproximação
            else: condicoes.append(f"emp.porte_da_empresa = '{porte}'")

        # 5. Idade
        if idade_range:
            # data_de_inicio_atividade YYYYMMDD
            current_year = int(time.strftime("%Y"))
            def get_date_str(years_ago):
                return f"{current_year - years_ago}0000"
            
            if idade_range == '0-2':
                condicoes.append(f"e.data_de_inicio_atividade >= '{get_date_str(2)}'")
            elif idade_range == '2-5':
                 condicoes.append(f"e.data_de_inicio_atividade >= '{get_date_str(5)}' AND e.data_de_inicio_atividade < '{get_date_str(2)}'")
            elif idade_range == '5-10':
                 condicoes.append(f"e.data_de_inicio_atividade >= '{get_date_str(10)}' AND e.data_de_inicio_atividade < '{get_date_str(5)}'")
            elif idade_range == '10+':
                 condicoes.append(f"e.data_de_inicio_atividade < '{get_date_str(10)}'")

        where_sql = "WHERE " + " AND ".join(condicoes)

        # Municipios Join
        mun_join = ""
        mun_col = "e.municipio"
        if Path(mun_path).exists():
           mun_join = f"LEFT JOIN read_parquet('{mun_path}') m ON CAST(e.municipio AS VARCHAR) = CAST(m.codigo AS VARCHAR)"
           mun_col = "m.descricao"

        # Sort Logic
        order_by = "emp.capital_social_da_empresa DESC" # Default
        if ordenacao:
            if 'capital_desc' in ordenacao: order_by = "emp.capital_social_da_empresa DESC"
            elif 'capital_asc' in ordenacao: order_by = "emp.capital_social_da_empresa ASC"
            elif 'idade_desc' in ordenacao: order_by = "e.data_de_inicio_atividade ASC" # Mais antiga = data menor
            elif 'idade_asc' in ordenacao: order_by = "e.data_de_inicio_atividade DESC" # Mais nova = data maior
            # Score sorting requires Python processing usually, or complex SQL. fallback for now.

        sql = f"""
            SELECT 
                e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj,
                emp.razao_social_nome_empresarial as razao_social,
                ANY_VALUE(s.nome_socio) as responsavel,
                ANY_VALUE(e.correio_eletronico) as email,
                ANY_VALUE(e.ddd_1) as ddd,
                ANY_VALUE(e.telefone_1) as telefone,
                ANY_VALUE(e.logradouro) || ', ' || ANY_VALUE(e.numero) as endereco,
                ANY_VALUE(e.bairro) as bairro,
                ANY_VALUE({mun_col}) as municipio_nome,
                ANY_VALUE(e.uf) as uf,
                ANY_VALUE(emp.capital_social_da_empresa) as capital_social,
                ANY_VALUE(emp.porte_da_empresa) as porte,
                ANY_VALUE(e.data_de_inicio_atividade) as data_inicio,
                ANY_VALUE(e.cnae_fiscal_principal) as cnae
            FROM read_parquet('{est_path}') e
            JOIN read_parquet('{emp_path}') emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN read_parquet('{soc_path}') s ON e.cnpj_basico = s.cnpj_basico
            {mun_join}
            {where_sql}
            GROUP BY e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv, emp.razao_social_nome_empresarial
            ORDER BY {order_by}
            LIMIT {limite} OFFSET {offset}
        """
        
        df = duckdb.sql(sql).to_df()
        
        lista = []
        current_year = int(time.strftime("%Y"))
        
        for _, r in df.iterrows():
            tel = ''
            try:
                ddd = str(int(float(r['ddd']))) if r['ddd'] else ''
                gomes = str(int(float(r['telefone']))) if r['telefone'] else ''
                if ddd and gomes:
                    tel = f"({ddd}) {gomes}"
            except: pass
            
            c = str(r['cnpj'])
            cnpj_fmt = f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}" if len(c) == 14 else c
            
            resp = r['responsavel']
            if not isinstance(resp, str): resp = '—'
            
            # Idade calc
            idade = 0
            try:
                dt_str = str(r['data_inicio'])
                y = int(dt_str[:4])
                idade = current_year - y
            except: pass

            # Porte desc
            porte_code = str(r['porte'])
            porte_desc = 'Demais'
            if porte_code in ['01','1']: porte_desc = 'Microempresa'
            elif porte_code in ['03','3']: porte_desc = 'Pequeno Porte'
            elif porte_code in ['05','5']: porte_desc = 'Demais'
            
            lista.append({
                'cnpj': cnpj_fmt,
                'razaoSocial': r['razao_social'],
                'nomeFantasia': r['razao_social'], # Fallback
                'responsavel': resp,
                'email': str(r['email']).lower() if isinstance(r['email'], str) else '—',
                'telefone': tel if tel else '—',
                'endereco': f"{r['endereco'] or ''} - {r['bairro'] or ''}",
                'municipio': r['municipio_nome'] if isinstance(r['municipio_nome'], str) else '—',
                'uf': r['uf'],
                'capitalSocial': float(r['capital_social'] or 0),
                'porte': porte_desc,
                'idade': idade,
                'cnae': r['cnae'],
                'cnaeDesc': '', # Could fetch description if needed
                'match': 0 # Basic search has no score
            })
            
        return jsonify({'data': lista, 'pagina': pagina, 'total_estimado': 1000 + offset})
        
    except Exception as e:
        logger.error(f"Erro rota_players_lista: {e}")
        return jsonify({'erro': str(e), 'data': []}), 500

@analises_bp.route('/ai/assist', methods=['POST'])
@_limit("20 per minute")
def api_ai_assist():
    dados = request.get_json() or {}
    mensagem = str(dados.get('mensagem') or '').strip()
    contexto = str(dados.get('contexto') or '').strip()
    session_id = str(dados.get('session_id') or request.remote_addr or 'default').strip()
    key = f"ai_session:{session_id}"
    historico = []
    try:
        historico = cache.get(key) or []
    except Exception:
        historico = []
    msgs = [{"role":"system","content":"Você é um analista especialista no contexto brasileiro (CNAE, NCM, natureza jurídica, PGFN, PNCP, IBGE, CAGED, COMEX, DIÁRIOS). Responda em português com objetividade, interprete códigos e termos, cruze dados fiscais e socioeconômicos e entregue ações práticas em bullets."}]
    if historico:
        msgs.extend(historico[-10:])
    rag_docs = _vector_search(mensagem, top_k=int(dados.get('top_k') or 5))
    rag_txt = ''
    if rag_docs:
        partes = []
        for d in rag_docs:
            meta = d.get('meta') or {}
            mtxt = ' '.join(f"{k}: {meta.get(k)}" for k in list(meta.keys())[:4]) if meta else ''
            partes.append(f"[{d.get('id')}] {mtxt}\n{d.get('text')}")
        rag_txt = "\n\nDocumentos relevantes:\n" + "\n\n".join(partes)
    user_content = f"Contexto: {contexto}{rag_txt}\n\nPergunta: {mensagem}"
    msgs.append({"role":"user","content": user_content})
    texto = _chat_ai(msgs, "")
    novo_hist = []
    try:
        novo_hist = (historico or []) + [{"role":"user","content": user_content}, {"role":"assistant","content": texto}]
        cache.set(key, novo_hist[-20:], expire=3600)
    except Exception:
        pass
    return jsonify({ 'texto': texto })

@analises_bp.route('/ai/index/build', methods=['POST'])
@_limit("5 per minute")
def api_ai_index_build():
    dados = request.get_json() or {}
    docs = dados.get('docs') or []
    mode = str(dados.get('mode') or 'replace')
    items = []
    for d in docs[:1000]:
        txt = str(d.get('text') or '').strip()
        if not txt:
            continue
        vid = str(d.get('id') or f"doc.{len(items)+1}")
        meta = d.get('meta') or {}
        vec = _embed_text(txt)
        items.append({ 'id': vid, 'text': txt, 'meta': meta, 'vector': vec.tobytes(), 'dim': int(vec.shape[0]), 'ts': int(time.time()) })
    res = _vector_upsert(items, mode=mode)
    return jsonify(res)

@analises_bp.route('/ai/search', methods=['POST'])
@_limit("30 per minute")
def api_ai_search():
    dados = request.get_json() or {}
    q = str(dados.get('query') or '').strip()
    k = int(dados.get('top_k') or 5)
    res = _vector_search(q, top_k=k)
    return jsonify({ 'results': res })

@analises_bp.route('/ai/schema', methods=['GET'])
@_limit("30 per minute")
def api_ai_schema():
    return jsonify(_schema_tables())

@analises_bp.route('/ai/nlq', methods=['POST'])
@_limit("20 per minute")
def api_ai_nlq():
    dados = request.get_json() or {}
    pergunta = str(dados.get('pergunta') or '').strip()
    limite = max(10, min(200, int(dados.get('limit') or 100)))
    if not pergunta:
        return jsonify({ 'error': 'pergunta vazia' }), 400
    schema = _schema_tables()
    api_key = os.environ.get('AI_API_KEY')
    if not api_key:
        try:
            df = buscar_por_palavra_chave(pergunta)
            if df is None or df.empty:
                return jsonify({ 'sql': None, 'columns': [], 'rows': [] })
            cols = [str(c) for c in df.columns][:50]
            rows = df.iloc[:limite].astype(str).values.tolist()
            return jsonify({ 'sql': None, 'columns': cols, 'rows': rows })
        except Exception:
            return jsonify({ 'sql': None, 'columns': [], 'rows': [] })
    system = (
        "Você é um gerador de SQL para DuckDB. Use as tabelas virtuais: empresas, estabelecimentos, socios, simples, cnaes, municipios. "
        "Gere apenas um SELECT seguro, sem DDL/DML. Limite resultados com LIMIT. Use nomes de colunas do esquema fornecido."
    )
    schema_text = "\n".join([f"{k}: {', '.join(v['columns'][:20])}" for k,v in schema.items()])
    prompt = f"Esquema:\n{schema_text}\n\nPergunta: {pergunta}\nSQL DuckDB:"
    msgs = [
        {"role":"system","content": system},
        {"role":"user","content": prompt}
    ]
    sql = _chat_ai(msgs, "")
    sql = (sql or '').strip()
    sql = sql.split(';')[0]
    bad = any(x in sql.lower() for x in ['insert','update','delete','drop','alter'])
    if bad or not sql.lower().startswith('select'):
        return jsonify({ 'error': 'sql inválido', 'sql': sql }), 400
    if 'limit' not in sql.lower():
        sql = sql + f" LIMIT {limite}"
    try:
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW empresas AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['empresas']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW estabelecimentos AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW socios AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['socios']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW simples AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['simples']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW cnaes AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['cnaes']).replace('\\','/')}')")
        con.execute(f"CREATE OR REPLACE VIEW municipios AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['municipios']).replace('\\','/')}')")
        df = con.execute(sql).df()
        con.close()
        cols = [str(c) for c in (list(df.columns) if df is not None else [])]
        rows = (df.iloc[:limite].astype(str).values.tolist() if df is not None else [])
        return jsonify({ 'sql': sql, 'columns': cols, 'rows': rows })
    except Exception:
        return jsonify({ 'error': 'falha ao executar sql', 'sql': sql }), 500

def _score_risco_fiscal(pgfn: dict) -> int:
    try:
        poss = bool(pgfn.get('possui_divida'))
        total = int(pgfn.get('total_registros') or 0)
        if not poss:
            return 15
        if total >= 10:
            return 90
        if total >= 5:
            return 75
        if total >= 1:
            return 60
        return 30
    except Exception:
        return 20

def _score_exportador(cnae: str) -> int:
    s = ''.join([c for c in str(cnae or '') if c.isdigit()])
    if not s:
        return 20
    if s.startswith('46'):
        return 55
    if s.startswith('10') or s.startswith('20') or s.startswith('22'):
        return 45
    return 25

def _score_logistico(uf: str) -> int:
    u = str(uf or '').upper()
    if u in ['SP','PR','SC','RS','MG','RJ']:
        return 80
    if u in ['BA','GO','ES','MS','DF']:
        return 65
    return 50

def _score_maturidade_esg(porte: str, anos: float) -> int:
    p = str(porte or '').upper()
    base = 40
    if p in ['GRANDE','MEDIO']:
        base += 20
    if anos >= 5:
        base += 20
    elif anos >= 3:
        base += 10
    return min(95, base)

def _anos_experiencia(data_str: str) -> float:
    try:
        s = str(data_str or '')
        for fmt in ['%Y-%m-%d','%d/%m/%Y','%Y%m%d']:
            try:
                dt = pd.to_datetime(s, format=fmt, errors='raise')
                return max(0.0, (pd.Timestamp.now() - dt).days/365.25)
            except Exception:
                continue
        dt = pd.to_datetime(s, errors='coerce')
        if pd.isna(dt):
            return 0.0
        return max(0.0, (pd.Timestamp.now() - dt).days/365.25)
    except Exception:
        return 0.0

def _to_float(x) -> float:
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x or '').strip()
        if not s:
            return 0.0
        s = s.replace('.', '').replace(',', '.')
        return float(s)
    except Exception:
        return 0.0

@analises_bp.route('/kpis/cnpj', methods=['POST'])
@_limit("30 per minute")
def api_kpis_cnpj():
    dados = request.get_json() or {}
    cnpj = str(dados.get('cnpj') or '').strip()
    if not cnpj:
        return jsonify({ 'error': 'cnpj requerido' }), 400
    info = consultar_cnpj_completo(cnpj)
    if info is None or info.empty:
        return jsonify({ 'error': 'cnpj não encontrado' }), 404
    cols = list(info.columns)
    def pick(name_opts):
        for n in name_opts:
            if n in cols:
                return n
        return None
    uf_col = pick(['uf'])
    mun_col = pick(['municipio','nome_municipio'])
    cnae_col = pick(['cnae_fiscal','cnae_fiscal_principal'])
    porte_col = pick(['porte_da_empresa','porte'])
    capital_col = pick(['capital_social_da_empresa','capital_social'])
    situacao_col = pick(['situacao_cadastral'])
    abertura_col = pick(['data_de_inicio_atividade','data_abertura'])
    row = info.iloc[0]
    uf = str(row.get(uf_col) or '')
    municipio = str(row.get(mun_col) or '')
    cnae = str(row.get(cnae_col) or '')
    porte = str(row.get(porte_col) or '')
    capital = _to_float(row.get(capital_col) or 0)
    situacao = str(row.get(situacao_col) or '')
    anos = _anos_experiencia(str(row.get(abertura_col) or ''))
    pgfn = verificar_divida_pgfn(cnpj)
    risco = _score_risco_fiscal(pgfn)
    exp = _score_exportador(cnae)
    logi = _score_logistico(uf)
    esg = _score_maturidade_esg(porte, anos)
    saz = 'Média' if cnae[:2] in ['10','11','47'] else 'Baixa'
    proj = 2.0 + (1.0 if anos>=5 else 0.5)
    return jsonify({
        'cnpj': cnpj,
        'uf': uf,
        'municipio': municipio,
        'cnae': cnae,
        'porte': porte,
        'capital_social': capital,
        'situacao': situacao,
        'anos_mercado': anos,
        'kpis': {
            'score_risco_fiscal': risco,
            'score_exportador': exp,
            'score_logistico': logi,
            'maturidade_esg': esg,
            'sazonalidade_setorial': saz,
            'projecao_movimento_percent': proj
        }
    })

@analises_bp.route('/kpis/template', methods=['POST'])
@_limit("20 per minute")
def api_kpis_template():
    dados = request.get_json() or {}
    cnpj = str(dados.get('cnpj') or '').strip()
    if not cnpj:
        return jsonify({ 'error': 'cnpj requerido' }), 400
    r = api_kpis_cnpj()
    if isinstance(r, tuple):
        return r
    data = r.get_json() or {}
    k = data.get('kpis') or {}
    tpl = (
        "# KPIs executivos\n\n"
        "- Score de risco fiscal: **{rf}**\n"
        "- Score exportador: **{ex}**\n"
        "- Score logístico: **{lg}**\n"
        "- Maturidade ESG: **{esg}**\n"
        "- Sazonalidade do setor: **{saz}**\n"
        "- Projeção de movimento econômico: **{pr}%**\n\n"
        "## Insights\n"
        "- UF: **{uf}**; Município: **{mun}**\n"
        "- CNAE: **{cnae}**; Porte: **{porte}**\n"
    ).format(rf=k.get('score_risco_fiscal'), ex=k.get('score_exportador'), lg=k.get('score_logistico'), esg=k.get('maturidade_esg'), saz=k.get('sazonalidade_setorial'), pr=k.get('projecao_movimento_percent'), uf=data.get('uf'), mun=data.get('municipio'), cnae=data.get('cnae'), porte=data.get('porte'))
    msgs = [
        {"role":"system","content":"Você é um analista especialista em KPIs brasileiros (CNAE, NCM, natureza jurídica, PGFN, PNCP, IBGE, CAGED, COMEX, DIÁRIOS). Reescreva o template em português claro, com bullets e próximos passos práticos."},
        {"role":"user","content": tpl}
    ]
    texto = _chat_ai(msgs, tpl)
    return jsonify({ 'texto': texto, 'kpis': k })

@analises_bp.route('/ai/report/pdf', methods=['POST'])
@_limit("15 per minute")
def api_ai_report_pdf():
    dados = request.get_json() or {}
    texto = str(dados.get('texto') or '').strip()
    titulo = str(dados.get('titulo') or 'Relatório Executivo').strip()
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_font('Inter', '', '', uni=True)
    try:
        pdf.set_font('Inter', size=14)
    except Exception:
        pdf.set_font('Arial', size=14)
    pdf.cell(0, 10, titulo, ln=1)
    try:
        pdf.set_font('Inter', size=11)
    except Exception:
        pdf.set_font('Arial', size=11)
    for line in texto.split('\n'):
        pdf.multi_cell(0, 8, line)
    out = pdf.output(dest='S').encode('latin-1', errors='ignore')
    resp = make_response(out)
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = 'attachment; filename=relatorio_executivo.pdf'
    return resp

@scoring_bp.route('/test', methods=['GET'])
def scoring_test_root():
    logger.info("[scoring] scoring_test_root chamada")
    return jsonify({ 'ok': True, 'msg': 'Scoring API ativa' })

@analises_bp.route('/prospecting/estudo', methods=['POST'])
@handle_errors
def api_prospecting_estudo():
    from services.services_analise_service import estudo_compatibilidade_mercado
    dados = request.get_json() or {}
    cnpj = str(dados.get('cnpj') or '').strip()
    top_n = int(dados.get('top_n') or 10)
    cnpj_digits = ''.join(ch for ch in cnpj if ch.isdigit())
    if not cnpj_digits or len(cnpj_digits) != 14:
        raise ValidationError("Informe um CNPJ válido com 14 dígitos")
    resultado = estudo_compatibilidade_mercado(cnpj_digits, top_n=top_n)
    return jsonify(resultado)

@analises_bp.route('/kpis/geral', methods=['GET'])
@handle_errors
def api_kpis_geral():
    uf = request.args.get('uf')
    municipio = request.args.get('municipio')
    cnae = request.args.get('cnae')
    ano_min = request.args.get('ano_min')
    ano_max = request.args.get('ano_max')
    try:
        con = duckdb.connect()
        fp = str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')
        dt_inicio = "STRPTIME(regexp_replace(cast(data_de_inicio_atividade as varchar), '[^0-9]', ''), '%Y%m%d')"
        dt_situacao = "STRPTIME(regexp_replace(cast(data_da_situacao_cadastral as varchar), '[^0-9]', ''), '%Y%m%d')"
        where = []
        if uf:
            where.append(f"upper(cast(uf as varchar)) = upper('{str(uf)}')")
        if municipio:
            where.append(f"upper(cast(municipio as varchar)) = upper('{str(municipio)}')")
        if cnae:
            where.append(f"cast(cnae_fiscal_principal as varchar) LIKE '%{str(cnae)}%'")
        if ano_min:
            where.append(f"CAST(strftime({dt_inicio}, '%Y') AS INTEGER) >= {int(ano_min)}")
        if ano_max:
            where.append(f"CAST(strftime({dt_inicio}, '%Y') AS INTEGER) <= {int(ano_max)}")
        where_sql = (' WHERE ' + ' AND '.join(where)) if where else ''
        q_total_filt = f"SELECT count(*) AS total FROM read_parquet('{fp}'){where_sql}"
        df_total_filt = con.execute(q_total_filt).df()
        total_filtrados = int(df_total_filt.iloc[0]['total']) if not df_total_filt.empty else 0
        q_ativas = (
            f"SELECT count(*) AS total FROM read_parquet('{fp}'){where_sql}{' AND ' if where_sql else ' WHERE '}cast(situacao_cadastral as varchar)='02'"
        )
        df_ativas = con.execute(q_ativas).df()
        total_ativas = int(df_ativas.iloc[0]['total']) if not df_ativas.empty else 0
        # Entradas nos últimos 30 dias
        try:
            cond_30d = f"{dt_inicio} >= dateadd('day', -30, current_date)"
            q_ent_30 = f"SELECT COUNT(*) AS c FROM read_parquet('{fp}'){where_sql}{' AND ' if where_sql else ' WHERE '}{cond_30d}"
            df_e30 = con.execute(q_ent_30).df()
            entradas_30dias = int(df_e30.iloc[0]['c']) if not df_e30.empty else 0
        except Exception:
            entradas_30dias = None
        q_idade = (
            f"SELECT avg(date_diff('year', {dt_inicio}, current_date)) AS idade_media "
            f"FROM read_parquet('{fp}'){where_sql}"
        )
        df_idade = con.execute(q_idade).df()
        idade_media = float(df_idade.iloc[0]['idade_media']) if not df_idade.empty else None
        q_ent12 = (
            f"SELECT strftime({dt_inicio}, '%Y-%m') AS ym, count(*) AS c "
            f"FROM read_parquet('{fp}'){where_sql}{' AND ' if where_sql else ' WHERE '}{dt_inicio} IS NOT NULL GROUP BY ym ORDER BY ym DESC LIMIT 12"
        )
        ent = con.execute(q_ent12).df()
        entradas = [{ 'label': str(r['ym']), 'count': int(r['c']) } for _, r in ent.iloc[::-1].iterrows()] if not ent.empty else []
        q_ent2 = (
            f"SELECT strftime({dt_inicio}, '%Y-%m') AS ym, count(*) AS c "
            f"FROM read_parquet('{fp}'){where_sql}{' AND ' if where_sql else ' WHERE '}{dt_inicio} IS NOT NULL GROUP BY ym ORDER BY ym DESC LIMIT 2"
        )
        ent2 = con.execute(q_ent2).df()
        entradas_mes_vigente = int(ent2.iloc[0]['c']) if not ent2.empty else None
        entradas_mes_anterior = int(ent2.iloc[1]['c']) if (ent2 is not None and len(ent2)>=2) else None
        entradas_mes_vigente_label = str(ent2.iloc[0]['ym']) if not ent2.empty else None
        entradas_mes_anterior_label = str(ent2.iloc[1]['ym']) if (ent2 is not None and len(ent2)>=2) else None
        try:
            q_sai12 = (
                f"SELECT strftime({dt_situacao}, '%Y-%m') AS ym, count(*) AS c "
                f"FROM read_parquet('{fp}'){where_sql}{' AND ' if where_sql else ' WHERE '}"
                f"({dt_situacao} IS NOT NULL) AND ("
                f"upper(cast(situacao_cadastral as varchar)) = '08' OR upper(cast(situacao_cadastral as varchar)) = 'BAIXADA'"
                f") GROUP BY ym ORDER BY ym DESC LIMIT 12"
            )
            sai = con.execute(q_sai12).df()
        except Exception:
            sai = None
        saidas = [{ 'label': str(r['ym']), 'count': int(r['c']) } for _, r in (sai.iloc[::-1].iterrows() if (sai is not None and not sai.empty) else [])] if (sai is not None and not sai.empty) else []
        q_validos = (
            f"SELECT count(*) AS total, sum(CASE WHEN coalesce(nullif(cast(correio_eletronico as varchar), ''), nullif(cast(telefone_1 as varchar), ''), nullif(cast(logradouro as varchar), ''), nullif(cast(cep as varchar), '')) IS NOT NULL THEN 1 ELSE 0 END) AS ok "
            f"FROM read_parquet('{fp}'){where_sql}"
        )
        try:
            dv = con.execute(q_validos).df()
            pct_validos = float((int(dv.iloc[0]['ok'])/max(1,int(dv.iloc[0]['total'])))*100.0) if not dv.empty else 0.0
        except Exception:
            pct_validos = None
        try:
            sai_latest = int((saidas[-1]['count'] if (saidas and len(saidas)>0) else 0))
        except Exception:
            sai_latest = 0
        churn_pct = (min(100.0, (sai_latest / max(1, total_ativas)) * 100.0) if total_ativas else None)
        risk_score = None
        try:
            base = 50.0
            inc = (churn_pct or 0.0) * 0.5
            dec = (pct_validos or 0.0) * 0.3
            risk_score = max(0.0, min(100.0, base + inc - dec))
        except Exception:
            risk_score = None
        q_top_cnae = f"SELECT cast(cnae_fiscal_principal as varchar) AS cnae, count(*) AS c FROM read_parquet('{fp}'){where_sql} GROUP BY cnae ORDER BY c DESC LIMIT 10"
        top_cnae = con.execute(q_top_cnae).df()
        
        # Tentar carregar descrições de CNAEs
        try:
            cnae_path = str(Config.ARQUIVOS_PARQUET.get('cnaes', '')).replace('\\','/')
            if Path(cnae_path).exists():
                # Reexecutar query com JOIN para pegar descrições
                q_top_cnae_desc = (
                    f"SELECT e.cnae_fiscal_principal AS cnae, c.descricao, COUNT(*) as c "
                    f"FROM read_parquet('{fp}') e "
                    f"LEFT JOIN read_parquet('{cnae_path}') c ON cast(e.cnae_fiscal_principal as varchar) = cast(c.codigo as varchar) "
                    f"{where_sql} GROUP BY e.cnae_fiscal_principal, c.descricao ORDER BY c DESC LIMIT 15"
                )
                top_cnae = con.execute(q_top_cnae_desc).df()
                setores = [
                    {
                        'label': f"{str(r.get('descricao', ''))[:50] if 'descricao' in r and r['descricao'] else str(r['cnae']).zfill(7)}", 
                        'cnae': str(r['cnae']).zfill(7),
                        'count': int(r['c'])
                    } 
                    for _, r in top_cnae.iterrows()
                ] if not top_cnae.empty else []
            else:
                setores = [{ 'label': str(r['cnae']), 'cnae': str(r['cnae']).zfill(7), 'count': int(r['c']) } for _, r in top_cnae.iterrows()] if not top_cnae.empty else []
        except Exception as e:
            logger.warning(f"Erro ao carregar descrições de CNAEs: {e}")
            setores = [{ 'label': str(r['cnae']), 'cnae': str(r['cnae']).zfill(7), 'count': int(r['c']) } for _, r in top_cnae.iterrows()] if not top_cnae.empty else []
        q_ufs = f"SELECT cast(uf as varchar) AS uf, count(*) AS c FROM read_parquet('{fp}'){where_sql} GROUP BY uf ORDER BY c DESC LIMIT 10"
        ufs = con.execute(q_ufs).df()
        mapa = [{ 'label': str(r['uf']), 'count': int(r['c']) } for _, r in ufs.iterrows()] if not ufs.empty else []
        try:
            q_ent_m1 = (
                "SELECT cast(uf as varchar) AS uf, count(*) AS c FROM read_parquet('{fp}') "
                f"WHERE strftime({dt_inicio}, '%Y-%m') = strftime(add_months(current_date,-1),'%Y-%m')"
                f"{' AND ' + ' AND '.join(where) if where else ''} GROUP BY uf"
            )
            q_ent_m2 = (
                "SELECT cast(uf as varchar) AS uf, count(*) AS c FROM read_parquet('{fp}') "
                f"WHERE strftime({dt_inicio}, '%Y-%m') = strftime(add_months(current_date,-2),'%Y-%m')"
                f"{' AND ' + ' AND '.join(where) if where else ''} GROUP BY uf"
            )
            df_m1 = con.execute(q_ent_m1).df()
            df_m2 = con.execute(q_ent_m2).df()
            d1 = { str(row['uf']): int(row['c']) for _, row in (df_m1.iterrows() if not df_m1.empty else []) }
            d2 = { str(row['uf']): int(row['c']) for _, row in (df_m2.iterrows() if not df_m2.empty else []) }
            states = []
            keys = set(d1.keys()) | set(d2.keys())
            for k in keys:
                states.append({ 'label': k, 'delta': int(d1.get(k,0) - d2.get(k,0)) })
            estados_crescimento = sorted(states, key=lambda x: x['delta'], reverse=True)[:10]
        except Exception:
            estados_crescimento = []
        con.close()
        evol_labels = [x['label'] for x in entradas]
        evol_vals = [x['count'] for x in entradas]
        ent_labels = [x['label'] for x in entradas]
        ent_vals = [x['count'] for x in entradas]
        sai_vals = [x['count'] for x in saidas] if saidas else [0]*len(ent_vals)
        return jsonify({
            'cards': {
                'total_ativas': total_ativas,
                'entradas_mensais': entradas,
                'saidas_mensais': saidas,
                'entradas_mes_vigente': entradas_mes_vigente,
                'entradas_mes_anterior': entradas_mes_anterior,
                'entradas_mes_vigente_label': entradas_mes_vigente_label,
                'entradas_mes_anterior_label': entradas_mes_anterior_label,
                'entradas_30_dias': entradas_30dias,
                'idade_media': idade_media,
                'pct_dados_validos': pct_validos,
                'risk_score': risk_score
            },
            'graficos': {
                'evolucao_ativas': { 'labels': evol_labels, 'valores': evol_vals },
                'entradas_vs_saidas': { 'labels': ent_labels, 'entradas': ent_vals, 'saidas': sai_vals },
                'mapa_calor': { 'labels': [m['label'] for m in mapa], 'valores': [m['count'] for m in mapa] }
            },
            'ranking': {
                'setores_top': setores,
                'estados_crescimento': estados_crescimento
            },
            'total_filtrados': total_filtrados
        })
    except Exception as e:
        return jsonify({ 'erro': str(e) }), 500

@analises_bp.route('/cnaes/setores', methods=['GET'])
@handle_errors
def api_listar_setores():
    """
    Retorna lista de setores da economia
    """
    from services.services_analise_service import get_setores_economia
    return jsonify(get_setores_economia())

@analises_bp.route('/cnaes/sugerir', methods=['GET'])
@handle_errors
def api_sugerir_cnaes():
    """
    Sugere CNAEs baseado em termo de busca e setor opcional
    """
    termo = request.args.get('termo', '').strip()
    setor = request.args.get('setor', '').strip()

    if len(termo) < 2 and not any(ch.isdigit() for ch in termo):
        return jsonify([])

    logger.info(f"Sugestão de CNAEs para: '{termo}' (Setor: {setor})")
    resultados = sugerir_cnaes(termo, setor_filtro=setor)
    return jsonify(resultados)

@analises_bp.route('/setorial', methods=['POST'])
@handle_errors
def api_analise_setorial():
    """
    Executa análise setorial completa
    """
    dados = request.json or {}

    cnae_codes = dados.get('cnae_codes')
    termo_busca = dados.get('termo_busca')
    uf_filtro = dados.get('uf')
    municipio_filtro = dados.get('municipio')
    somente_ativas = bool(dados.get('somente_ativas', False))
    ano_inicio_min = dados.get('ano_inicio_min')
    ano_inicio_max = dados.get('ano_inicio_max')
    try:
        limite = int(dados.get('limite', 20))
    except Exception:
        limite = 20
    if limite <= 0:
        limite = 20
    if limite > 200:
        limite = 200

    # Valida entrada
    if not cnae_codes and not termo_busca:
        raise ValidationError("É necessário informar 'cnae_codes' ou 'termo_busca'")

    if cnae_codes:
        if not isinstance(cnae_codes, list) or not cnae_codes:
            raise ValidationError("cnae_codes deve ser uma lista não vazia")

        # Valida formato dos CNAEs
        for cnae in cnae_codes:
            if not str(cnae).isdigit():
                raise ValidationError(f"CNAE inválido: {cnae}")

    logger.info(
        f"Análise setorial - CNAEs: {cnae_codes}, "
        f"Termo: {termo_busca}, UF: {uf_filtro}"
    )

    # Executa análise
    resultado = executar_analise_setorial(
        cnae_codes=cnae_codes,
        termo_busca=termo_busca,
        uf=uf_filtro,
        municipio=municipio_filtro,
        somente_ativas=somente_ativas,
        ano_inicio_min=ano_inicio_min,
        ano_inicio_max=ano_inicio_max,
        limit=limite
    )

    if not resultado:
        raise NotFoundError(
            "Nenhum estabelecimento encontrado com os filtros informados"
        )

    # Serializa dados
    if 'empresas' in resultado and resultado['empresas'] is not None:
        resultado['empresas'] = serializar_dataframe(resultado['empresas'])

    if 'dados_graficos' in resultado:
        resultado['dados_graficos'] = serializar_dados_graficos(
            resultado['dados_graficos']
        )

    return jsonify(resultado)
@analises_bp.route('/ai/index/build_from_parquet', methods=['POST'])
@_limit("5 per minute")
def api_ai_index_build_from_parquet():
    dados = request.get_json() or {}
    table = str(dados.get('table') or '').strip()
    limit = max(10, min(500, int(dados.get('limit') or 100)))
    uf = dados.get('uf')
    municipio = dados.get('municipio')
    if table not in Config.ARQUIVOS_PARQUET.keys():
        return jsonify({ 'error': 'tabela inválida' }), 400
    try:
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{str(Config.ARQUIVOS_PARQUET[table]).replace('\\','/')}')")
        where = []
        if uf:
            where.append(f"upper(cast(uf as varchar)) = upper('{str(uf)}')")
        if municipio:
            where.append(f"upper(cast(municipio as varchar)) = upper('{str(municipio)}')")
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        q = f"SELECT * FROM {table}{where_sql} LIMIT {limit}"
        df = con.execute(q).df()
        con.close()
    except Exception:
        return jsonify({ 'error': 'falha ao ler parquet' }), 500
    if df is None or df.empty:
        return jsonify({ 'ok': True, 'total_indexed': 0 })
    cols = list(df.columns)
    items = []
    for i in range(min(limit, len(df))):
        row = df.iloc[i]
        razao = str(row.get('razao_social_nome_empresarial') or row.get('razao_social') or row.get('nome_empresarial') or '').strip()
        cnpj_basico = str(row.get('cnpj_basico') or '')
        ufv = str(row.get('uf') or '')
        mun = str(row.get('municipio') or '')
        cnae = str(row.get('cnae_fiscal_principal') or '')
        porte = str(row.get('porte_da_empresa') or row.get('porte') or '')
        nat = str(row.get('natureza_juridica') or '')
        try:
            from services.services_cnpj_service import _cnae_desc
            cnae_desc = _cnae_desc(cnae) or ''
        except Exception:
            cnae_desc = ''
        text = f"Empresa {razao} CNPJ {cnpj_basico} UF {ufv} Municipio {mun} CNAE {cnae} {cnae_desc} Porte {porte} Natureza {nat}"
        meta = { 'cnpj_basico': cnpj_basico, 'uf': ufv, 'municipio': mun, 'cnae': cnae, 'porte': porte, 'natureza_juridica': nat, 'table': table }
        vec = _embed_text(text)
        items.append({ 'id': f"parquet:{table}:{i}:{cnpj_basico}", 'text': text, 'meta': meta, 'vector': vec.tobytes(), 'dim': int(vec.shape[0]), 'ts': int(time.time()) })
    total = _index_append_items(items)
    return jsonify({ 'ok': True, 'total_indexed': len(items), 'total_items': total })
