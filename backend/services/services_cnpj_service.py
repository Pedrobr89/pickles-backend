"""
Rotas da API para integrações com autarquias públicas
"""

import pandas as pd
from typing import Optional, Dict
from pathlib import Path
from core.config import Config
from services.services_cache_service import cache
import duckdb
from pathlib import Path
import pyarrow.parquet as pq
from functools import lru_cache
import logging
from utils.utils_validator import normalizar_cnpj

logger = logging.getLogger(__name__)

_orquestrador = None

def _read_parquet(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None

def _find_cnpj_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [c for c in df.columns if 'cnpj' in c.lower()]
    return candidates[0] if candidates else None

def _digits(val) -> str:
    try:
        return ''.join(filter(str.isdigit, str(val)))
    except Exception:
        return ''

def _get_field(df: Optional[pd.DataFrame], candidates: list):
    try:
        if df is None or df.empty:
            return None
        for c in candidates:
            if c in df.columns:
                v = df.iloc[0].get(c)
                if v is not None and str(v).strip() != '':
                    return v
        return None
    except Exception:
        return None

def obter_cnae_principal_por_cnpj(cnpj: str) -> Optional[str]:
    """
    Obtém o CNAE principal de um CNPJ a partir de ESTABELECIMENTOS, com fallback para EMPRESAS
    """
    num = normalizar_cnpj(cnpj)
    if not num or len(num) != 14:
        return None
    try:
        df_est = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[
                ('cnpj_basico','==', num[:8]),
                ('cnpj_ordem','==', num[8:12]),
                ('cnpj_dv','==', num[12:])
            ],
            columns=['cnae_fiscal_principal']
        )
        if df_est is not None and not df_est.empty:
            val = df_est.iloc[0].get('cnae_fiscal_principal')
            if val:
                return str(val).zfill(7)
    except Exception:
        pass
    try:
        df_emp = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico','==', num[:8])],
            columns=['cnae_fiscal']
        )
        if df_emp is not None and not df_emp.empty:
            val = df_emp.iloc[0].get('cnae_fiscal')
            if val:
                return str(val).zfill(7)
    except Exception:
        pass
    return None

def consultar_cnpj_completo(cnpj: str) -> Optional[pd.DataFrame]:
    cnpj_num = normalizar_cnpj(cnpj)
    if not cnpj_num or len(cnpj_num) != 14:
        return None
    cache_key = f"cnpj:{cnpj_num}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    # Tentativa 0: reconstruir CNPJ pelas colunas (estabelecimentos + empresas) usando DuckDB
    try:
        pf_est = pq.ParquetFile(str(Config.ARQUIVOS_PARQUET['estabelecimentos']))
        cols_est = [c.name for c in pf_est.schema]
        basico = next((c for c in ['cnpj_basico','CNPJ_BASICO','cnpjBasico','cnpjbasico'] if c in cols_est), None)
        ordem = next((c for c in ['cnpj_ordem','CNPJ_ORDEM','cnpjOrdem','ordem'] if c in cols_est), None)
        dv = next((c for c in ['cnpj_dv','CNPJ_DV','cnpjDV','dv'] if c in cols_est), None)
        if not (basico and ordem and dv):
            raise ValueError('Colunas de CNPJ no ESTABELECIMENTOS não encontradas')

        pf_emp = pq.ParquetFile(str(Config.ARQUIVOS_PARQUET['empresas']))
        cols_emp = [c.name for c in pf_emp.schema]
        emp_basico = next((c for c in ['cnpj_basico','CNPJ_BASICO','cnpjBasico','cnpjbasico'] if c in cols_emp), None)
        razao = next((c for c in ['razao_social_nome_empresarial','razao_social','nome_empresarial'] if c in cols_emp), None) or cols_emp[0]
        nome_emp = next((c for c in ['nome_empresarial','razao_social_nome_empresarial','razao_social'] if c in cols_emp), None) or razao
        capital1 = next((c for c in ['capital_social_da_empresa','capital_social'] if c in cols_emp), None)
        porte1 = next((c for c in ['porte_da_empresa','porte'] if c in cols_emp), None)
        natureza = next((c for c in ['natureza_juridica'] if c in cols_emp), None)
        cnae = next((c for c in ['cnae_fiscal'] if c in cols_emp), None)
        if not emp_basico:
            raise ValueError('Coluna de cnpj_basico no EMPRESAS não encontrada')

        q = f"""
        WITH est AS (
            SELECT 
                regexp_replace(cast({basico} as VARCHAR),'[^0-9]','') AS est_basico,
                regexp_replace(cast({ordem} as VARCHAR),'[^0-9]','') AS est_ordem,
                regexp_replace(cast({dv} as VARCHAR),'[^0-9]','') AS est_dv
            FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')}')
        ), emp AS (
            SELECT 
                regexp_replace(cast({emp_basico} as VARCHAR),'[^0-9]','') AS emp_basico,
                {razao} AS razao_social_nome_empresarial,
                {nome_emp} AS nome_empresarial,
                {capital1 or 'NULL'} AS capital_social_da_empresa,
                {porte1 or 'NULL'} AS porte_da_empresa,
                {natureza or 'NULL'} AS natureza_juridica,
                {cnae or 'NULL'} AS cnae_fiscal
            FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['empresas']).replace('\\','/')}')
        )
        SELECT 
            emp.emp_basico AS cnpj_basico,
            razao_social_nome_empresarial,
            nome_empresarial,
            capital_social_da_empresa,
            porte_da_empresa,
            natureza_juridica,
            cnae_fiscal,
            (est.est_basico || est.est_ordem || est.est_dv) AS cnpj
        FROM est 
        JOIN emp ON emp.emp_basico = est.est_basico
        WHERE (est.est_basico || est.est_ordem || est.est_dv) = '{cnpj_num}'
        LIMIT 1
        """
        df_join = duckdb.sql(q).to_df()
        if df_join is not None and not df_join.empty:
            cache.set(cache_key, df_join, expire=3600)
            return df_join
    except Exception:
        pass

    # Primeira tentativa: EMPRESAS por cnpj_basico (rápido)
    try:
        df_emp = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico','==', cnpj_num[:8])],
            columns=['cnpj_basico','razao_social_nome_empresarial','nome_empresarial','capital_social_da_empresa','capital_social','porte_da_empresa','porte','natureza_juridica','cnae_fiscal']
        )
        if df_emp is not None and not df_emp.empty:
            cache.set(cache_key, df_emp, expire=3600)
            return df_emp
    except Exception:
        pass
    try:
        df_est = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[
                ('cnpj_basico', '==', cnpj_num[:8]),
                ('cnpj_ordem', '==', cnpj_num[8:12]),
                ('cnpj_dv', '==', cnpj_num[12:])
            ],
            columns=['cnpj_basico']
        )
        if df_est is not None and not df_est.empty:
            basicos = df_est['cnpj_basico'].astype(str).unique().tolist()
            if basicos:
                df_emp = pd.read_parquet(
                    Config.ARQUIVOS_PARQUET['empresas'],
                    filters=[('cnpj_basico', 'in', basicos)],
                    columns=['cnpj_basico','razao_social_nome_empresarial','nome_empresarial','capital_social_da_empresa','capital_social','porte_da_empresa','porte','natureza_juridica','cnae_fiscal']
                )
                if df_emp is not None and not df_emp.empty:
                    cache.set(cache_key, df_emp, expire=3600)
                    return df_emp
    except Exception:
        pass
    # Fallback 2: busca com DuckDB usando coluna de CNPJ detectada (robusto a formatos)
    try:
        q = f"SELECT * FROM parquet_scan('{Config.ARQUIVOS_PARQUET['empresas']}') WHERE regexp_replace(cast({{'cnpj_col'}} as VARCHAR), '[^0-9]', '') LIKE '%{cnpj_num}%'"
        empresas = _read_parquet(Config.ARQUIVOS_PARQUET['empresas'])
        if empresas is None or empresas.empty:
            return None
        col = _find_cnpj_column(empresas)
        if not col:
            return None
        q = q.replace("{{'cnpj_col'}}", col)
        df_emp = duckdb.sql(q).to_df()
        if df_emp is None or df_emp.empty:
            return None
        cache.set(cache_key, df_emp, expire=3600)
        return df_emp
    except Exception:
        return None

def verificar_divida_pgfn(cnpj: str) -> dict:
    num = normalizar_cnpj(cnpj)
    if not num or len(num)!=14:
        return {"possui_divida": False, "total_registros": 0, "arquivos": []}
    cache_key = f"pgfn:{num}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    base = Config.PGFN_DIR
    if not base.exists():
        return {"possui_divida": False, "total_registros": 0, "arquivos": []}
    arquivos = list(base.glob('**/*.parquet'))
    matched_files = []
    total = 0
    for f in arquivos[:50]:
        try:
            pf = pq.ParquetFile(str(f))
            cols = [c.name for c in pf.schema]
            cand = next((c for c in cols if 'cnpj' in c.lower()), None)
            if not cand:
                continue
            q = f"SELECT COUNT(*) as n FROM read_parquet('{str(f).replace('\\','/')}') WHERE regexp_replace(cast({cand} as VARCHAR),'[^0-9]','') = '{num}'"
            n = duckdb.sql(q).to_df()['n'].iloc[0]
            if int(n)>0:
                matched_files.append(f.name)
                total += int(n)
        except Exception:
            continue
    result = {"possui_divida": total>0, "total_registros": total, "arquivos": matched_files}
    cache.set(cache_key, result, expire=3600)
    return result

def consultar_cnpj_simples_enriquecida(cnpj: str, light: bool = False) -> Optional[dict]:
    df_emp = consultar_cnpj_completo(cnpj)
    if df_emp is None or df_emp.empty:
        return None
    row = df_emp.iloc[0].to_dict()
    num = normalizar_cnpj(cnpj)
    est = None
    try:
        est = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[
                ('cnpj_basico','==', num[:8]),
                ('cnpj_ordem','==', num[8:12]),
                ('cnpj_dv','==', num[12:])
            ]
        )
    except Exception:
        est = None
    socios = None
    simples = None
    pgfn = None
    if not light:
        try:
            socios = pd.read_parquet(
                Config.ARQUIVOS_PARQUET['socios'],
                filters=[('cnpj_basico','==', row.get('cnpj_basico'))],
                columns=['nome_socio','qualificacao_socio','cnpj_basico']
            )
        except Exception:
            socios = None
        try:
            simples = pd.read_parquet(
                Config.ARQUIVOS_PARQUET['simples'],
                filters=[('cnpj_basico','==', row.get('cnpj_basico'))]
            )
        except Exception:
            simples = None
        pgfn = verificar_divida_pgfn(num)
    municipio_code = _get_field(est, ['codigo_municipio','municipio_codigo','cod_municipio','cod_municipio_ibge','codigo_municipio_ibge','municipio'])
    uf_val = _get_field(est, ['uf','uf_sigla'])
    cnae_code = _get_field(est, ['cnae_fiscal_principal','cnae_fiscal'])
    situacao = _get_field(est, ['situacao_cadastral','situacao'])
    logradouro = _get_field(est, ['logradouro','nome_logradouro','endereco','descricao_logradouro'])
    bairro = _get_field(est, ['bairro','bairro_nome','nome_bairro','bairro_descricao'])
    cep = _get_field(est, ['cep','cep_formatado','codigo_cep'])
    nome_fantasia = _get_field(est, ['nome_fantasia'])
    matriz_filial = _get_field(est, ['matriz_filial','ind_matriz','tipo_estabelecimento'])

    sec_candidates = ['cnaes_fiscais_secundarios','cnae_fiscal_secundaria','cnaes_secundarios','cnaes']
    sec_raw = None
    for c in sec_candidates:
        if est is not None and not est.empty and c in est.columns:
            sec_raw = est.iloc[0].get(c)
            if sec_raw:
                break
    def _parse_sec(val):
        try:
            if val is None:
                return []
            def norm_token(t):
                d = _digits(t)
                return d.zfill(7) if d else ''
            tokens = []
            if isinstance(val, list):
                tokens = [norm_token(x) for x in val]
            else:
                s = str(val)
                parts = [p.strip() for p in s.replace(';','|').replace(',','|').split('|') if p.strip()]
                tokens = [norm_token(p) for p in parts]
            tokens = [t for t in tokens if len(t)==7]
            seen = set(); out = []
            for t in tokens:
                if t not in seen:
                    seen.add(t); out.append(t)
            return out
        except Exception:
            return []
    cnaes_secundarios = _parse_sec(sec_raw)
    def _fmt_cep(v):
        try:
            d = _digits(v)
            return d.zfill(8) if d else None
        except Exception:
            return None
    if cep is not None:
        f = _fmt_cep(cep)
        cep = f if f else cep
    if not logradouro:
        tl = _get_field(est, ['tipo_logradouro','tipo_logradouro_nome','tipo_logr'])
        ln = _get_field(est, ['logradouro','nome_logradouro','endereco','descricao_logradouro'])
        nr = _get_field(est, ['numero','numero_logradouro','numero_endereco'])
        cp = _get_field(est, ['complemento','complemento_endereco'])
        parts = [str(x).strip() for x in [tl, ln, nr, cp] if x is not None and str(x).strip()!='']
        logradouro = (' '.join(parts) if parts else None)
    if not cnae_code:
        cnae_code = obter_cnae_principal_por_cnpj(num)
    if cnae_code:
        cnae_code = str(cnae_code).zfill(7)
    def _mf_nome(v):
        try:
            s = str(v).strip().upper()
            if s in ['1','MATRIZ','M']:
                return 'Matriz'
            if s in ['2','FILIAL','F']:
                return 'Filial'
            return None
        except Exception:
            return None
    matriz_filial_nome = _mf_nome(matriz_filial)
    cnaes_secundarios_info = []
    for code in cnaes_secundarios:
        try:
            cdesc = _cnae_desc(code)
            cnaes_secundarios_info.append({"codigo": str(code).zfill(7), "descricao": cdesc})
        except Exception:
            cnaes_secundarios_info.append({"codigo": str(code).zfill(7), "descricao": None})
    natureza_code = row.get('natureza_juridica')

    abertura_raw = _get_field(est, ['data_inicio_atividade','data_abertura','inicio_atividade','data_inicio'])
    if not abertura_raw:
        abertura_raw = _get_row_field(row, ['data_inicio_atividade','data_abertura','inicio_atividade','data_inicio'])
    abertura_date = _parse_date(abertura_raw)
    abertura_fmt = _format_date_br(abertura_date)
    tempo_anos = None
    try:
        if abertura_date is not None:
            from datetime import date
            diff_days = (date.today() - abertura_date).days
            tempo_anos = int(diff_days // 365)
    except Exception:
        tempo_anos = None

    simples_row = (simples.head(1).to_dict(orient='records')[0] if (simples is not None and not simples.empty) else None)
    simples_optante = None
    simples_situacao_nome = None
    if simples_row:
        for k in ['opcao_pelo_simples','optante','optante_simples','situacao']:
            v = simples_row.get(k)
            if v is not None:
                simples_optante = str(v).strip().upper() in ['S','SIM','OPTANTE','ATIVA']
                break
        for k2 in ['situacao','opcao_pelo_simples','optante','optante_simples']:
            vv = simples_row.get(k2)
            if vv is not None and str(vv).strip()!='':
                simples_situacao_nome = str(vv).strip()
                break

    return {
        "cnpj": num,
        "razao_social_nome_empresarial": row.get('razao_social_nome_empresarial') or row.get('nome_empresarial'),
        "porte_da_empresa": row.get('porte_da_empresa') or row.get('porte'),
        "natureza_juridica": natureza_code,
        "natureza_juridica_nome": _natureza_nome(natureza_code),
        "cnpj_basico": row.get('cnpj_basico'),
        "capital_social_da_empresa": row.get('capital_social_da_empresa') or row.get('capital_social'),
        "capital_social": row.get('capital_social_da_empresa') or row.get('capital_social'),
        "data_abertura": abertura_fmt,
        "tempo_atividade_anos": tempo_anos,
        "cnae_fiscal": cnae_code,
        "cnae_descricao": _cnae_desc(cnae_code),
        "cnaes_secundarios": cnaes_secundarios,
        "cnaes_secundarios_info": cnaes_secundarios_info,
        "municipio": municipio_code,
        "municipio_nome": _municipio_nome(municipio_code, uf_val),
        "uf": uf_val,
        "logradouro": logradouro,
        "bairro": bairro,
        "cep": cep,
        "nome_fantasia": nome_fantasia,
        "matriz_filial": matriz_filial,
        "matriz_filial_nome": matriz_filial_nome,
        "situacao_cadastral": situacao,
        "situacao_cadastral_nome": _situacao_nome(situacao),
        "qsa": (socios[['nome_socio','qualificacao_socio']].head(10).to_dict(orient='records') if (not light and socios is not None and not socios.empty) else []),
        "simples": (None if light else simples_row),
        "simples_optante": (None if light else simples_optante),
        "simples_situacao_nome": (None if light else simples_situacao_nome),
        "pgfn": (None if light else pgfn)
    }

@lru_cache(maxsize=1)
def _municipios_df():
    try:
        return pd.read_parquet(Config.ARQUIVOS_PARQUET['municipios'])
    except Exception:
        return pd.DataFrame()

def _municipio_nome(code, uf):
    try:
        df = _municipios_df()
        if df.empty or code is None:
            return None
        code_col = 'codigo' if 'codigo' in df.columns else df.columns[0]
        name_candidates = ['nome','municipio','descricao','nome_municipio']
        name_col = next((c for c in name_candidates if c in df.columns), None)
        if not name_col:
            name_col = next((c for c in df.columns if c.lower()!=code_col.lower() and df[c].dtype==object), None)
        ci = None
        try:
            ci = int(str(code))
        except Exception:
            return None
        row = df[df[code_col].astype(int)==ci]
        if not row.empty:
            return row.iloc[0][name_col]
        mod = ci % 10000
        cand = df[df[code_col].astype(int)%10000==mod]
        if not cand.empty:
            return cand.iloc[0][name_col]
        return None
    except Exception:
        return None

@lru_cache(maxsize=1)
def _cnaes_df():
    try:
        return pd.read_parquet(Config.ARQUIVOS_PARQUET['cnaes'])
    except Exception:
        return pd.DataFrame()

def _cnae_desc(code):
    try:
        if code is None:
            return None
        df = _cnaes_df()
        if df.empty:
            return None
        code_col = next((c for c in ['codigo','cnae','cnae_fiscal'] if c in df.columns), df.columns[0])
        name_col = next((c for c in ['descricao','nome','titulo'] if c in df.columns), None)
        if not name_col:
            name_col = next((c for c in df.columns if c.lower()!=code_col.lower() and df[c].dtype==object), None)
        val = str(code).zfill(7)
        row = df[df[code_col].astype(str).str.zfill(7)==val]
        if not row.empty:
            return row.iloc[0][name_col]
        return None
    except Exception:
        return None

def _natureza_nome(code):
    try:
        m = {
            '2062': 'Sociedade Empresária Limitada',
            '2135': 'EIRELI (Empresa Individual de Responsabilidade Ltda.)',
            '2305': 'Sociedade Anônima Fechada',
            '2263': 'Sociedade Simples Limitada',
            '1050': 'Empresário (Individual)',
            '2011': 'Sociedade Simples',
            '4014': 'Associação Privada',
            '3069': 'Fundação Privada'
        }
        return m.get(str(code), None)
    except Exception:
        return None

def _situacao_nome(code):
    try:
        m = {
            '01': 'Nula',
            '1': 'Nula',
            '02': 'Ativa',
            '2': 'Ativa',
            '03': 'Suspensa',
            '3': 'Suspensa',
            '04': 'Inapta',
            '4': 'Inapta',
            '05': 'Baixada',
            '5': 'Baixada'
        }
        return m.get(str(code), None)
    except Exception:
        return None

def buscar_por_palavra_chave(termo: str, uf: Optional[str] = None, municipio: Optional[str] = None, page: int = 1, page_size: int = 50) -> Optional[pd.DataFrame]:
    empresas = _read_parquet(Config.ARQUIVOS_PARQUET['empresas'])
    if empresas is None or empresas.empty:
        return None
    text_cols = [c for c in empresas.columns if empresas[c].dtype == object]
    if not text_cols:
        return None
    mask = False
    for c in text_cols:
        mask = mask | empresas[c].astype(str).str.contains(termo, case=False, na=False)
    if uf:
        uf_cols = [c for c in empresas.columns if c.lower() == 'uf']
        if uf_cols:
            mask = mask & (empresas[uf_cols[0]].astype(str).str.upper() == uf.upper())
    if municipio:
        mun_cols = [c for c in empresas.columns if 'municipio' in c.lower()]
        if mun_cols:
            mask = mask & empresas[mun_cols[0]].astype(str).str.contains(municipio, case=False, na=False)
    result = empresas[mask]
    if result.empty:
        return None
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return result.iloc[start:end]

def buscar_empresas_por_socio(nome_socio: str) -> Optional[pd.DataFrame]:
    # Tenta buscar com DuckDB diretamente no parquet (mais rápido em arquivos grandes)
    try:
        pf = pq.ParquetFile(str(Config.ARQUIVOS_PARQUET['socios']))
        cols = [c.name for c in pf.schema]
        cand = next((c for c in ['nome_socio','socio','nome'] if c in cols), None)
        qual_col = next((c for c in ['qualificacao_socio','qualificacao'] if c in cols), None)
        cnpj_col = next((c for c in ['cnpj_basico','cnpj'] if c in cols), None)
        if cand is None:
            raise ValueError('coluna de nome de sócio não encontrada')
        select_cols = ', '.join([c for c in [cnpj_col, cand + ' as nome_socio', qual_col] if c])
        q = (
            f"SELECT {select_cols} FROM read_parquet('{str(Config.ARQUIVOS_PARQUET['socios']).replace('\\','/')}') "
            f"WHERE lower({cand}) LIKE lower('%{nome_socio}%') LIMIT 500"
        )
        df = duckdb.sql(q).to_df()
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    # Fallback para pandas se DuckDB falhar
    socios = _read_parquet(Config.ARQUIVOS_PARQUET['socios'])
    if socios is None or socios.empty:
        return None
    cols = [c for c in socios.columns if 'socio' in c.lower() or 'nome' in c.lower()]
    if not cols:
        return None
    mask = False
    for c in cols:
        mask = mask | socios[c].astype(str).str.contains(nome_socio, case=False, na=False)
    result = socios[mask]
    if result.empty:
        return None
    return result
def _get_row_field(row: dict, candidates: list):
    try:
        for c in candidates:
            v = row.get(c)
            if v is not None and str(v).strip() != '':
                return v
        return None
    except Exception:
        return None

def _parse_date(val):
    try:
        s = str(val or '').strip()
        if not s:
            return None
        from datetime import datetime
        fmts = ['%Y-%m-%d', '%d/%m/%Y', '%Y%m%d', '%d-%m-%Y']
        for f in fmts:
            try:
                return datetime.strptime(s, f).date()
            except Exception:
                continue
        return None
    except Exception:
        return None

def _format_date_br(d):
    try:
        return d.strftime('%d/%m/%Y') if d else None
    except Exception:
        return None
