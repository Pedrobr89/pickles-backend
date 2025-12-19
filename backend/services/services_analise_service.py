"""
Serviços de análise setorial
"""
from services.services_cnpj_service import consultar_cnpj_completo, consultar_cnpj_simples_enriquecida, _municipios_df, _municipio_nome, obter_cnae_principal_por_cnpj, _cnae_desc, _situacao_nome
from services.services_cache_service import cache
from services.services_integracao_service import buscar_licitacoes_pncp, PNCPIntegration
from utils.utils_serializer import serializar_dataframe

import pandas as pd
import logging
from core.config import Config
from functools import lru_cache
from utils.utils_analise_utils import (
    calcular_indice_gini,
    calcular_hhi,
    calcular_entropia_shannon
)
from typing import Dict, List, Any
from utils.utils_validator import normalizar_cnpj
import duckdb
from datetime import datetime
from core.scoring_engine import ScoringEngine, CriterioScore, ResultadoScore
try:
    from cnae_mapping_config import get_cnae_targets, is_concorrente, get_potencial_score
except ImportError:
    def get_cnae_targets(cnae):
        return {'clientes_primarios': [], 'clientes_secundarios': []}
    def is_concorrente(cnae1, cnae2):
        return False
    def get_potencial_score(cnae1, cnae2):
        return ("Baixo", 0)

logger = logging.getLogger(__name__)


def _map_cnpj_enriquecido_to_scoring(data: Dict[str, Any]) -> Dict[str, Any]:
    num = normalizar_cnpj(data.get('cnpj',''))
    cnae = str(data.get('cnae_fiscal') or data.get('cnae_fiscal_principal') or '').zfill(7)
    desc = str(data.get('cnae_descricao') or '').strip()
    _cap_raw = data.get('capital_social_da_empresa') or data.get('capital_social') or 0
    capital = float(str(_cap_raw).replace('.', '').replace(',', '.') or 0)
    porte = str(data.get('porte_da_empresa') or data.get('porte') or '').strip()
    municipio = str(data.get('municipio_nome') or data.get('municipio') or '').strip().upper()
    uf = str(data.get('uf') or '').upper()
    def _to_int_safe(v):
        try:
            if isinstance(v, (int, float)):
                return int(v)
            s = str(v or '').strip()
            if not s:
                return 0
            s = s.replace('.', '').replace(',', '.')
            return int(float(s))
        except Exception:
            try:
                import re
                digits = ''.join(re.findall(r'\d+', str(v or '')))
                return int(digits) if digits else 0
            except Exception:
                return 0
    historico = _to_int_safe(data.get('historico_contratos'))
    tempo = _to_int_safe(data.get('tempo_atividade_anos'))
    certs = bool(data.get('certidoes_regulares', True))
    return {
        'cnpj': num,
        'cnae_fiscal': cnae,
        'cnae_descricao': desc,
        'cnaes_secundarios': data.get('cnaes_secundarios') or [],
        'capital_social': capital,
        'porte': porte,
        'tempo_atividade_anos': tempo,
        'historico_contratos': historico,
        'certidoes_regulares': certs,
        'municipio': municipio,
        'uf': uf,
    }

def _parse_float_br(val: Any) -> float:
    try:
        if val is None:
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if not s:
            return 0.0
        s = s.replace('.', '').replace(',', '.')
        return float(s)
    except Exception:
        return 0.0

def carregar_dataframe_empresas_filtrado(filtros_parquet: List[tuple]) -> pd.DataFrame:
    try:
        from pyarrow import parquet as pq
        fp = str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')
        pf = pq.ParquetFile(fp)
        cols = [c.name for c in pf.schema]
        def pick(cands, default=None):
            return next((c for c in cands if c in cols), default)
        basico = pick(['cnpj_basico','CNPJ_BASICO','cnpjBasico'])
        ordem = pick(['cnpj_ordem','CNPJ_ORDEM','cnpjOrdem'])
        dv = pick(['cnpj_dv','CNPJ_DV','cnpjDV'])
        uf_col = pick(['uf','UF','sigla_uf'])
        mun_col = pick(['municipio','MUNICIPIO','nome_municipio','municipio_nome'])
        cnae_col = pick(['cnae_fiscal_principal','cnae_fiscal','CNAE_FISCAL'])
        nome_fantasia = pick(['nome_fantasia','NOME_FANTASIA'])
        sit_col = pick(['situacao_cadastral','SITUACAO_CADASTRAL'])
        d1 = pick(['ddd_1','DDD_1'])
        t1 = pick(['telefone_1','TELEFONE_1'])
        email_col = pick(['correio_eletronico','email','EMAIL'])
        log_col = pick(['logradouro','LOGRADOURO'])
        num_col = pick(['numero','NUMERO'])
        bairro_col = pick(['bairro','BAIRRO'])
        cep_col = pick(['cep','CEP'])

        select_cols = [x for x in [basico,ordem,dv,nome_fantasia,uf_col,mun_col,cnae_col,sit_col,d1,t1,email_col,log_col,num_col,bairro_col,cep_col] if x]
        where = []
        for col, op, val in filtros_parquet or []:
            if col not in cols:
                continue
            c = str(col)
            if op == 'in' and isinstance(val, (list, tuple, set)):
                vals = ",".join([f"'{str(x)}'" for x in val])
                where.append(f"CAST({c} AS VARCHAR) IN ({vals})")
            elif op == '==':
                where.append(f"CAST({c} AS VARCHAR) = '{str(val)}'")
            elif op == '>=':
                where.append(f"CAST({c} AS DOUBLE) >= {float(val)}")
            elif op == '<=':
                where.append(f"CAST({c} AS DOUBLE) <= {float(val)}")
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        q = (
            f"SELECT {', '.join(select_cols)} FROM read_parquet('{fp}') USING SAMPLE 100000 ROWS" +
            f"{where_sql}"
        )
        df_est = duckdb.sql(q).to_df()
        if df_est is None or df_est.empty:
            return pd.DataFrame()
        df_est = df_est.rename(columns={
            basico: 'cnpj_basico', ordem: 'cnpj_ordem', dv: 'cnpj_dv',
            uf_col: 'uf', mun_col: 'municipio', cnae_col: 'cnae_fiscal_principal',
            nome_fantasia: 'nome_fantasia', sit_col: 'situacao_cadastral',
            d1: 'ddd_1', t1: 'telefone_1', email_col: 'correio_eletronico',
            log_col: 'logradouro', num_col: 'numero', bairro_col: 'bairro', cep_col: 'cep'
        })

        basicos = df_est['cnpj_basico'].astype(str).unique().tolist()
        df_emp = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico','in', basicos)],
            columns=None
        )
        # Detecta coluna de razão social
        if df_emp is not None and not df_emp.empty:
            rs_col = next((c for c in ['razao_social_nome_empresarial','razao_social','nome_empresarial'] if c in df_emp.columns), df_emp.columns[0])
            cap_col = next((c for c in ['capital_social_da_empresa','capital_social'] if c in df_emp.columns), None)
            porte_col = next((c for c in ['porte_da_empresa','porte'] if c in df_emp.columns), None)
            df_emp = df_emp[[c for c in ['cnpj_basico', rs_col, cap_col, porte_col] if c]]
            df_emp = df_emp.rename(columns={rs_col: 'razao_social_nome_empresarial', cap_col: 'capital_social_da_empresa', porte_col: 'porte_da_empresa'})
        df = pd.merge(df_est, df_emp, on='cnpj_basico', how='left')
        df['cnpj'] = df['cnpj_basico'].astype(str) + df['cnpj_ordem'].astype(str) + df['cnpj_dv'].astype(str)
        df['razao_social'] = df['razao_social_nome_empresarial']
        df['email'] = df.get('correio_eletronico') if 'correio_eletronico' in df.columns else None
        # Normaliza capital social para número
        if 'capital_social_da_empresa' in df.columns:
            df['capital_social_da_empresa'] = df['capital_social_da_empresa'].apply(_parse_float_br)
        try:
            df['cnae_fiscal_descricao'] = df['cnae_fiscal_principal'].apply(_cnae_desc)
        except Exception:
            df['cnae_fiscal_descricao'] = None
        try:
            df['situacao_cadastral_nome'] = df['situacao_cadastral'].apply(_situacao_nome)
        except Exception:
            pass
        return df
    except Exception:
        return pd.DataFrame()

def scoring_compatibilidade(cnpj: str, edital: Dict[str, Any]) -> Dict[str, Any]:
    info = consultar_cnpj_simples_enriquecida(cnpj)
    if not info:
        return {'erro':'CNPJ não encontrado'}
    cnpj_data = _map_cnpj_enriquecido_to_scoring(info)
    edital_data = dict(edital or {})
    engine = ScoringEngine()
    result = engine.calcular_score(cnpj_data, edital_data)
    return getattr(result, '__dict__', result)

def scoring_ranking(cnpj: str, filtros: Dict[str, Any], limite: int = 50) -> Dict[str, Any]:
    info = consultar_cnpj_simples_enriquecida(cnpj)
    if not info:
        return {'erro':'CNPJ não encontrado'}
    cnpj_data = _map_cnpj_enriquecido_to_scoring(info)
    cliente = PNCPIntegration()
    page = int(filtros.get('pagina') or 1); size = int(filtros.get('tamanhoPagina') or 50)
    resp = cliente.listar_editais(pagina=page, tamanho=size, filtros=filtros)
    items = resp.get('data') or []
    engine = ScoringEngine(); resultados = []
    for it in items:
        try:
            r = engine.calcular_score(cnpj_data, it)
            resultados.append(getattr(r, '__dict__', r))
        except Exception:
            continue
    resultados.sort(key=lambda x: x['score_total'], reverse=True)
    top = resultados[:limite]
    media = (sum(r['score_total'] for r in resultados)/len(resultados)) if resultados else 0
    return {
        'cnpj': cnpj_data.get('cnpj'),
        'total_analisados': len(resultados),
        'top_oportunidades': top,
        'estatisticas': {
            'score_medio': media,
            'alta_compatibilidade': len([r for r in resultados if r['score_total'] >= 80]),
            'media_compatibilidade': len([r for r in resultados if 60 <= r['score_total'] < 80]),
            'baixa_compatibilidade': len([r for r in resultados if r['score_total'] < 60])
        }
    }




def ranking_empresas_por_prestador(cnpj_prestador: str, filtros: Dict[str, Any], limite: int = 50) -> Dict[str, Any]:
    """
    Gera ranking de empresas (leads) mais compatíveis - VERSÃO CORRIGIDA
    """
    try:
        print(f"[INFO] 🔍 Iniciando busca de leads para {cnpj_prestador}")
        
        # ETAPA 1: INFORMAÇÕES DO PRESTADOR
        info_prestador = consultar_cnpj_simples_enriquecida(cnpj_prestador)
        if not info_prestador:
            return {'erro': 'CNPJ do prestador não encontrado', 'resultados': [], 'total': 0}
        
        prestador_data = _map_cnpj_enriquecido_to_scoring(info_prestador)
        cnae_prestador = str(prestador_data.get('cnae_fiscal', '')).zfill(7)
        cnae_prest_2dig = cnae_prestador[:2]
        cnae_prest_4dig = cnae_prestador[:4]
        
        print(f"[INFO] 📋 CNAE prestador: {cnae_prestador} | Setor: {cnae_prest_2dig}")
        
        # ETAPA 2: CNAES-ALVO COM FALLBACK
        targets = get_cnae_targets(cnae_prestador)
        cnaes_primarios = targets.get('clientes_primarios', [])
        cnaes_secundarios = targets.get('clientes_secundarios', [])
        
        print(f"[INFO] 🎯 CNAEs primários: {len(cnaes_primarios)} | Secundários: {len(cnaes_secundarios)}")
        
        # ETAPA 3: FILTROS BASE (SEM CNAE POR DEFAULT)
        filtros_parquet = []
        
        # Filtros geográficos e porte (OBRIGATÓRIOS)
        if filtros.get('uf'):
            filtros_parquet.append(('uf', '==', filtros['uf'].upper()))
        if filtros.get('municipio'):
            filtros_parquet.append(('municipio', '==', filtros['municipio'].upper()))
        if filtros.get('porte'):
            p_map = {'MEI': '1', 'Microempresa': '01', 'Pequeno Porte': '03', 'Médio Porte': '05', 'Grande Porte': '05'}
            p_val = p_map.get(filtros['porte']) or filtros['porte']
            # Tenta filtrar por código se mapeado, senão tenta valor literal
            # Parquet usually has codes. If map fails, user might have sent raw code.
            # However, 'carregar_dataframe_empresas_filtrado' handles simple equality.
            # To sustain 'IN' logic for Porte groups, specialized logic in 'carregar_dataframe_empresas_filtrado' might be better,
            # but here we stick to simple equality for now or pass list.
            if filtros['porte'] in ['Microempresa','01']:
                 filtros_parquet.append(('porte_da_empresa', 'in', ['01','1']))
            elif filtros['porte'] in ['Pequeno Porte','03']:
                 filtros_parquet.append(('porte_da_empresa', 'in', ['03','3']))
            elif filtros['porte'] in ['Médio Porte','Grande Porte','05']:
                 filtros_parquet.append(('porte_da_empresa', 'in', ['05','5']))
            else:
                 filtros_parquet.append(('porte_da_empresa', '==', p_val))

        # Filtro de Situação
        sit = filtros.get('situacao', '').lower()
        if sit == 'ativa' or not sit: # Default to active if not specified
             filtros_parquet.append(('situacao_cadastral', '==', '02'))
        # Other situations might require mapping codes

        # Filtro de Idade (Data de Início)
        idade_range = filtros.get('idade')
        if idade_range:
            import time
            current_year = int(time.strftime("%Y"))
            def date_int(years_ago): return int(f"{current_year - years_ago}0000")
            
            if idade_range == '0-2':
                filtros_parquet.append(('data_de_inicio_atividade', '>=', date_int(2)))
            elif idade_range == '2-5':
                filtros_parquet.append(('data_de_inicio_atividade', '>=', date_int(5)))
                filtros_parquet.append(('data_de_inicio_atividade', '<', date_int(2)))
            elif idade_range == '5-10':
                filtros_parquet.append(('data_de_inicio_atividade', '>=', date_int(10)))
                filtros_parquet.append(('data_de_inicio_atividade', '<', date_int(5)))
            elif idade_range == '10+':
                filtros_parquet.append(('data_de_inicio_atividade', '<', date_int(10)))

        # Filtros de capital
        if filtros.get('capital_min'):
            filtros_parquet.append(('capital_social_da_empresa', '>=', float(filtros['capital_min'])))
        if filtros.get('capital_max'):
            filtros_parquet.append(('capital_social_da_empresa', '<=', float(filtros['capital_max'])))
        
        print(f"[INFO] 🧹 Filtros base: {len(filtros_parquet)} filtros")
        
        # ETAPA 4: CARREGAR EMPRESAS (SEM LIMIT RESTRIÇÃO CNAE)
        print("[INFO] 📊 Carregando empresas...")
        df = carregar_dataframe_empresas_filtrado(filtros_parquet)
        
        if df.empty:
            print("[❌] Nenhuma empresa encontrada - EXPANDINDO BUSCA")
            # FALLBACK: Remove filtros mais restritivos e tenta novamente
            df = carregar_dataframe_empresas_filtrado([
                ('situacao_cadastral', '==', '02')  # Apenas ativas
            ])
        
        if df.empty:
            print('[WARNING] Nenhuma empresa encontrada com os filtros — ampliando escopo real')
            df = carregar_dataframe_empresas_filtrado([])
            if df.empty:
                return {
                    'resultados': [],
                    'total': 0,
                    'pagina': 1,
                    'limite': limite,
                    'mensagem': 'Nenhuma empresa encontrada nos arquivos reais',
                    'debug': {'cnae_prestador': cnae_prestador}
                }
        
        print(f"[✅] Empresas carregadas: {len(df):,}")
        
        # ETAPA 5: FILTRAGEM CNAE FLEXÍVEL (OPCIONAL)
        df_filtrado = df.copy()
        empresas_com_cnae_alvo = 0
        
        # Prioriza CNAEs-alvo SE HÁ MAPEAMENTO
        if cnaes_primarios or cnaes_secundarios:
            cnaes_formatados = []
            for cnae in set(cnaes_primarios + cnaes_secundarios):
                if len(str(cnae)) == 4:
                    cnaes_formatados.extend([cnae + '000', cnae + '100', cnae + '200'])
                else:
                    cnaes_formatados.append(str(cnae).zfill(7))
            
            if cnaes_formatados:
                mask_cnae = df_filtrado['cnae_fiscal_principal'].astype(str).isin(cnaes_formatados)
                empresas_com_cnae_alvo = mask_cnae.sum()
                df_filtrado = df_filtrado[mask_cnae]
                print(f"[INFO] 🎯 Empresas com CNAE-alvo: {empresas_com_cnae_alvo}")
        
        # ETAPA 6: EXCLUIR CONCORRENTES (MENOS RESTRITIVO)
        if len(df_filtrado) > 100:  # Só exclui concorrentes se há muitas empresas
            df_original_len = len(df_filtrado)
            df_filtrado = df_filtrado[
                ~df_filtrado['cnae_fiscal_principal'].astype(str).str[:2].eq(cnae_prest_2dig)
            ]
            concorrentes_removidos = df_original_len - len(df_filtrado)
            print(f"[INFO] 🚫 Concorrentes removidos: {concorrentes_removidos}")
        else:
            concorrentes_removidos = 0
        
        total_filtrado = len(df_filtrado)
        print(f"[✅] Empresas após filtros: {total_filtrado:,}")
        
        if total_filtrado == 0:
            return {
                'resultados': [],
                'total': 0,
                'mensagem': 'Nenhuma empresa não-concorrente encontrada',
                'debug': {
                    'total_carregadas': len(df),
                    'com_cnae_alvo': empresas_com_cnae_alvo,
                    'concorrentes_removidos': concorrentes_removidos
                }
            }
        
        # ETAPA 7: ORDENAÇÃO E DEFINIÇÃO DO CONJUNTO A PROCESSAR
        pagina = max(1, int(filtros.get('pagina', 1)))
        inicio = (pagina - 1) * limite
        total_empresas = total_filtrado
        df_processar = df_filtrado.sort_values('capital_social_da_empresa', ascending=False)
        processar_todas = True
        max_processar = int(filtros.get('max_processar', 100))
        df_iter = df_processar
        if len(df_iter) > max_processar:
            print(f"[WARN] Volume alto ({len(df_iter)}) – limitando processamento a {max_processar} registros")
            df_iter = df_iter.head(max_processar)
        print(f"[INFO] 📄 Processando {len(df_iter)} empresas (total filtradas: {total_empresas})")
        
        # ETAPA 8: CALCULAR SCORES
        engine = ScoringEngine()
        resultados = []
        
        for idx, row in df_iter.iterrows():
            try:
                empresa_data = {
                    'cnpj': str(row.get('cnpj', '')),
                    'razao_social': str(row.get('razao_social_nome_empresarial', '')),
                    'nome_fantasia': str(row.get('nome_fantasia', '')),
                    'cnae_fiscal_principal': str(row.get('cnae_fiscal_principal', '')).zfill(7),
                    'cnae_descricao': str(row.get('cnae_fiscal_descricao', '')),
                    'uf': str(row.get('uf', '')),
                    'municipio': str(row.get('municipio', '')),
                    'porte_da_empresa': str(row.get('porte_da_empresa', '')),
                    'capital_social_da_empresa': _parse_float_br(row.get('capital_social_da_empresa', 0)),
                    'data_de_inicio_atividade': str(row.get('data_de_inicio_atividade', '')),
                    'email': str(row.get('email', '')),
                    'telefone': str(row.get('telefone_1', '')),
                }
                
                # Edital simulado melhorado
                edital_simulado = {
                    'cnae_relacionado': cnae_prestador,
                    'uf': empresa_data['uf'],
                    'municipio': empresa_data['municipio'],
                    'valorEstimado': float(empresa_data['capital_social_da_empresa']) * 0.05,
                    'porte_preferencial': [empresa_data['porte_da_empresa']],
                    'exige_experiencia': False,
                    'exige_certidoes': False,
                    'palavras_chave': []
                }
                
                score_result = engine.calcular_score(prestador_data, edital_simulado)
                score_base = getattr(score_result, 'score_total', 0)
                potencial_texto, bonus_score = get_potencial_score(cnae_prestador, empresa_data['cnae_fiscal_principal'])
                score_final = min(100, score_base + bonus_score)
                
                resultado = {
                    'empresa': empresa_data,
                    'compatibilidade': round(score_final, 1),
                    'potencial': potencial_texto,
                    'score_base': round(score_base, 1),
                    'bonus_potencial': bonus_score,
                    'match_cnae_alvo': empresa_data['cnae_fiscal_principal'][:4] in cnaes_primarios + cnaes_secundarios
                }
                resultados.append(resultado)
                
            except Exception as e:
                print(f"[⚠️] Skip {row.get('cnpj')}: {str(e)[:50]}")
                continue
        
        # ETAPA 9: RESULTADO FINAL
        resultados_ordenados = sorted(resultados, key=lambda x: (x['compatibilidade'], x['empresa']['capital_social_da_empresa']), reverse=True)
        top_n = resultados_ordenados[:limite]
        print(f"[✅] 🎉 {len(resultados_ordenados)} LEADS GERADOS! | Top retornado: {len(top_n)}")
        media_score = round(sum(x.get('compatibilidade', 0) for x in top_n) / len(top_n), 1) if top_n else 0
        max_score = max([x.get('compatibilidade', 0) for x in top_n]) if top_n else 0
        dist_uf = {}
        for x in top_n:
            ufv = str(x.get('empresa', {}).get('uf', '') or '')
            dist_uf[ufv] = (dist_uf.get(ufv, 0) + 1)
        top_uf = sorted(dist_uf.items(), key=lambda kv: kv[1], reverse=True)[0][0] if dist_uf else '—'
        pot_counts = {'Alto': 0, 'Médio': 0, 'Baixo': 0}
        for x in top_n:
            p = str(x.get('potencial') or '')
            if p in pot_counts:
                pot_counts[p] += 1
        sel_uf = str(filtros.get('uf', '') or '')
        sel_mun = str(filtros.get('municipio', '') or '')
        relatorio = (
            f"Levantamento de leads com base no CNAE do prestador {cnae_prestador}. "
            f"Foram processadas {len(df_iter)} empresas não-concorrentes e retornados {len(top_n)} leads. "
            f"Score médio {media_score} e máximo {max_score}. "
            f"{('Filtro geográfico aplicado: UF ' + sel_uf) if sel_uf else ''}"
            f"{(', Município ' + sel_mun) if sel_mun else ''}. "
            f"CNAEs-alvo priorizados quando disponíveis, com bônus de potencial. "
            f"Top UF: {top_uf}. Distribuição de potencial: Alto {pot_counts['Alto']}, Médio {pot_counts['Médio']}, Baixo {pot_counts['Baixo']}."
        ).strip()
        
        return {
            'resultados': top_n,
            'total': total_empresas,
            'pagina': pagina,
            'limite': limite,
            'total_paginas': (total_empresas + limite - 1) // limite,
            'estatisticas': {
                'total_carregadas': len(df),
                'com_cnae_alvo': empresas_com_cnae_alvo,
                'concorrentes_removidos': concorrentes_removidos,
                'leads_gerados': len(resultados_ordenados),
                'processadas': len(df_iter),
                'cnae_prestador': cnae_prestador
            },
            'dashboard': {
                'total_leads': len(top_n),
                'media_score': media_score,
                'max_score': max_score,
                'top_uf': top_uf,
                'potencial_alto': pot_counts['Alto'],
                'potencial_medio': pot_counts['Médio'],
                'potencial_baixo': pot_counts['Baixo']
            },
            'relatorio_executivo': relatorio
        }
        
    except Exception as e:
        print(f"[FATAL] 💥 Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'erro': str(e), 'resultados': [], 'total': 0}


# Mapeamento de CNAE (2 primeiros dígitos) para Macro Setores
SECTOR_MAPPING = {
    'Agropecuária': list(range(1, 4)),  # 01-03
    'Indústria': list(range(5, 34)),    # 05-33
    'Serviços': list(range(35, 40)) + list(range(49, 86)) + list(range(89, 100)), # 35-39 + 49-85 + 89-99
    'Construção': list(range(41, 44)),  # 41-43
    'Comércio': list(range(45, 48)),    # 45-47
    'Saúde': list(range(86, 89)),       # 86-88 (Saúde humana e assistência social)
}

# Inverte o mapeamento para busca rápida: '01' -> 'Agropecuária'
PREFIX_TO_SECTOR = {}
for sector, prefixes in SECTOR_MAPPING.items():
    for p in prefixes:
        PREFIX_TO_SECTOR[f"{p:02d}"] = sector

def get_setores_economia():
    """Retorna lista de setores da economia disponíveis."""
    return list(SECTOR_MAPPING.keys())

def sugerir_cnaes(termo: str, setor_filtro: str = None):
    """
    Sugere CNAEs baseado em termo de busca (por número ou descrição).
    Opcionalmente filtra por setor da economia.
    """
    try:
        try:
            from unidecode import unidecode
        except Exception:
            unidecode = None
        from services_cache_service import cache

        df_cnaes = cache.get('cnaes')
        if df_cnaes is None:
            df_cnaes = pd.read_parquet(Config.ARQUIVOS_PARQUET['cnaes'])
            
            # Enriquece o cache com a coluna de setor se ainda não tiver
            if 'setor' not in df_cnaes.columns:
                def _get_sector(codigo):
                    cod_str = str(codigo).zfill(7)
                    prefix = cod_str[:2]
                    return PREFIX_TO_SECTOR.get(prefix, 'Outros')
                df_cnaes['setor'] = df_cnaes['codigo'].apply(_get_sector)
            
            cache.set('cnaes', df_cnaes, expire=Config.CACHE_DEFAULT_TIMEOUT)

        if df_cnaes is None or df_cnaes.empty:
            logger.warning("Tabela auxiliar de CNAEs não carregada")
            return []

        if 'descricao' not in df_cnaes.columns:
            logger.error("Tabela de CNAEs não contém a coluna 'descricao'")
            return []

        # Garante que a coluna setor exista no DataFrame recuperado do cache
        if 'setor' not in df_cnaes.columns:
             def _get_sector(codigo):
                cod_str = str(codigo).zfill(7)
                prefix = cod_str[:2]
                return PREFIX_TO_SECTOR.get(prefix, 'Outros')
             df_cnaes['setor'] = df_cnaes['codigo'].apply(_get_sector)

        # Corrige possíveis problemas de encoding (mojibake)
        try:
            sample = df_cnaes['descricao'].astype(str).head(5).str.cat(sep=' ')
            if 'Ã' in sample or '\ufffd' in sample:
                def _fix_text(s):
                    try:
                        return str(s).encode('latin1').decode('utf-8')
                    except Exception:
                        return str(s)
                df_cnaes['descricao'] = df_cnaes['descricao'].astype(str).apply(_fix_text)
        except Exception:
            pass

        def _norm(s: str) -> str:
            s2 = str(s or '').strip().upper()
            return unidecode(s2) if unidecode else s2

        termo_str = str(termo or '').strip()
        termo_normalizado = _norm(termo_str)
        termo_digitos = ''.join(ch for ch in termo_str if ch.isdigit())

        df = df_cnaes.copy()
        
        # Filtro de Setor
        if setor_filtro and setor_filtro != 'Todos':
            df = df[df['setor'] == setor_filtro]

        df['descricao_normalizada'] = df['descricao'].astype(str).apply(_norm)
        df['codigo_str'] = df['codigo'].astype(str).str.zfill(7)

        resultados = pd.DataFrame()
        if termo_digitos:
            resultados = df[df['codigo_str'].str.startswith(termo_digitos)]
        else:
            resultados = df[df['descricao_normalizada'].str.contains(termo_normalizado, na=False)]

        if resultados is None or resultados.empty:
            return []

        return resultados[['codigo', 'descricao', 'setor']].head(50).to_dict(orient='records')

    except Exception as e:
        logger.error(f"Erro ao sugerir CNAEs para '{termo}': {e}", exc_info=True)
        return []

def executar_analise_setorial(cnae_codes=None, termo_busca=None, uf=None,
                              municipio=None, somente_ativas=False,
                              ano_inicio_min=None, ano_inicio_max=None,
                              limit: int = 20):
    """
    Executa análise setorial completa
    """
    try:
        cache_key = (
            f"setorial:cnaes:{','.join(map(str, cnae_codes or []))}:termo:{str(termo_busca or '').strip().lower()}"
            f":uf:{','.join(map(lambda x: str(x).upper(), uf if isinstance(uf,(list,tuple,set)) else ([uf] if uf else [])))}"
            f":mun:{str(municipio or '').upper()}:ativas:{int(bool(somente_ativas))}:ai:{ano_inicio_min}:af:{ano_inicio_max}:lim:{int(limit or 20)}"
        )
        try:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
        except Exception as e:
            logger.warning(f"Erro ao acessar cache: {e}")
        # Carrega dados de estabelecimentos
        filtros = []
        if cnae_codes:
            filtros.append(('cnae_fiscal_principal', 'in', cnae_codes))

        cols_estab = [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'nome_fantasia', 'uf',
            'municipio', 'data_de_inicio_atividade', 'situacao_cadastral',
            'cnae_fiscal_principal',
            'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep',
            'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'correio_eletronico'
        ]
        use_duck = True
        df_estabelecimentos = None
        if use_duck:
            try:
                where = []
                if cnae_codes:
                    cvals = ",".join([f"'{str(c)}'" for c in cnae_codes])
                    where.append(f"CAST(cnae_fiscal_principal AS VARCHAR) IN ({cvals})")
                if uf:
                    if isinstance(uf, (list, tuple, set)):
                        uvals = ",".join([f"'{str(x).upper()}'" for x in uf])
                        where.append(f"uf IN ({uvals})")
                    else:
                        where.append(f"uf = '{str(uf).upper()}'")
                if municipio:
                    where.append(f"CAST(municipio AS VARCHAR) = '{str(municipio).upper()}'")
                if somente_ativas:
                    where.append("situacao_cadastral = '02'")
                if ano_inicio_min or ano_inicio_max:
                    year_expr = "EXTRACT(YEAR FROM STRPTIME(CAST(data_de_inicio_atividade AS VARCHAR), '%Y%m%d'))"
                    if ano_inicio_min:
                        where.append(f"{year_expr} >= {int(ano_inicio_min)}")
                    if ano_inicio_max:
                        where.append(f"{year_expr} <= {int(ano_inicio_max)}")
                where_sql = (" WHERE " + " AND ".join(where)) if where else ""
                sel_cols = ",".join(cols_estab)
                q = (
                    f"SELECT {sel_cols} FROM parquet_scan('{str(Config.ARQUIVOS_PARQUET['estabelecimentos']).replace('\\','/')}')"
                    f"{where_sql} LIMIT 20000"
                )
                df_estabelecimentos = duckdb.sql(q).to_df()
            except Exception:
                df_estabelecimentos = None
        if df_estabelecimentos is None:
            df_estabelecimentos = pd.read_parquet(
                Config.ARQUIVOS_PARQUET['estabelecimentos'],
                filters=filtros if filtros else None,
                columns=cols_estab
            )

        if df_estabelecimentos.empty:
            return None

        # Aplica filtros adicionais
        if uf:
            try:
                if isinstance(uf, (list, tuple, set)):
                    uf_list = [str(x).upper() for x in uf]
                    df_estabelecimentos = df_estabelecimentos[df_estabelecimentos['uf'].isin(uf_list)]
                else:
                    df_estabelecimentos = df_estabelecimentos[df_estabelecimentos['uf'] == str(uf).upper()]
            except Exception:
                df_estabelecimentos = df_estabelecimentos[df_estabelecimentos['uf'] == str(uf).upper()]

        if municipio:
            df_estabelecimentos = df_estabelecimentos[
                df_estabelecimentos['municipio'].astype(str).str.upper() == str(municipio).upper()
            ]

        if somente_ativas:
            df_estabelecimentos = df_estabelecimentos[
                df_estabelecimentos['situacao_cadastral'] == '02'
            ]

        # Adiciona coluna de ano de início apenas quando necessário
        if ano_inicio_min or ano_inicio_max:
            df_estabelecimentos['ano_inicio'] = pd.to_datetime(
                df_estabelecimentos['data_de_inicio_atividade'].astype(str),
                format='%Y%m%d',
                errors='coerce'
            ).dt.year

        # Aplica filtros de ano
        if ano_inicio_min:
            df_estabelecimentos = df_estabelecimentos[
                df_estabelecimentos['ano_inicio'] >= int(ano_inicio_min)
            ]

        if ano_inicio_max:
            df_estabelecimentos = df_estabelecimentos[
                df_estabelecimentos['ano_inicio'] <= int(ano_inicio_max)
            ]

        # Aplica limitação de linhas para estabilizar processamento
        try:
            base_limit = max(20, int(limit or 20))
            max_rows = min(20000, base_limit * 500)
            if len(df_estabelecimentos) > max_rows:
                df_estabelecimentos = df_estabelecimentos.head(max_rows)
        except Exception:
            pass

        # Obtém CNPJs básicos únicos
        cnpjs_basicos = df_estabelecimentos['cnpj_basico'].astype(str).unique().tolist()

        # Carrega dados das empresas
        df_empresas = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=[
                'cnpj_basico', 'razao_social_nome_empresarial',
                'natureza_juridica', 'capital_social_da_empresa',
                'porte_da_empresa'
            ]
        )

        # Carrega dados dos sócios
        df_socios = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['socios'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=['cnpj_basico','nome_socio','cpf_cnpj_do_socio','qualificacao_socio']
        )

        # Executa análise
        from analise_setorial import analisar_dados_setoriais
        resultados_analise = analisar_dados_setoriais(
            df_estabelecimentos, df_empresas, df_socios
        )

        df_lista = pd.merge(df_estabelecimentos, df_empresas, on='cnpj_basico', how='left')

        try:
            df_tmp = df_socios.copy()
            if 'qualificacao_socio' in df_tmp.columns:
                q = df_tmp['qualificacao_socio'].astype(str).str.upper()
                df_tmp['__is_admin'] = q.str.contains('ADMIN') | q.str.contains('DIRETOR') | q.str.contains('PRESIDENTE') | q.str.contains('GERENTE')
            else:
                df_tmp['__is_admin'] = False
            df_tmp['__ord'] = (~df_tmp['__is_admin']).astype(int)
            df_tmp = df_tmp.sort_values(['cnpj_basico','__ord']).drop_duplicates(['cnpj_basico'])
            cols_merge = ['cnpj_basico']
            if 'nome_socio' in df_tmp.columns:
                cols_merge.append('nome_socio')
            if 'cpf_cnpj_do_socio' in df_tmp.columns:
                cols_merge.append('cpf_cnpj_do_socio')
            df_resp = df_tmp[cols_merge]
            df_lista = pd.merge(df_lista, df_resp, on='cnpj_basico', how='left')
            if 'nome_socio' in df_lista.columns:
                df_lista.rename(columns={'nome_socio': 'responsavel'}, inplace=True)
            if 'cpf_cnpj_do_socio' in df_lista.columns:
                df_lista.rename(columns={'cpf_cnpj_do_socio': 'responsavel_documento'}, inplace=True)
            if 'responsavel_documento' in df_lista.columns:
                df_lista['responsavel_documento'] = df_lista['responsavel_documento'].astype(str).str.replace(r'\D','', regex=True)
        except Exception:
            df_lista['responsavel'] = None

        try:
            def fmt_cep(v):
                try:
                    s = ''.join([c for c in str(v) if c.isdigit()])
                    return f"{s[:5]}-{s[5:8]}" if len(s)==8 else None
                except Exception:
                    return None

            tipo = df_lista['tipo_de_logradouro'].astype(str) if 'tipo_de_logradouro' in df_lista.columns else None
            logradouro = df_lista['logradouro'].astype(str) if 'logradouro' in df_lista.columns else None
            numero = df_lista['numero'].astype(str) if 'numero' in df_lista.columns else None
            complemento = df_lista['complemento'].astype(str) if 'complemento' in df_lista.columns else None
            bairro = df_lista['bairro'].astype(str) if 'bairro' in df_lista.columns else None
            cep = df_lista['cep'].apply(fmt_cep) if 'cep' in df_lista.columns else None

            partes = []
            if tipo is not None and logradouro is not None:
                partes.append((tipo.fillna('') + ' ' + logradouro.fillna('')).str.strip())
            elif logradouro is not None:
                partes.append(logradouro.fillna(''))
            if numero is not None:
                partes.append(('Nº ' + numero.fillna('')).str.strip())
            if complemento is not None:
                partes.append(complemento.fillna(''))
            if bairro is not None:
                partes.append(bairro.fillna(''))
            if cep is not None:
                partes.append(pd.Series(cep).fillna(''))

            if partes:
                end = partes[0]
                for p in partes[1:]:
                    end = (end + ' - ' + p).str.replace(r'\s+-\s+-', ' - ', regex=True)
                df_lista['endereco'] = end.str.strip().replace({'': None})
            else:
                df_lista['endereco'] = None
        except Exception:
            df_lista['endereco'] = None

        try:
            tel1 = df_lista['telefone_1'].astype(str) if 'telefone_1' in df_lista.columns else None
            ddd1 = df_lista['ddd_1'].astype(str) if 'ddd_1' in df_lista.columns else None
            tel2 = df_lista['telefone_2'].astype(str) if 'telefone_2' in df_lista.columns else None
            ddd2 = df_lista['ddd_2'].astype(str) if 'ddd_2' in df_lista.columns else None

            def join_tel(ddd, tel):
                if ddd is None or tel is None:
                    return None
                return (('(' + ddd.fillna('') + ') ' + tel.fillna('')).str.strip().replace({'() ': ''}))

            t1 = join_tel(ddd1, tel1)
            t2 = join_tel(ddd2, tel2)
            if t1 is not None:
                df_lista['telefone'] = t1.where(t1.notna(), t2 if t2 is not None else None)
            elif t2 is not None:
                df_lista['telefone'] = t2
            else:
                df_lista['telefone'] = None

            if 'correio_eletronico' in df_lista.columns:
                df_lista['email'] = df_lista['correio_eletronico']
            else:
                email_col = next((c for c in df_lista.columns if 'mail' in c.lower() or 'email' in c.lower()), None)
                df_lista['email'] = df_lista[email_col] if email_col else None
        except Exception:
            df_lista['telefone'] = None
            df_lista['email'] = None

        try:
            df_lista = _attach_municipio_names(df_lista)
            if 'municipio' in df_lista.columns:
                df_lista['municipio'] = df_lista['municipio'].astype(str).map(_fix_text)
        except Exception:
            pass
        df_lista['cnpj'] = df_lista['cnpj_basico'].astype(str) + \
                          df_lista['cnpj_ordem'].astype(str) + \
                          df_lista['cnpj_dv'].astype(str)
        if 'responsavel_documento' not in df_lista.columns:
            df_lista['responsavel_documento'] = None

        cols_lista = [
            'cnpj',
            'razao_social_nome_empresarial',
            'responsavel',
            'responsavel_documento',
            'endereco',
            'telefone',
            'email',
            'municipio',
            'uf'
        ]

        titulo_analise = termo_busca.title() if termo_busca else "Setores Selecionados"
        texto_intro = f"Análise para '{titulo_analise}', abrangendo {len(df_estabelecimentos)} estabelecimentos."
        texto_analise_completo = f"{texto_intro}\n\n{resultados_analise.get('texto', '')}"

        total_empresas = int(len(df_lista))
        resp = {
            "texto_analise": texto_analise_completo,
            "dados_graficos": resultados_analise.get("dados_graficos", {}),
            "kpis": resultados_analise.get("kpis", {}),
            "total_empresas": total_empresas,
            "empresas": df_lista[cols_lista].head(int(limit) if limit else 20)
        }
        try:
            cache.set(cache_key, resp, expire=300)
        except Exception:
            pass
        return resp

    except Exception as e:
        logger.error(f"Erro na análise setorial: {e}", exc_info=True)
        return {
            'erro': str(e),
            'texto_analise': 'Erro ao processar análise',
            'dados_graficos': {},
            'kpis': {},
            'total_empresas': 0,
            'empresas': pd.DataFrame()
        }

def get_cnpjs_por_cnae(cnaes: List[str]):
    try:
        vals = [str(c) for c in cnaes]
        df = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[('cnae_fiscal_principal','in', vals)],
            columns=['cnpj_basico','cnae_fiscal_principal']
        )
        if df is None or df.empty:
            return []
        return df['cnpj_basico'].astype(str).unique().tolist()
    except Exception as e:
        logger.error(f"Erro em get_cnpjs_por_cnae: {e}", exc_info=True)
        return []

def get_cnpjs_por_uf(uf):
    try:
        df = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[('uf','==', str(uf).upper())],
            columns=['cnpj_basico','uf']
        )
        if df is None or df.empty:
            return []
        return df['cnpj_basico'].astype(str).unique().tolist()
    except Exception:
        return []

def get_cnpjs_por_municipio(municipio: str):
    try:
        df = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=[('municipio','==', str(municipio).upper())],
            columns=['cnpj_basico','municipio']
        )
        if df is None or df.empty:
            return []
        return df['cnpj_basico'].astype(str).unique().tolist()
    except Exception as e:
        logger.error(f"Erro em get_cnpjs_por_municipio: {e}", exc_info=True)
        return []
@lru_cache(maxsize=1)

def _map_municipio_series(codes_series, uf_series):
    return codes_series.combine(uf_series, lambda c,u: _municipio_nome(c,u))

def _attach_municipio_names(df):
    try:
        df_mun = _municipios_df()
        if df_mun.empty or 'municipio' not in df.columns:
            return df
        code_col = 'codigo' if 'codigo' in df_mun.columns else df_mun.columns[0]
        # prefer explicit name columns
        name_candidates = ['nome','municipio','descricao','nome_municipio']
        name_col = next((c for c in name_candidates if c in df_mun.columns), None)
        if not name_col:
            name_col = next((c for c in df_mun.columns if c.lower() != code_col.lower() and df_mun[c].dtype == object), None)
        if not name_col and len(df_mun.columns) > 1:
            name_col = df_mun.columns[1]
        if not name_col:
            return df

        df_tmp = df.copy()
        df_tmp['__mun_code'] = pd.to_numeric(df_tmp['municipio'], errors='coerce').astype('Int64')
        mun_tmp = df_mun[[code_col, name_col]].copy()
        mun_tmp[code_col] = pd.to_numeric(mun_tmp[code_col], errors='coerce').astype('Int64')

        df_tmp = df_tmp.merge(mun_tmp, left_on='__mun_code', right_on=code_col, how='left')
        df_tmp.drop(columns=['__mun_code', code_col], inplace=True, errors='ignore')
        df_tmp['municipio'] = df_tmp[name_col].where(df_tmp[name_col].notna(), df_tmp['municipio'])
        df_tmp.drop(columns=[name_col], inplace=True, errors='ignore')
        return df_tmp
    except Exception:
        return df

def _fix_text(s):
    """Corrige encoding de texto mojibake (latin1 -> utf8)"""
    try:
        return str(s).encode('latin1').decode('utf-8')
    except Exception:
        return str(s)

def analisar_licitacoes_por_cnpj(cnpj: str):
    """
    Realiza análise de oportunidades de licitação para a empresa com base no seu CNPJ
    """
    try:
        logger.info(f"Iniciando análise de licitação para {cnpj}")

        empresa_df = consultar_cnpj_completo(cnpj)
        if empresa_df is None or empresa_df.empty:
            logger.warning(f"Empresa não encontrada para {cnpj}")
            return {"erro": "CNPJ não encontrado na base."}

        empresa = empresa_df.iloc[0]
        razao_social = empresa.get("razao_social_nome_empresarial", "")
        cnae = obter_cnae_principal_por_cnpj(cnpj)

        if not cnae:
            return {"erro": "CNAE não disponível para esta empresa."}

        licitacoes_df = buscar_licitacoes_pncp(cnae=cnae)

        if licitacoes_df is None or licitacoes_df.empty:
            try:
                cliente = PNCPIntegration()
                registros = []
                cnpj_digits = ''.join(filter(str.isdigit, cnpj))
                cnpj_fmt = f"{cnpj_digits[:2]}.{cnpj_digits[2:5]}.{cnpj_digits[5:8]}/{cnpj_digits[8:12]}-{cnpj_digits[12:]}" if len(cnpj_digits)==14 else cnpj_digits
                for key in [cnpj_digits, cnpj_fmt, razao_social]:
                    if not key:
                        continue
                    registros = cliente._buscar_publicacoes(str(key))
                    if registros:
                        break
                if registros:
                    licitacoes_df = pd.json_normalize(registros)
                    if 'objeto' not in licitacoes_df.columns and 'objetoCompra' in licitacoes_df.columns:
                        licitacoes_df['objeto'] = licitacoes_df['objetoCompra']
                else:
                    licitacoes_df = pd.DataFrame()
            except Exception:
                licitacoes_df = pd.DataFrame()
        if licitacoes_df is None or licitacoes_df.empty:
            return {
                "cnpj": cnpj,
                "empresa": razao_social,
                "cnae": cnae,
                "oportunidades": [],
                "total": 0,
                "diretas": [],
                "indiretas": []
            }

        licitacoes_df = licitacoes_df.copy()
        desc = None
        try:
            df_c = pd.read_parquet(Config.ARQUIVOS_PARQUET['cnaes'])
            if df_c is not None and not df_c.empty:
                code_col = next((c for c in ['codigo','cnae','cnae_fiscal'] if c in df_c.columns), df_c.columns[0])
                name_col = next((c for c in ['descricao','nome','titulo'] if c in df_c.columns), None)
                if name_col is None:
                    name_col = next((c for c in df_c.columns if c.lower()!=code_col.lower() and df_c[c].dtype==object), None)
                rowc = df_c[df_c[code_col].astype(str).str.zfill(7)==str(cnae).zfill(7)]
                if not rowc.empty:
                    desc = str(rowc.iloc[0][name_col])
        except Exception:
            desc = None
        import re
        def norm(s):
            try:
                return re.sub(r"[^a-z0-9 ]"," ", str(s).lower())
            except Exception:
                return ""
        kw = []
        base_text = norm(desc)
        if base_text:
            for w in base_text.split():
                if len(w)>=4:
                    kw.append(w)
        if not kw:
            # fallback para palavras da razão social
            rbase = norm(razao_social)
            for w in rbase.split():
                if len(w)>=4:
                    kw.append(w)
        kw = list(dict.fromkeys(kw))
        def score(text):
            t = norm(text)
            if not t:
                return 0
            sc = 0
            for w in kw:
                if w in t:
                    sc += 1
            if cnae[:2] in t:
                sc += 1
            return sc
        licitacoes_df['__score'] = licitacoes_df['objeto'].apply(score)
        max_sc = max(len(kw) + 1, 1)
        licitacoes_df['__percent'] = (licitacoes_df['__score'] / max_sc * 100).clip(lower=0, upper=100)
        licitacoes_df['__percent'] = licitacoes_df['__percent'].round(0).astype(int)
        diretas_df = licitacoes_df[licitacoes_df['__score']>0]
        indiretas_df = licitacoes_df[licitacoes_df['__score']==0]
        diretas_df = diretas_df.sort_values(by=['__score','__percent'], ascending=[False, False])
        diretas_df = diretas_df.assign(score_percent=diretas_df['__percent'])
        indiretas_df = indiretas_df.assign(score_percent=indiretas_df['__percent'])
        compat_dir = int((len(diretas_df) / max(len(licitacoes_df), 1)) * 100)
        media_dir = int((diretas_df['__percent'].mean() if not diretas_df.empty else 0))
        resultado = {
            "cnpj": cnpj,
            "empresa": razao_social,
            "cnae": cnae,
            "total": int(len(licitacoes_df)),
            "total_diretas": int(len(diretas_df)),
            "total_indiretas": int(len(indiretas_df)),
            "analise": {
                "compatibilidade_direta_percent": compat_dir,
                "compatibilidade_media_percent": media_dir
            },
            "oportunidades": serializar_dataframe(diretas_df.head(50)),
            "diretas": serializar_dataframe(diretas_df.head(50)),
            "indiretas": serializar_dataframe(indiretas_df.head(50))
        }
        return resultado

    except Exception as e:
        logger.exception(f"Erro ao analisar licitações por CNPJ: {e}")
        return {"erro": "Falha na análise de licitações."}

def estudo_compatibilidade_mercado(cnpj: str, top_n: int = 10, incluir_subclasse: bool = True):
    """Gera um ranking de setores (CNAEs) mais compatíveis para prospectar,
    a partir do CNPJ informado. Retorna lista ordenada por score e KPIs do setor.
    Este método combina: CNAE da empresa, expansão para subclasse/classe, e um scoring
    onde cada setor é tratado como um 'edital fictício'."""
    try:
        if not cnpj:
            return {'erro': 'CNPJ não informado'}
        # 1) obter dados básicos da empresa
        info = consultar_cnpj_simples_enriquecida(cnpj)
        if not info:
            return {'erro': 'CNPJ não encontrado'}
        # mapeia dados da empresa para scoring
        cnpj_data = _map_cnpj_enriquecido_to_scoring(info)
        cnae_principal = str(info.get('cnae_fiscal') or info.get('cnae_fiscal_principal') or '').zfill(7)
        if not cnae_principal or set(cnae_principal) == set(['0']):
            # fallback tentar pela descrição
            descricao = str(info.get('cnae_descricao') or info.get('descricao') or '').strip()
            sugeridos = sugerir_cnaes(descricao) if callable(sugerir_cnaes) else []
            if sugeridos:
                cnae_principal = str(sugeridos[0]['codigo']).zfill(7)
        # 2) gerar candidatos de CNAE a serem avaliados
        candidatos = [cnae_principal] if cnae_principal else []
        if incluir_subclasse and hasattr(__import__(__name__), 'expandir_cnaes_relacionados'):
            try:
                from importlib import import_module
                mod = import_module(__name__)
                exp = getattr(mod, 'expandir_cnaes_relacionados', None)
                if exp:
                    exp_list = exp(cnae_principal, profundidade=2)
                    for x in exp_list:
                        if x not in candidatos:
                            candidatos.append(x)
            except Exception:
                pass
        # also try to suggest from description
        try:
            descricao = str(info.get('cnae_descricao') or info.get('razao_social_nome_empresarial') or '')
            sugeridos = sugerir_cnaes(descricao) if callable(sugerir_cnaes) else []
            for s in sugeridos[:5]:
                code = str(s.get('codigo') or '').zfill(7)
                if code and code not in candidatos:
                    candidatos.append(code)
        except Exception:
            pass

        resultados = []
        engine = ScoringEngine()

        # Iterate candidates and score
        for c in candidatos:
            try:
                # run sector analysis (light) if available
                analise = None
                try:
                    analise = executar_analise_setorial(cnae_codes=[c], somente_ativas=True)
                except Exception:
                    analise = None
                # prepare a mock 'edital' from sector KPIs
                kpis = (analise.get('kpis') if analise else {}) or {}
                # estimate valorEstimado: if sector has capital mean, use it * 5, else fallback 100k
                valor_estimado = float(kpis.get('capital_social_medio') or 0) * 5 if kpis.get('capital_social_medio') else 100000.0
                # preferred portes: try to infer most common porte from grafico
                portes = []
                try:
                    graf = analise.get('dados_graficos', {}) if analise else {}
                    distrib = graf.get('distribuicao_porte', {}).get('series') if graf else None
                    if distrib is not None:
                        # take top 2 porte labels
                        top_portes = list(distrib.index[:2].astype(str)) if hasattr(distrib, 'index') else []
                        portes = top_portes
                except Exception:
                    portes = []

                edital_mock = {
                    'cnae_relacionado': c,
                    'valorEstimado': valor_estimado,
                    'uf': None,
                    'municipio': None,
                    'porte_preferencial': portes,
                    'exige_experiencia': False,
                    'exige_certidoes': False,
                    'palavras_chave': []
                }
                # compute score for this edital_mock
                r = engine.calcular_score(cnpj_data, edital_mock)
                # attach sector KPIs summary
                resultados.append({
                    'cnae': c,
                    'descricao': None,
                    'score_total': r.score_total if hasattr(r,'score_total') else (getattr(r,'score_total', None) if isinstance(r, dict) else None),
                    'classificacao': r.classificacao if hasattr(r,'classificacao') else getattr(r,'classificacao', None),
                    'kpis_setor': kpis,
                    'empresas_no_setor': int(analise.get('empresas').shape[0]) if analise and analise.get('empresas') is not None else None,
                    'texto_setor': analise.get('texto_analise') if analise else None
                })
            except Exception as e:
                continue

        # sort by score desc
        resultados_sorted = sorted(resultados, key=lambda x: (x.get('score_total') or 0), reverse=True)
        return {
            'cnpj': cnpj,
            'cnpj_mapeado': cnpj_data.get('cnpj'),
            'cnae_principal': cnae_principal,
            'candidatos_avaliados': len(resultados_sorted),
            'top_setores': resultados_sorted[:top_n],
            'todos_resultados': resultados_sorted
        }
    except Exception as e:
        import logging
        logging.exception('Erro no estudo_compatibilidade_mercado')
        return {'erro': 'Falha ao executar estudo de compatibilidade'}

def obter_kpis_base_completo():
    """
    Retorna KPIs da base completa SEM filtros.
    Esta função carrega TODA a base e gera métricas agregadas.
    """
    try:
        from config import Config
        import pandas as pd
        
        logger.info("Carregando base completa para KPIs...")
        
        # Carregar estabelecimentos (amostra maior)
        df_estab = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            columns=[
                'cnpj_basico', 'situacao_cadastral', 'uf', 'municipio',
                'cnae_fiscal_principal', 'data_de_inicio_atividade'
            ]
        )
        
        # Limitar para performance (opcional - remova se quiser 100% da base)
        # df_estab = df_estab.head(1000000)  # 1M de registros
        
        logger.info(f"Estabelecimentos carregados: {len(df_estab):,}")
        
        # Carregar empresas correspondentes
        cnpjs_basicos = df_estab['cnpj_basico'].astype(str).unique().tolist()[:100000]  # Limitar para performance
        
        df_empresas = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=['cnpj_basico', 'capital_social_da_empresa', 'porte_da_empresa']
        )
        
        # Carregar sócios
        df_socios = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['socios'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=['cnpj_basico', 'nome_socio', 'qualificacao_socio']
        )
        
        logger.info(f"Empresas: {len(df_empresas):,} | Sócios: {len(df_socios):,}")
        
        # Executar análise setorial
        from analise_setorial import analisar_dados_setoriais
        resultado = analisar_dados_setoriais(df_estab, df_empresas, df_socios)
        
        if not resultado or not resultado.get('sucesso'):
            logger.warning("Análise retornou erro")
            return {
                'cards': {},
                'graficos': {},
                'kpis': {},
                'ranking': {},
                'top': {}
            }
        
        # Estruturar resposta
        resposta = {
            'cards': resultado.get('kpis', {}),
            'graficos': resultado.get('dados_graficos', {}),
            'kpis': resultado.get('kpis', {}),
            'ranking': resultado.get('ranking', {}),
            'top': resultado.get('top', {}),
            'resumo': resultado.get('resumo', {})
        }
        
        logger.info(f"KPIs base gerados com sucesso: {len(resposta['kpis'])} métricas")
        return resposta
        
    except Exception as e:
        logger.error(f"Erro ao obter KPIs base: {e}", exc_info=True)
        return {
            'cards': {},
            'graficos': {},
            'kpis': {},
            'ranking': {},
            'top': {},
            'erro': str(e)
        }


def obter_kpis_geral_filtrado(cnae=None, uf=None, municipio=None, ano_min=None):
    """
    Retorna KPIs COM filtros aplicados (para análise específica).
    """
    try:
        from config import Config
        import pandas as pd
        
        logger.info(f"Carregando KPIs com filtros: CNAE={cnae}, UF={uf}, Mun={municipio}")
        
        # Construir filtros para leitura otimizada
        filtros = []
        
        if cnae:
            cnaes = [cnae] if isinstance(cnae, str) else cnae
            filtros.append(('cnae_fiscal_principal', 'in', cnaes))
        
        if uf:
            ufs = [uf] if isinstance(uf, str) else uf
            filtros.append(('uf', 'in', ufs))
        
        # Carregar estabelecimentos com filtros
        df_estab = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['estabelecimentos'],
            filters=filtros if filtros else None,
            columns=[
                'cnpj_basico', 'situacao_cadastral', 'uf', 'municipio',
                'cnae_fiscal_principal', 'data_de_inicio_atividade'
            ]
        )
        
        if df_estab.empty:
            logger.warning("Nenhum estabelecimento encontrado com os filtros")
            return {
                'cards': {},
                'graficos': {},
                'kpis': {},
                'ranking': {},
                'top': {}
            }
        
        # Aplicar filtros adicionais em memória
        if municipio:
            df_estab = df_estab[
                df_estab['municipio'].astype(str).str.upper() == str(municipio).upper()
            ]
        
        if ano_min:
            df_estab['ano_inicio'] = pd.to_datetime(
                df_estab['data_de_inicio_atividade'].astype(str),
                format='%Y%m%d',
                errors='coerce'
            ).dt.year
            df_estab = df_estab[df_estab['ano_inicio'] >= int(ano_min)]
        
        logger.info(f"Estabelecimentos após filtros: {len(df_estab):,}")
        
        # Carregar dados complementares
        cnpjs_basicos = df_estab['cnpj_basico'].astype(str).unique().tolist()
        
        df_empresas = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['empresas'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=['cnpj_basico', 'capital_social_da_empresa', 'porte_da_empresa']
        )
        
        df_socios = pd.read_parquet(
            Config.ARQUIVOS_PARQUET['socios'],
            filters=[('cnpj_basico', 'in', cnpjs_basicos)],
            columns=['cnpj_basico', 'nome_socio']
        )
        
        # Executar análise
        from analise_setorial import analisar_dados_setoriais
        resultado = analisar_dados_setoriais(df_estab, df_empresas, df_socios)
        
        if not resultado or not resultado.get('sucesso'):
            return {
                'cards': {},
                'graficos': {},
                'kpis': {},
                'ranking': {}
            }
        
        return {
            'cards': resultado.get('kpis', {}),
            'graficos': resultado.get('dados_graficos', {}),
            'kpis': resultado.get('kpis', {}),
            'ranking': resultado.get('ranking', {}),
            'resumo': resultado.get('resumo', {})
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter KPIs filtrados: {e}", exc_info=True)
        return {
            'cards': {},
            'graficos': {},
            'kpis': {},
            'ranking': {},
            'erro': str(e)
        }
