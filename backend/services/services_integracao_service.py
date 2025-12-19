"""
Serviços de integração com fontes externas
"""

import logging
import requests
import asyncio
import aiohttp
from typing import Dict, Any
from core.config import Config
from services.services_cache_service import cache
from services.services_cnpj_service import consultar_cnpj_completo
from services.compat import requests_kwargs
from utils.utils_validator import normalizar_cnpj

logger = logging.getLogger(__name__)

class OrquestradorIntegracoes:
    """
    Orquestrador de integrações com fontes externas
    """

    def __init__(self):
        self.sessao = None

    async def _get_session(self):
        """Obtém uma sessão HTTP"""
        if self.sessao is None or self.sessao.closed:
            self.sessao = aiohttp.ClientSession()
        return self.sessao

    async def _close_session(self):
        """Fecha a sessão HTTP"""
        if self.sessao and not self.sessao.closed:
            await self.sessao.close()

    async def analise_completa_empresa(self, cnpj: str, incluir_historico: bool = True) -> Dict[str, Any]:
        """
        Executa análise completa de uma empresa com dados de múltiplas fontes
        """
        try:
            # Obtém dados básicos da empresa
            dados_basicos = await self._obter_dados_basicos(cnpj)

            # Obtém dados de licitações
            dados_licitacoes = await self._obter_dados_licitacoes(cnpj)

            # Obtém dados de compliance
            dados_compliance = await self._obter_dados_compliance(cnpj)

            # Obtém dados de emprego
            dados_emprego = await self._obter_dados_emprego(cnpj)

            # Consolida resultados
            resultado = {
                "cnpj": cnpj,
                "identificacao": dados_basicos.get("identificacao", {}),
                "operacional": dados_basicos.get("operacional", {}),
                "licitacoes": dados_licitacoes,
                "compliance": dados_compliance,
                "emprego": dados_emprego,
                "score_geral": self._calcular_score_geral(
                    dados_basicos, dados_licitacoes, dados_compliance, dados_emprego
                )
            }

            return resultado

        except Exception as e:
            logger.error(f"Erro na análise 360° para {cnpj}: {e}")
            return {"erro": str(e)}

    async def _obter_dados_basicos(self, cnpj: str) -> Dict[str, Any]:
        """Obtém dados básicos da empresa"""
        # Implementação simplificada - na prática, você chamaria sua API interna
        return {
            "identificacao": {
                "razao_social": "Empresa Exemplo LTDA",
                "porte": "MEDIO PORTE",
                "natureza_juridica": "Sociedade Empresária Limitada"
            },
            "operacional": {
                "atividade_principal": {
                    "codigo": "4711301",
                    "text": "Comércio varejista de mercadorias em geral, com predominância de produtos alimentícios - hipermercados"
                },
                "vinculos_empregatícios": 150
            }
        }

    async def _obter_dados_licitacoes(self, cnpj: str) -> Dict[str, Any]:
        """Obtém dados de licitações"""
        # Implementação simplificada - na prática, você chamaria a API do PNCP
        return {
            "total_contratos": 15,
            "valor_total": 1250000.00,
            "anos_atuacao": [2020, 2021, 2022, 2023],
            "orgaos_contratantes": ["Ministério da Saúde", "Prefeitura de São Paulo"],
            "score_experiencia": {
                "score": 85,
                "classificacao": "Bom"
            }
        }

    async def _obter_dados_compliance(self, cnpj: str) -> Dict[str, Any]:
        """Obtém dados de compliance"""
        # Implementação simplificada
        return {
            "divida_ativa": False,
            "processos_judiciais": 0,
            "score_compliance": 100
        }

    async def _obter_dados_emprego(self, cnpj: str) -> Dict[str, Any]:
        """Obtém dados de emprego"""
        # Implementação simplificada
        return {
            "vinculos_ativos": 150,
            "vinculos_ultimo_ano": 165,
            "variacao_percentual": 10.0,
            "score_emprego": 90
        }

    def _calcular_score_geral(self, dados_basicos, dados_licitacoes,
                             dados_compliance, dados_emprego) -> Dict[str, Any]:
        """Calcula o score geral da empresa"""
        # Implementação simplificada
        score = 0

        # Peso dos componentes
        score += dados_licitacoes.get("score_experiencia", {}).get("score", 0) * 0.3
        score += dados_compliance.get("score_compliance", 0) * 0.3
        score += dados_emprego.get("score_emprego", 0) * 0.2
        score += 20  # Score base

        # Classificação
        if score >= 90:
            classificacao = "Excelente"
        elif score >= 70:
            classificacao = "Bom"
        elif score >= 50:
            classificacao = "Regular"
        else:
            classificacao = "Ruim"

        return {
            "score": round(score, 1),
            "classificacao": classificacao,
            "componentes": {
                "licitacoes": dados_licitacoes.get("score_experiencia", {}).get("score", 0),
                "compliance": dados_compliance.get("score_compliance", 0),
                "emprego": dados_emprego.get("score_emprego", 0),
                "base": 20
            }
        }

class PNCPIntegration:
    """
    Integração com o Portal Nacional de Contratações Públicas (PNCP)
    """

    def __init__(self):
        self.base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        self._sample_items = [
            {
                "numeroControlePNCP": "SAMPLE-1",
                "objeto": "Aquisição de materiais de escritório",
                "orgao": "Prefeitura Municipal Exemplo",
                "unidadeGestora": "Secretaria de Administração",
                "ufSigla": "SP",
                "municipioNome": "SAO PAULO",
                "valorTotalEstimado": 150000.00,
                "dataPublicacao": "2025-11-15",
                "dataAberturaProposta": "2025-12-01",
                "dataEncerramentoProposta": "2025-12-10",
                "codigoModalidadeContratacao": 5,
            },
            {
                "numeroControlePNCP": "SAMPLE-2",
                "objetoCompra": "Serviços de manutenção predial",
                "orgao": "Governo do Estado Exemplo",
                "unidadeGestora": "Secretaria de Obras",
                "ufSigla": "RJ",
                "municipioNome": "RIO DE JANEIRO",
                "valorTotalEstimado": 320000.00,
                "dataPublicacaoPncp": "2025-11-18",
                "dataAberturaProposta": "2025-12-03",
                "dataEncerramentoProposta": "2025-12-12",
                "codigoModalidadeContratacao": 3,
            }
        ]
        pass

    def _kw(self):
        return requests_kwargs(timeout=15, headers={"User-Agent":"Mozilla/5.0"})

    def _buscar_publicacoes(self, palavra_chave: str) -> list:
        import requests
        import time
        inicio = time.time()
        params = {
            "tamanhoPagina": 200,
            "pagina": 1,
            "palavraChave": palavra_chave
        }
        try:
            # Cache por palavra-chave para acelerar
            cache_key = f"pncp:pub:{palavra_chave}:p1:200"
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
            r = requests.get(self.base_url, params=params, **requests_kwargs(timeout=10))
            if not r.ok:
                return []
            js = r.json()
            data = js.get("data") or js.get("items") or []
            cache.set(cache_key, data, expire=3600)
            return data
        except Exception:
            return []

    def listar_editais(self, pagina: int = 1, tamanho: int = 10, filtros: dict | None = None) -> dict:
        import requests
        import logging
        logger = logging.getLogger(__name__)
        filtros = filtros or {}
        params = {
            "pagina": max(int(pagina or 1), 1),
            "tamanhoPagina": max(min(int(tamanho or 10), 200), 1)
        }
        # Aplica filtros conforme manual da API (versão 1.0)
        for k in [
            "situacao",
            "modalidade",
            "orgao",
            "unidadeGestora",
            "palavraChave",
            "municipioNome",
            "esferaId",
            "poderId",
            "tipoInstrumentoConvocatorioCodigo",
            "tipoMargemPreferencia",
            "conteudoNacional",
            "dataInicial",
            "dataFinal",
            "codigoModalidadeContratacao",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "uf",
            "codigoMunicipioIbge"
        ]:
            v = filtros.get(k)
            if v:
                params[k] = v
        # Mapas de compatibilidade: aceitar chaves alternativas usadas no front
        if filtros.get("ufSigla") and not params.get("uf"):
            params["uf"] = filtros["ufSigla"]
        # município por código IBGE (se fornecido como numero)
        muni = filtros.get("municipioIbge") or filtros.get("municipioCodigo")
        if muni and not params.get("codigoMunicipioIbge"):
            params["codigoMunicipioIbge"] = str(muni)
        # Normaliza datas para AAAAMMDD (somente dígitos)
        def _normdate(s):
            if not s:
                return None
            d = "".join(ch for ch in str(s) if ch.isdigit())
            return d[:8] if len(d) >= 8 else None
        di = _normdate(params.get("dataInicial"))
        df = _normdate(params.get("dataFinal"))
        if di:
            params["dataInicial"] = di
        if df:
            params["dataFinal"] = df
        cache_key = (
            f"pncp:feed:p{params['pagina']}:s{params['tamanhoPagina']}"
            f":di{params.get('dataInicial','')}:df{params.get('dataFinal','')}"
            f":mod{params.get('codigoModalidadeContratacao','')}"
            f":sit{params.get('situacao','')}:org{params.get('orgao','')}:ug{params.get('unidadeGestora','')}:pk{params.get('palavraChave','')}"
            f":uf{params.get('uf','')}:mun{params.get('municipioNome','')}:munibge{params.get('codigoMunicipioIbge','')}:esf{params.get('esferaId','')}:pod{params.get('poderId','')}"
            f":tic{params.get('tipoInstrumentoConvocatorioCodigo','')}:tmp{params.get('tipoMargemPreferencia','')}:ecn{params.get('conteudoNacional','')}"
            f":cnpj{params.get('cnpj','')}:uadm{params.get('codigoUnidadeAdministrativa','')}:usr{params.get('idUsuario','')}"
        )
        cached = None
        try:
            kw = self._kw(); kw['timeout'] = 10
            r = requests.get(self.base_url, params=params, **kw)
            if not r.ok:
                logger.warning(f"PNCP feed falhou: {r.status_code} {r.text[:200]}")
                # tentativa com verify=False
                try:
                    kw2 = self._kw(); kw2['timeout'] = 10; kw2['verify'] = False
                    r = requests.get(self.base_url, params=params, **kw2)
                except Exception as e:
                    logger.error(f"PNCP verify=False erro: {e}")
                    return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": []}
                if not r.ok:
                    return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": self._sample_items}
            js = r.json()
            if isinstance(js, list):
                data = js
                extra = {}
            else:
                data = js.get("data") or js.get("items") or js.get("content") or []
                # Quando API retorna estrutura com total/paginas, preserve alguns campos
                extra = { k: js.get(k) for k in ["totalRegistros","totalPaginas","numeroPagina","paginasRestantes"] if k in js }
            resultado = {
                "pagina": params['pagina'],
                "tamanhoPagina": params['tamanhoPagina'],
                "data": data,
                **extra
            }
            cache.set(cache_key, resultado, expire=300)
            return resultado
        except Exception as e:
            logger.error(f"Erro ao listar editais PNCP: {e}", exc_info=True)
            return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": self._sample_items}

    def listar_editais_todos(self, dias: int = 30, tamanho: int = 200, filtros: dict | None = None, max_total: int = 10000) -> dict:
        import requests
        import logging
        logger = logging.getLogger(__name__)
        from datetime import datetime, timedelta
        filtros = filtros or {}
        dtf = datetime.utcnow()
        dti = dtf - timedelta(days=int(dias or 30))
        mods = filtros.get('codigoModalidadeContratacao')
        if mods in (None, '', 'todos', 'all', 0, '0'):
            modalidade_lista = [1,2,3,4,5,6,7,8,9,10]
        else:
            try:
                modalidade_lista = [int(mods)]
            except Exception:
                modalidade_lista = [5]
        itens = []
        meta = {"modalidades": [], "total": 0}
        for mod in modalidade_lista:
            pagina = 1
            coletados_mod = 0
            while True:
                params = {
                    "pagina": pagina,
                    "tamanhoPagina": max(min(int(tamanho or 200), 200), 1),
                    "dataInicial": dti.strftime("%Y%m%d"),
                    "dataFinal": dtf.strftime("%Y%m%d"),
                    "codigoModalidadeContratacao": mod
                }
                for k in [
                    "situacao",
                    "orgao",
                    "unidadeGestora",
                    "palavraChave",
                    "municipioNome",
                    "esferaId",
                    "poderId",
                    "tipoInstrumentoConvocatorioCodigo",
                    "tipoMargemPreferencia",
                    "conteudoNacional",
                    "cnpj",
                    "codigoUnidadeAdministrativa",
                    "idUsuario",
                    "uf",
                    "codigoMunicipioIbge"
                ]:
                    v = filtros.get(k)
                    if v:
                        params[k] = v
                if filtros.get("ufSigla") and not params.get("uf"):
                    params["uf"] = filtros["ufSigla"]
                muni = filtros.get("municipioIbge") or filtros.get("municipioCodigo")
                if muni and not params.get("codigoMunicipioIbge"):
                    params["codigoMunicipioIbge"] = str(muni)
                cache_key = (
                    f"pncp:all:mod{mod}:p{pagina}:s{params['tamanhoPagina']}:di{params['dataInicial']}:df{params['dataFinal']}:pk{params.get('palavraChave','')}"
                    f":uf{params.get('uf','')}:mun{params.get('municipioNome','')}:munibge{params.get('codigoMunicipioIbge','')}:esf{params.get('esferaId','')}:pod{params.get('poderId','')}"
                    f":tic{params.get('tipoInstrumentoConvocatorioCodigo','')}:tmp{params.get('tipoMargemPreferencia','')}:ecn{params.get('conteudoNacional','')}"
                    f":cnpj{params.get('cnpj','')}:uadm{params.get('codigoUnidadeAdministrativa','')}:usr{params.get('idUsuario','')}"
                )
                cached = None
                try:
                    kw = self._kw()
                    r = requests.get(self.base_url, params=params, **kw)
                    if not r.ok:
                        logger.warning(f"PNCP todos falhou mod={mod} p={pagina}: {r.status_code} {r.text[:200]}")
                        try:
                            kw2 = self._kw(); kw2['verify'] = False
                            r = requests.get(self.base_url, params=params, **kw2)
                        except Exception as e:
                            logger.error(f"PNCP todos verify=False erro: {e}")
                            js = {"data": self._sample_items, "totalPaginas": 1, "numeroPagina": pagina}
                        else:
                            if not r.ok:
                                js = {"data": self._sample_items, "totalPaginas": 1, "numeroPagina": pagina}
                            else:
                                js = r.json()
                    else:
                        js = r.json()
                except Exception as e:
                    logger.error(f"PNCP todos erro: {e}")
                    js = {"data": self._sample_items, "totalPaginas": 1, "numeroPagina": pagina}
                arr = js if isinstance(js, list) else (js.get("data") or js.get("items") or js.get("content") or [])
                if not arr:
                    # Fallback offline: usa amostras
                    arr = self._sample_items
                for it in arr:
                    itens.append(it)
                    coletados_mod += 1
                    if len(itens) >= max_total:
                        meta["modalidades"].append({"codigo": mod, "coletados": coletados_mod})
                        meta["total"] = len(itens)
                        return {"data": itens, "total": len(itens), **meta}
                total_pag = js.get("totalPaginas") if isinstance(js, dict) else None
                num_pag = js.get("numeroPagina") if isinstance(js, dict) else None
                if total_pag and num_pag and pagina >= int(total_pag):
                    break
                pagina += 1
            meta["modalidades"].append({"codigo": mod, "coletados": coletados_mod})
        meta["total"] = len(itens)
        return {"data": itens, "total": len(itens), **meta}

    def _listar_generico(self, url: str, pagina: int = 1, tamanho: int = 10, filtros: dict | None = None) -> dict:
        import requests
        import logging
        logger = logging.getLogger(__name__)
        filtros = filtros or {}
        params = {
            "pagina": max(int(pagina or 1), 1),
            "tamanhoPagina": max(min(int(tamanho or 10), 200), 1)
        }
        for k in [
            "situacao","modalidade","orgao","unidadeGestora","palavraChave",
            "municipioNome","esferaId","poderId","tipoInstrumentoConvocatorioCodigo",
            "tipoMargemPreferencia","conteudoNacional","dataInicial","dataFinal",
            "codigoModalidadeContratacao","cnpj","codigoUnidadeAdministrativa","idUsuario",
            "uf","codigoMunicipioIbge"
        ]:
            v = filtros.get(k)
            if v:
                params[k] = v
        if filtros.get("ufSigla") and not params.get("uf"):
            params["uf"] = filtros["ufSigla"]
        muni = filtros.get("municipioIbge") or filtros.get("municipioCodigo")
        if muni and not params.get("codigoMunicipioIbge"):
            params["codigoMunicipioIbge"] = str(muni)
        def _normdate(s):
            if not s:
                return None
            d = "".join(ch for ch in str(s) if ch.isdigit())
            return d[:8] if len(d) >= 8 else None
        di = _normdate(params.get("dataInicial")); df = _normdate(params.get("dataFinal"))
        if di: params["dataInicial"] = di
        if df: params["dataFinal"] = df
        try:
            kw = self._kw(); kw['timeout'] = 10
            r = requests.get(url, params=params, **kw)
            if not r.ok:
                logger.warning(f"PNCP gen falhou: {r.status_code} {r.text[:200]}")
                try:
                    kw2 = self._kw(); kw2['timeout'] = 10; kw2['verify'] = False
                    r = requests.get(url, params=params, **kw2)
                except Exception as e:
                    logger.error(f"PNCP gen verify=False erro: {e}")
                    return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": []}
                if not r.ok:
                    return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": []}
            js = r.json()
            data = js if isinstance(js, list) else (js.get("data") or js.get("items") or js.get("content") or [])
            extra = { k: js.get(k) for k in ["totalRegistros","totalPaginas","numeroPagina","paginasRestantes"] if isinstance(js, dict) and k in js }
            return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": data, **extra}
        except Exception as e:
            logger.error(f"Erro ao listar genérico PNCP: {e}", exc_info=True)
            return {"pagina": params['pagina'], "tamanhoPagina": params['tamanhoPagina'], "data": self._sample_items}

    def listar_itens(self, pagina: int = 1, tamanho: int = 10, filtros: dict | None = None) -> dict:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/item"
        return self._listar_generico(url, pagina=pagina, tamanho=tamanho, filtros=filtros)

    def listar_contratos(self, pagina: int = 1, tamanho: int = 10, filtros: dict | None = None) -> dict:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/contrato"
        return self._listar_generico(url, pagina=pagina, tamanho=tamanho, filtros=filtros)

    def listar_raw(self, tipo: str, pagina: int = 1, tamanho: int = 10, filtros: dict | None = None) -> dict:
        tipo = str(tipo or '').strip().lower()
        allowed = {
            'publicacao': "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
            'item': "https://pncp.gov.br/api/consulta/v1/contratacoes/item",
            'contrato': "https://pncp.gov.br/api/consulta/v1/contratacoes/contrato",
        }
        if tipo not in allowed:
            return {"erro": "Tipo inválido", "tipos": list(allowed.keys())}
        return self._listar_generico(allowed[tipo], pagina=pagina, tamanho=tamanho, filtros=filtros)

    def analisar_perfil_licitacoes(self, cnpj: str) -> Dict[str, Any]:
        """
        Analisa o perfil de licitações de um CNPJ usando PNCP (dados reais)
        """
        import math
        from datetime import datetime
        # Tenta por múltiplas palavras-chave: CNPJ puro, CNPJ formatado e razão social
        registros = []
        cnpj_digits = normalizar_cnpj(cnpj)
        cnpj_fmt = f"{cnpj_digits[:2]}.{cnpj_digits[2:5]}.{cnpj_digits[5:8]}/{cnpj_digits[8:12]}-{cnpj_digits[12:]}" if len(cnpj_digits)==14 else cnpj_digits
        for key in [cnpj_digits, cnpj_fmt]:
            if not key: continue
            registros = self._buscar_publicacoes(key)
            if registros:
                break
        # Fallback: buscar por razão social e por CNAE
        if not registros:
            try:
                df_emp = consultar_cnpj_completo(cnpj_digits)
                if df_emp is not None and not df_emp.empty:
                    razao = str(df_emp.iloc[0].get('razao_social_nome_empresarial','')).strip()
                    if razao:
                        registros = self._buscar_publicacoes(razao)
                    if not registros:
                        # tenta por prefixo do CNAE
                        cnae_col = next((c for c in df_emp.columns if 'cnae' in c.lower()), None)
                        if cnae_col:
                            cnae_val = str(df_emp.iloc[0].get(cnae_col,'')).strip()
                            if cnae_val:
                                registros = self._buscar_publicacoes(cnae_val[:2])
            except Exception:
                pass
        if not registros:
            return {
                "cnpj": cnpj,
                "total_contratos": 0,
                "valor_total": 0.0,
                "anos_atuacao": [],
                "orgaos_contratantes": [],
                "score_experiencia": {"score": 0, "classificacao": "Indisponível"}
            }
        def _val(x):
            try:
                return float(x)
            except Exception:
                return 0.0
        total = len(registros)
        valores = []
        anos = set()
        orgaos = []
        for it in registros:
            v = _val(it.get("valorTotalEstimado") or it.get("valorTotal") or 0)
            valores.append(v)
            dt = it.get("dataPublicacaoPncp") or it.get("dataPublicacao") or None
            if dt:
                try:
                    anos.add(int(str(dt)[:4]))
                except Exception:
                    pass
            org = it.get("orgao") or it.get("unidadeGestora") or it.get("orgaoEntidade")
            if org:
                orgaos.append(org)
        valor_total = sum(valores)
        # Score simples baseado em volume e valor
        score = min(100, int((total*5) + (valor_total/1_000_000)))
        classificacao = "Excelente" if score>=90 else ("Bom" if score>=70 else ("Regular" if score>=50 else "Baixo"))
        # Top órgãos
        try:
            from collections import Counter
            orgs_top = [k for k,_ in Counter(orgaos).most_common(10)]
        except Exception:
            orgs_top = list(set(orgaos))
        return {
            "cnpj": cnpj,
            "total_contratos": int(total),
            "valor_total": float(valor_total),
            "anos_atuacao": sorted(list(anos)),
            "orgaos_contratantes": orgs_top,
            "score_experiencia": {"score": score, "classificacao": classificacao}
        }
        
        import requests
import pandas as pd

def buscar_licitacoes_pncp(cnae: str):
    """
    Realiza busca por CNAE no PNCP (API pública)
    Exemplo usando scraping simples ou API pública se disponível
    """
    try:
        url = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        chave = str(cnae or '').strip()
        palavra = chave[:2]
        cache_key = f"pncp:cnae:{palavra}:p1:200"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        params = {
            "tamanhoPagina": 200,
            "pagina": 1,
            "palavraChave": palavra
        }

        response = requests.get(url, params=params, timeout=10)
        if not response.ok:
            logger.warning(f"PNCP CNAE falhou: {response.status_code} {response.text[:200]}")
            return pd.DataFrame()

        data = response.json().get("data", [])
        if not data:
            df = pd.DataFrame()
            cache.set(cache_key, df, expire=3600)
            return df

        df = pd.json_normalize(data)
        cache.set(cache_key, df, expire=3600)
        return df

    except Exception as e:
        logger.error(f"Erro ao buscar licitações PNCP para CNAE {cnae}: {e}", exc_info=True)
        return pd.DataFrame()
