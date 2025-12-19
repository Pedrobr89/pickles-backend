"""
Serviço de gerenciamento de cache
"""

import logging
from pathlib import Path
from core.config import Config
import pandas as pd
import diskcache as dc
import time

logger = logging.getLogger(__name__)

# Cache em disco
cache = dc.Cache(str(Config.CACHE_DIR))

def pre_carregar_dados_essenciais():
    """
    Pré-carrega dados essenciais no cache
    """
    try:
        logger.info("Iniciando pré-carregamento de dados essenciais")

        # Carrega tabela de CNAEs
        if 'cnaes' not in cache:
            df_cnaes = pd.read_parquet(Config.ARQUIVOS_PARQUET['cnaes'])
            cache.set('cnaes', df_cnaes, expire=Config.CACHE_DEFAULT_TIMEOUT)
            logger.info("Tabela de CNAEs carregada no cache")

        # Carrega tabela de municípios
        if 'municipios' not in cache:
            df_municipios = pd.read_parquet(Config.ARQUIVOS_PARQUET['municipios'])
            cache.set('municipios', df_municipios, expire=Config.CACHE_DEFAULT_TIMEOUT)
            logger.info("Tabela de municípios carregada no cache")

        logger.info("Pré-carregamento concluído")

    except Exception as e:
        logger.error(f"Erro no pré-carregamento: {e}")

def obter_estatisticas_cache():
    """
    Obtém estatísticas do cache
    """
    try:
        stats = {
            "tamanho_memoria_mb": 0,
            "tamanho_disco_mb": 0,
            "chaves": 0,
            "hits": 0,
            "misses": 0
        }

        # Obtém estatísticas do cache
        stats["chaves"] = len(cache)
        stats["tamanho_disco_mb"] = round(sum(Path(f).stat().st_size for f in Config.CACHE_DIR.glob('*')) / (1024 * 1024), 2)

        return stats

    except Exception as e:
        logger.error(f"Erro ao obter estatísticas do cache: {e}")
        return {
            "erro": str(e),
            "tamanho_memoria_mb": 0,
            "tamanho_disco_mb": 0,
            "chaves": 0
        }

def limpar_todo_cache():
    """
    Limpa todo o cache
    """
    try:
        cache.clear()
        logger.info("Cache limpo com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        return False