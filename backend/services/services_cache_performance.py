"""
Serviço de Cache e Performance B2G (Sprint 6)
Otimizações de cache e performance
"""

import logging
from typing import Dict, List, Optional, Any
import time
import hashlib
import json

logger = logging.getLogger(__name__)


class CacheB2GService:
    """Serviço de cache para otimização"""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.cache_memoria = {}  # Cache em memória (simplificado)
    
    def get_cache(self, chave: str) -> Optional[Any]:
        """Obtém valor do cache"""
        try:
            # Verificar cache em memória primeiro
            if chave in self.cache_memoria:
                entrada = self.cache_memoria[chave]
                if entrada['expira_em'] > time.time():
                    logger.debug(f"Cache hit (memória): {chave}")
                    return entrada['valor']
                else:
                    del self.cache_memoria[chave]
            
            # Verificar cache no banco
            if self.db:
                cursor = self.db.cursor()
                cursor.execute("""
                    SELECT valor, expira_em FROM match_cache
                    WHERE chave = ? AND expira_em > ?
                """, (chave, int(time.time())))
                
                row = cursor.fetchone()
                if row:
                    logger.debug(f"Cache hit (BD): {chave}")
                    valor = json.loads(row[0])
                    # Copiar para memória
                    self.cache_memoria[chave] = {
                        'valor': valor,
                        'expira_em': row[1]
                    }
                    return valor
            
            logger.debug(f"Cache miss: {chave}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao obter cache: {e}")
            return None
    
    def set_cache(
        self,
        chave: str,
        valor: Any,
        ttl_segundos: int = 3600
    ) -> bool:
        """Define valor no cache"""
        try:
            expira_em = int(time.time()) + ttl_segundos
            
            # Cache em memória
            self.cache_memoria[chave] = {
                'valor': valor,
                'expira_em': expira_em
            }
            
            # Cache no banco
            if self.db:
                cursor = self.db.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO match_cache (chave, valor, expira_em)
                    VALUES (?, ?, ?)
                """, (chave, json.dumps(valor), expira_em))
                
                self.db.commit()
            
            logger.debug(f"Cache set: {chave} (TTL: {ttl_segundos}s)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao definir cache: {e}")
            return False
    
    def invalidar_cache(self, padrao: str = None) -> int:
        """Invalida cache por padrão"""
        try:
            total = 0
            
            # Limpar memória
            if padrao:
                chaves_remover = [k for k in self.cache_memoria.keys() if padrao in k]
                for chave in chaves_remover:
                    del self.cache_memoria[chave]
                    total += 1
            else:
                total = len(self.cache_memoria)
                self.cache_memoria.clear()
            
            # Limpar banco
            if self.db:
                if padrao:
                    cursor = self.db.cursor()
                    cursor.execute("DELETE FROM match_cache WHERE chave LIKE ?", (f'%{padrao}%',))
                else:
                    cursor = self.db.cursor()
                    cursor.execute("DELETE FROM match_cache")
                
                self.db.commit()
                total += cursor.rowcount
            
            logger.info(f"Cache invalidado: {total} entradas")
            return total
            
        except Exception as e:
            logger.error(f"Erro ao invalidar cache: {e}")
            return 0
    
    def gerar_chave_cache(self, *args, **kwargs) -> str:
        """Gera chave de cache baseada em argumentos"""
        conteudo = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True)
        return hashlib.md5(conteudo.encode()).hexdigest()
    
    def limpar_cache_expirado(self) -> int:
        """Remove entradas expiradas do cache"""
        try:
            agora = int(time.time())
            total = 0
            
            # Limpar memória
            chaves_expiradas = [
                k for k, v in self.cache_memoria.items()
                if v['expira_em'] <= agora
            ]
            for chave in chaves_expiradas:
                del self.cache_memoria[chave]
                total += 1
            
            # Limpar banco
            if self.db:
                cursor = self.db.cursor()
                cursor.execute("DELETE FROM match_cache WHERE expira_em <= ?", (agora,))
                self.db.commit()
                total += cursor.rowcount
            
            if total > 0:
                logger.info(f"Cache expirado limpo: {total} entradas")
            
            return total
            
        except Exception as e:
            logger.error(f"Erro ao limpar cache expirado: {e}")
            return 0


# Decorator para cache automático
def cached(ttl_segundos=3600):
    """Decorator para cachear resultados de função"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            cache_service = CacheB2GService()
            chave = cache_service.gerar_chave_cache(func.__name__, *args, **kwargs)
            
            # Tentar obter do cache
            resultado = cache_service.get_cache(chave)
            if resultado is not None:
                return resultado
            
            # Executar função e cachear
            resultado = func(*args, **kwargs)
            cache_service.set_cache(chave, resultado, ttl_segundos)
            
            return resultado
        
        return wrapper
    return decorator
