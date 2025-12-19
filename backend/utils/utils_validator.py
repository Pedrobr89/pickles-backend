"""
Funções de validação
"""

import re
import logging
from typing import Dict, List
from core.config import Config
from utils.utils_error_handler import ValidationError

logger = logging.getLogger(__name__)

def validar_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ (formato e dígitos verificadores)
    """
    if not re.fullmatch(r"\d{14}", cnpj):
        logger.warning(f"CNPJ inválido (formato): {cnpj}")
        return False
    if len(set(cnpj)) == 1:
        logger.warning(f"CNPJ inválido (sequência repetida): {cnpj}")
        return False

    nums = [int(x) for x in cnpj]
    base = nums[:12]

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(d * w for d, w in zip(base, pesos1))
    dig1 = 0 if (soma1 % 11) < 2 else 11 - (soma1 % 11)

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(d * w for d, w in zip(base + [dig1], pesos2))
    dig2 = 0 if (soma2 % 11) < 2 else 11 - (soma2 % 11)

    valido = (dig1 == nums[12] and dig2 == nums[13])
    if not valido:
        logger.warning(f"CNPJ inválido (DV): {cnpj}")
    return valido

def normalizar_cnpj(cnpj: str) -> str:
    """
    Remove caracteres não numéricos e retorna apenas os 14 dígitos do CNPJ
    """
    digits = ''.join(ch for ch in str(cnpj or '') if ch.isdigit())
    if not digits:
        return ''
    if len(digits) < 14:
        return digits.zfill(14)
    return digits[:14]

def validar_arquivos_dados() -> bool:
    """
    Verifica se os arquivos de dados essenciais existem
    """
    todos_existem = True
    essenciais = {'empresas','estabelecimentos','socios','simples','cnaes','municipios'}
    for nome, caminho in Config.ARQUIVOS_PARQUET.items():
        ok = caminho.exists()
        if nome in essenciais:
            if not ok:
                logger.error(f"Arquivo não encontrado: {caminho}")
                todos_existem = False
        else:
            if not ok:
                logger.warning(f"Arquivo opcional ausente: {caminho}")
    return todos_existem

def validar_parametros_obrigatorios(params: Dict[str, object], required: List[str]) -> None:
    """
    Valida parâmetros obrigatórios
    """
    missing = [p for p in required if p not in params or not params[p]]
    if missing:
        logger.warning(f"Parâmetros obrigatórios ausentes: {missing}")
        raise ValidationError(f"Parâmetros obrigatórios ausentes: {', '.join(missing)}")
