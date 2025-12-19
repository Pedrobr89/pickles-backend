"""
Funções utilitárias para análise de dados
"""

import numpy as np
import pandas as pd

def calcular_indice_gini(array) -> float:
    """
    Calcula o Índice de Gini para um array de valores
    """
    arr = np.asarray(array, dtype=float)
    if arr.size == 0:
        return 0.0
    arr_sorted = np.sort(arr)
    n = arr_sorted.size
    cumx = np.cumsum(arr_sorted, dtype=float)
    total = cumx[-1]
    if total == 0:
        return 0.0
    return float((n + 1 - 2 * np.sum(cumx) / total) / n)

def calcular_hhi(series_contagem) -> float:
    """
    Calcula o Índice Herfindahl-Hirschman (HHI)
    """
    arr = np.asarray(series_contagem, dtype=float)
    total = float(arr.sum())

    if total == 0:
        return 0.0

    market_share = (arr / total) * 100.0
    return float(np.sum(market_share ** 2))

def calcular_entropia_shannon(series_contagem) -> float:
    """
    Calcula a Entropia de Shannon
    """
    arr = np.asarray(series_contagem, dtype=float)
    total = float(arr.sum())

    if total == 0:
        return 0.0

    p = arr / total
    p = p[p > 0]
    return float(-np.sum(p * np.log2(p)))
