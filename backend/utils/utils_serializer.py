"""
Funções de serialização de dados
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def serializar_dataframe(df):
    """
    Serializa um DataFrame para JSON
    """
    if df is None or df.empty:
        return []

    try:
        df_copy = df.copy()

        # Converte datas para string
        for col in df_copy.select_dtypes(include=['datetime64[ns]']).columns:
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')

        # Converte NaN/NaT para None
        return df_copy.replace({pd.NaT: None, pd.NA: None}).to_dict(orient='records')

    except Exception as e:
        logger.error(f"Erro ao serializar DataFrame: {e}")
        return []

def serializar_dados_graficos(dados_graficos):
    """
    Serializa dados de gráficos para JSON
    """
    if not dados_graficos:
        return {}

    dados_json = {}

    for nome, data in dados_graficos.items():
        try:
            if 'series' in data and isinstance(data['series'], pd.Series):
                # É uma pandas Series
                series = data['series']
                dados_json[nome] = {
                    "titulo": data.get('titulo', 'Sem Título'),
                    "labels": series.index.tolist(),
                    "valores": series.values.tolist()
                }
            elif 'raw_values' in data:
                # Dados brutos (ex: boxplot)
                rv = data.get('raw_values', [])
                dados_json[nome] = {
                    "titulo": data.get('titulo', 'Sem Título'),
                    "raw_values": list(rv)
                }
            else:
                # Outros tipos de dados
                dados_json[nome] = data
        except Exception as e:
            logger.error(f"Erro ao serializar gráfico '{nome}': {e}")
            dados_json[nome] = {"erro": str(e)}

    return dados_json

def serializar_oportunidade(df):
    """
    Serializa oportunidades de licitação em colunas chave se disponíveis.
    """
    if df is None or df.empty:
        return []
    cols = ["numeroControlePNCP", "objetoCompra", "valorTotalEstimado", "dataPublicacaoPncp"]
    cols_exist = [c for c in cols if c in df.columns]
    if not cols_exist:
        return df.to_dict(orient="records")
    return df[cols_exist].to_dict(orient="records")
