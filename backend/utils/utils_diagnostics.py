"""
Funções de diagnóstico do sistema
"""

import pandas as pd
import pyarrow.parquet as pq
import time
import psutil
import os
from pathlib import Path
from typing import Optional, Dict, Any
from core.config import Config
import logging

logger = logging.getLogger(__name__)

def _get_disk_usage_percent(path: str) -> Optional[float]:
    try:
        partitions = psutil.disk_partitions(all=False)
        best = None
        for part in partitions:
            mp = part.mountpoint
            if path.startswith(mp.rstrip(os.sep)):
                if best is None or len(mp) > len(best.mountpoint):
                    best = part
        target = best.mountpoint if best else (Path(path).anchor or os.sep)
        return psutil.disk_usage(target).percent
    except Exception:
        try:
            return psutil.disk_usage(os.sep).percent
        except Exception:
            return None

def _get_mountpoint_for_path(path: str) -> str:
    try:
        partitions = psutil.disk_partitions(all=False)
        best = None
        for part in partitions:
            mp = part.mountpoint
            if path.startswith(mp.rstrip(os.sep)):
                if best is None or len(mp) > len(best.mountpoint):
                    best = part
        return best.mountpoint if best else (Path(path).anchor or os.sep)
    except Exception:
        return os.sep

def verificar_saude_sistema() -> Dict[str, Any]:
    """
    Verifica a saúde geral do sistema
    """
    status = {
        "status": "healthy",
        "timestamp": pd.Timestamp.now().isoformat(),
        "componentes": {
            "api": "ok",
            "arquivos": {},
            "sistema": {}
        },
        "erros": []
    }

    # Verifica arquivos
    for nome, caminho in Config.ARQUIVOS_PARQUET.items():
        status["componentes"]["arquivos"][nome] = {
            "existe": caminho.exists(),
            "tamanho_mb": round(caminho.stat().st_size / (1024 * 1024), 2) if caminho.exists() else 0
        }

        if not caminho.exists():
            status["erros"].append(f"Arquivo {nome} não encontrado")
            status["status"] = "degraded"

    disk_percent = _get_disk_usage_percent(str(Config.DATA_DIR))

    status["componentes"]["sistema"] = {
        "cpu_usage": psutil.cpu_percent(interval=1),
        "memory_usage": psutil.virtual_memory().percent,
        "disk_usage": disk_percent
    }

    return status

def obter_metricas_sistema() -> Dict[str, Any]:
    try:
        mp = _get_mountpoint_for_path(str(Config.DATA_DIR))
        du = psutil.disk_usage(mp)
    except Exception:
        du = psutil.disk_usage(os.sep)

    return {
        "sistema": {
            "cpu": {
                "percent": psutil.cpu_percent(interval=1),
                "cores": psutil.cpu_count()
            },
            "memoria": {
                "total_gb": round(psutil.virtual_memory().total / (1024 ** 3), 2),
                "usada_gb": round(psutil.virtual_memory().used / (1024 ** 3), 2),
                "percent": psutil.virtual_memory().percent
            },
            "disco": {
                "total_gb": round(du.total / (1024 ** 3), 2),
                "usado_gb": round(du.used / (1024 ** 3), 2),
                "percent": _get_disk_usage_percent(str(Config.DATA_DIR))
            }
        },
        "processo": {
            "pid": os.getpid(),
            "threads": len(psutil.Process().threads()),
            "memoria_mb": round(psutil.Process().memory_info().rss / (1024 * 1024), 2)
        }
    }

def diagnosticar_colunas():
    """
    Diagnóstico de colunas dos arquivos Parquet
    """
    sections = []

    for nome, caminho in Config.ARQUIVOS_PARQUET.items():
        if not caminho.exists():
            sections.append(f"<h2>{nome}</h2><p>Arquivo não encontrado: {caminho}</p>")
            continue

        try:
            schema = pq.read_schema(caminho)
            lista = ''.join(f"<li>{field.name}</li>" for field in schema)
            sections.append(f"<h2>{nome} ({len(schema)} colunas)</h2><ul>{lista}</ul>")
        except Exception as e:
            sections.append(f"<h2>{nome}</h2><p>Erro ao ler: {e}</p>")

    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Diagnóstico de Colunas</title>
      <style>
        body{{font-family:system-ui,Segoe UI,Arial;max-width:900px;margin:40px auto;padding:0 20px}}
        h1{{margin-top:0}}
        h2{{margin-top:24px}}
        ul{{columns:2;-webkit-columns:2;-moz-columns:2}}
        li{{margin:2px 0}}
      </style>
    </head>
    <body>
      <h1>📋 Diagnóstico de Colunas dos Arquivos</h1>
      {''.join(sections)}
    </body>
    </html>
    """

    return html, 200, {'Content-Type': 'text/html; charset=utf-8'}
