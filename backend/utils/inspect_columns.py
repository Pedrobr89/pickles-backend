from config import Config
import pyarrow.parquet as pq
import sys
import os

# Adicionar o diretório atual ao path para importar config corretamente se necessário
sys.path.append(os.getcwd())

for nome, path in Config.ARQUIVOS_PARQUET.items():
    if nome in ['empresas', 'estabelecimentos', 'socios']:
        print(f"--- Tabela: {nome} ---")
        try:
            pf = pq.ParquetFile(str(path))
            # Imprimir apenas nomes de colunas que contenham 'data' ou 'inicio' ou 'fim' ou 'situacao'
            cols = pf.schema.names
            date_cols = [c for c in cols if any(x in c.lower() for x in ['data', 'inicio', 'fim', 'situacao', 'motivo'])]
            print(f"Todas colunas: {cols}")
            print(f"Colunas de Data/Situacao: {date_cols}")
        except Exception as e:
            print(f"Erro ao ler {path}: {e}")
