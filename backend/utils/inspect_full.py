from config import Config
import pyarrow.parquet as pq
import sys, os
sys.path.append(os.getcwd())

path = str(Config.ARQUIVOS_PARQUET['estabelecimentos'])
print(f"Lendo: {path}")
try:
    pf = pq.ParquetFile(path)
    print(pf.schema.names)
except Exception as e:
    print(f"Erro: {e}")
