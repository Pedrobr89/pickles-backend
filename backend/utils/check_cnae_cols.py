
import pandas as pd
from pathlib import Path

def check_cnaes():
    base = Path('c:/Users/pehef/Desktop/Projeto Pickles/2_fonte_de_dados')
    # Use rglob to find it like the app does
    files = list(base.rglob('*CNAES*.parquet'))
    if not files:
        print("CNAES parquet not found")
        return
    
    f = files[0]
    print(f"Reading {f}")
    try:
        df = pd.read_parquet(f)
        print("Columns:", df.columns.tolist())
        print(df.head())
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    check_cnaes()
