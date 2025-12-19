import sys
import os

# Adicionar diretório atual ao path
sys.path.append(os.getcwd())

print("Verificando importação de routes_analises...")
try:
    # Tenta importar o blueprint para validar sintaxe
    from routes_analises import analises_bp
    print("Sintaxe OK - Import Sucesso")
except SyntaxError as e:
    print(f"ERRO DE SINTAXE: {e}")
except ImportError as e:
    print(f"ERRO DE IMPORT: {e}")
except Exception as e:
    print(f"ERRO GERAL: {e}")
