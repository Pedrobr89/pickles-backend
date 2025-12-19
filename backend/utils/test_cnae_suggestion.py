
import sys
import os

# Add SCRIPTS to path
sys.path.append(os.path.join(os.getcwd()))

from services_analise_service import sugerir_cnaes

def test_suggestions():
    terms = ['soft', 'cultivo', '0111', 'consultoria']
    for t in terms:
        print(f"---Testing term: {t}---")
        try:
            res = sugerir_cnaes(t)
            print(f"Found {len(res)} results")
            if res:
                print("First result:", res[0])
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    test_suggestions()
