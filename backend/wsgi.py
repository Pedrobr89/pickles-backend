import os
import sys
import logging
import traceback

# Configura logging para stdout/stderr para aparecer no Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WSGI_DEBUG")

# === FIX RENDERE/SUPABASE IPV6 ===
# Força resolução IPv4 para evitar "Network is unreachable" em conexões de banco
import socket
_old_getaddrinfo = socket.getaddrinfo
def _new_getaddrinfo(*args, **kwargs):
    responses = _old_getaddrinfo(*args, **kwargs)
    return [r for r in responses if r[0] == socket.AF_INET]
socket.getaddrinfo = _new_getaddrinfo
# =================================

print("--- WSGI STARTUP ---", file=sys.stderr)
print(f"CWD: {os.getcwd()}", file=sys.stderr)
print(f"PYTHONPATH: {sys.path}", file=sys.stderr)

try:
    # Garante que o diretório atual está no path
    sys.path.insert(0, os.getcwd())
    
    print("Importing server...", file=sys.stderr)
    from server import create_app
    print("Server imported. Creating app...", file=sys.stderr)
    
    app = create_app('production')
    print("App created successfully.", file=sys.stderr)
    
except Exception as e:
    print("!!! CRASH DURING STARTUP !!!", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    app.run()
