import os
import sys
from server import create_app

# Configura para produção
app = create_app('production')

if __name__ == "__main__":
    app.run()
