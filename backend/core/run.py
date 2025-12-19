#!/usr/bin/env python
"""
Script de inicialização da aplicação
"""

import sys
import os
from pathlib import Path
import logging

# Adiciona o diretório do projeto ao PATH
sys.path.insert(0, str(Path(__file__).parent))

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Função principal"""

    # Define ambiente
    env = os.environ.get('FLASK_ENV', 'development')
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '127.0.0.1')

    logger.info(f"""
    ╔══════════════════════════════════════════╗
    ║   API de Análise de Dados da Receita     ║
    ║            Versão 4.0                    ║
    ╚══════════════════════════════════════════╝

    Ambiente: {env}
    Servidor: http://{host}:{port}
    Documentação: http://{host}:{port}/api/docs
    """)

    # Importa e executa a aplicação
    from app import create_app

    app = create_app(env)

    if env == 'production':
        # Produção: usar servidor WSGI
        from waitress import serve
        logger.info("Iniciando servidor de produção com Waitress")
        serve(app, host=host, port=port, threads=4)
    else:
        # Desenvolvimento: usar servidor Flask
        logger.info("Iniciando servidor de desenvolvimento")
        app.run(
            host=host,
            port=port,
            debug=(env == 'development'),
            use_reloader=False,
            threaded=True
        )

if __name__ == '__main__':
    main()
