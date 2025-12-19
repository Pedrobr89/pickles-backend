"""
Configurações centralizadas da aplicação
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

class Config:
    """Configuração base"""

    # Paths
    BASE_DIR = Path(__file__).parent
    DATA_DIR = (BASE_DIR.parent.parent / 'data')
    CACHE_DIR = BASE_DIR / 'cache'
    PGFN_DIR = (DATA_DIR / '3_PGFN')

    # Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key')
    JSON_AS_ASCII = False
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max

    # Database
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', f'sqlite:///{BASE_DIR.parent}/users.db')
    
    # Fix para Render/Supabase:
    # 1. Garante uso da porta 6543 (Pooler)
    # 2. Resolve hostname para IPv4 explicitamente (Psycopg2 bug bypass)
    try:
        if "supabase.co" in SQLALCHEMY_DATABASE_URI:
            import socket
            from urllib.parse import urlparse, urlunparse

            # Se estiver na porta 5432, move para 6543
            if ":5432" in SQLALCHEMY_DATABASE_URI:
                SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.replace(":5432", ":6543")

            # Resolve DNS para IPv4
            # Isso força o psycopg2 a conectar no IP direto, ignorando a pilha IPv6 do sistema
            parsed = urlparse(SQLALCHEMY_DATABASE_URI)
            host = parsed.hostname
            if host:
                # Pega o primeiro IP IPv4 disponível
                infos = socket.getaddrinfo(host, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
                if infos:
                    ip = infos[0][4][0]
                    # Reconstrói a URL trocando hostname pelo IP [IPv4]
                    # urlparse coloca user:pass@host:port em netloc
                    # Vamos substituir apenas o host na string netloc
                    
                    new_netloc = parsed.netloc.replace(host, f"{ip}")
                    # Se tiver porta, ela já está em netloc e será preservada
                    
                    SQLALCHEMY_DATABASE_URI = parsed._replace(netloc=new_netloc).geturl()
                    print(f"WSGI Fix: DNS resolvido para {ip} (IPv4)", flush=True)

    except Exception as e:
        print(f"WSGI Fix Error: {e}", flush=True)

    if SQLALCHEMY_DATABASE_URI.startswith("postgres://"):
        SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.replace("postgres://", "postgresql://", 1)
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # CORS
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', 'http://localhost:*,http://127.0.0.1:*,null').split(',')

    # Rate Limiting
    RATE_LIMIT_ENABLED = os.environ.get('RATE_LIMIT_ENABLED', 'true').lower() == 'true'
    RATE_LIMIT_DEFAULT = os.environ.get('RATE_LIMIT_DEFAULT', '1000 per hour, 100 per minute').split(',')
    RATE_LIMIT_STORAGE = os.environ.get('RATE_LIMIT_STORAGE', 'memory://')

    # Cache
    CACHE_TYPE = os.environ.get('CACHE_TYPE', 'simple')
    CACHE_DEFAULT_TIMEOUT = int(os.environ.get('CACHE_DEFAULT_TIMEOUT', 300))
    CACHE_THRESHOLD = int(os.environ.get('CACHE_THRESHOLD', 1000))

    # API
    API_VERSION = '4.0'
    API_TITLE = 'API de Análise de Dados da Receita'
    API_HOST = os.environ.get('API_HOST', '127.0.0.1')
    API_PORT = int(os.environ.get('API_PORT', 5000))

    # Arquivos de dados
    @staticmethod
    def _find_file(root: Path, patterns: list[str]) -> Path:
        try:
            for pat in patterns:
                cand = next(root.rglob(pat), None)
                if cand:
                    return cand
        except Exception:
            pass
        return root / patterns[0]

    # Resolver dinamicamente os arquivos dentro de '2_fonte_de_dados'
    ARQUIVOS_PARQUET = {
        'empresas': _find_file.__func__(DATA_DIR, [
            '*EMPRESAS*.parquet','*empresas*.parquet'
        ]),
        'estabelecimentos': _find_file.__func__(DATA_DIR, [
            '*ESTABELECIMENTOS*.parquet','*estabelecimentos*.parquet','*K3241*K3241*.parquet'
        ]),
        'socios': _find_file.__func__(DATA_DIR, [
            '*SOCIOS*.parquet','*socios*.parquet','*QSA*.parquet'
        ]),
        'simples': _find_file.__func__(DATA_DIR, [
            '*SIMPLES*.parquet','*simples*.parquet'
        ]),
        'cnaes': _find_file.__func__(DATA_DIR, [
            '*CNAES*.parquet','*cnae*.parquet'
        ]),
        'municipios': _find_file.__func__(DATA_DIR, [
            '*MUNICIPIOS*.parquet','*municipio*.parquet','*IBGE*MUN*.parquet'
        ]),
        'pgfn': _find_file.__func__(DATA_DIR, [
            '**/*PGFN*.parquet','**/*divida*.parquet'
        ]),
        'comex': _find_file.__func__(DATA_DIR, [
            '**/*COMEX*.parquet','**/*export*.parquet','**/*import*.parquet'
        ]),
        'caged': _find_file.__func__(DATA_DIR, [
            '**/*CAGED*.parquet','**/*emprego*.parquet'
        ]),
        'antt': _find_file.__func__(DATA_DIR, [
            '**/*ANTT*.parquet','**/*logistica*.parquet','**/*transporte*.parquet'
        ]),
        'diarios': _find_file.__func__(DATA_DIR, [
            '**/*DIARIO*.parquet','**/*DOU*.parquet','**/*diario_oficial*.parquet'
        ])
    }

    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Performance
    MAX_PAGE_SIZE = 500
    DEFAULT_PAGE_SIZE = 50
    QUERY_TIMEOUT = 30  # segundos

class DevelopmentConfig(Config):
    """Configuração de desenvolvimento"""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """Configuração de produção"""
    DEBUG = False
    TESTING = False
    RATE_LIMIT_DEFAULT = ["10000 per hour", "500 per minute"]

class TestingConfig(Config):
    """Configuração de testes"""
    DEBUG = True
    TESTING = True
    RATE_LIMIT_ENABLED = False

# Mapeamento de configurações
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
