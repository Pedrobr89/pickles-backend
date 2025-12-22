"""
API Principal - Sistema de Análise de Dados da Receita Federal
Versão 4.0 - Arquitetura Modular e Otimizada
"""

from flask import Flask, jsonify, render_template, send_file, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from pathlib import Path
import os
from dotenv import load_dotenv
# import stripe

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from app.core.database import db
from app.core.extensions import login_manager, oauth, mail
from app.models.user_model import User
import os

logger = logging.getLogger(__name__)
limiter = None

def configure_oauth(app):
    oauth.init_app(app)
    oauth.register(
        name='github',
        client_id=os.environ.get('GITHUB_CLIENT_ID'),
        client_secret=os.environ.get('GITHUB_CLIENT_SECRET'),
        access_token_url='https://github.com/login/oauth/access_token',
        access_token_params=None,
        authorize_url='https://github.com/login/oauth/authorize',
        authorize_params=None,
        api_base_url='https://api.github.com/',
        client_kwargs={'scope': 'user:email'},
    )


def create_app(config_name='development'):
    """Factory para criação da aplicação Flask"""

    # Cria instância do Flask
    # Define a pasta estática corretamente (relativo a backend/server.py -> ../frontend/static)
    app = Flask(__name__, static_folder='../frontend/static', static_url_path='/static')

    # Configurações básicas
    app.config.from_object(f'core.config.{config_name.capitalize()}Config')

    app.config['JSON_AS_ASCII'] = False

    # Configura CORS
    CORS(app, origins=app.config['CORS_ORIGINS'])

    # Configura Rate Limiting
    if app.config['RATE_LIMIT_ENABLED']:
        global limiter
        limiter = Limiter(
            app=app,
            key_func=get_remote_address,
            default_limits=app.config['RATE_LIMIT_DEFAULT'],
            storage_uri=app.config['RATE_LIMIT_STORAGE']
        )
        logger.info(f"✓ Rate limiting ativado: {app.config['RATE_LIMIT_DEFAULT']}")
    else:
        logger.warning("⚠ Rate limiting desativado")

    # Inicializa Banco de Dados
    db.init_app(app)
    
    # Inicializa OAuth
    configure_oauth(app)
    
    # Inicializa Flask-Mail
    mail.init_app(app)
    logger.info("✓ Flask-Mail inicializado")
    
    # Inicializa Login Manager
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login' # type: ignore

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # Registra blueprints
    from app.api.routes_consultas import consultas_bp
    from app.api.routes_analises import analises_bp, scoring_bp
    from app.api.routes_integracoes import integracoes_bp
    from app.api.routes_auth import auth_bp
    from app.api.routes_admin import admin_bp
    from app.api.routes_user import user_bp
    from app.api.routes_payments import payments_bp
    from app.api.routes_favoritos import bp as favoritos_bp
    # Sprint 2 - Enriquecimento e IA
    from app.api.routes_b2g_enriquecida import b2g_enriquecida_bp
    # Sprint 3 - Alertas e Notificações
    from app.api.routes_alertas_notificacoes import alertas_bp, notificacoes_bp
    # Sprint 4 - Filtros, Mapas e Exportação
    from app.api.routes_filtros_exportacao import filtros_bp, mapa_bp, export_bp

    app.register_blueprint(consultas_bp, url_prefix='/api/consulta')
    app.register_blueprint(analises_bp, url_prefix='/api/analise')
    app.register_blueprint(scoring_bp, url_prefix='/api/scoring')
    app.register_blueprint(integracoes_bp, url_prefix='/api/integracoes')
    app.register_blueprint(auth_bp, url_prefix='/api/auth')
    app.register_blueprint(admin_bp, url_prefix='/api/admin')
    app.register_blueprint(user_bp, url_prefix='/api/user')
    app.register_blueprint(payments_bp, url_prefix='/api/payments')
    app.register_blueprint(favoritos_bp) # Já tem url_prefix='/api/favoritos' no blueprint
    app.register_blueprint(b2g_enriquecida_bp, url_prefix='/api/b2g')
    app.register_blueprint(alertas_bp, url_prefix='/api/alertas')
    app.register_blueprint(notificacoes_bp, url_prefix='/api/notificacoes')
    app.register_blueprint(filtros_bp, url_prefix='/api/filtros')
    app.register_blueprint(mapa_bp, url_prefix='/api/mapa')
    app.register_blueprint(export_bp, url_prefix='/api/export')
    logger.info("✓ Blueprints registrados")

    # Error Handlers
    # Error Handlers
    from utils.utils_error_handler import register_error_handlers
    register_error_handlers(app)
    logger.info("✓ Error handlers registrados")

    # Rotas principais


    @app.route('/cnpj', methods=['GET'])
    def page_cnpj():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'cnpj.html'
        return send_file(static_path)

    @app.route('/compat', methods=['GET'])
    def page_compat():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'compat.html'
        return send_file(static_path)

    @app.route('/index2', methods=['GET'])
    def page_index2():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'index2.html'
        return send_file(static_path)

    @app.route('/health', methods=['GET'])
    def page_health():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'health.html'
        return send_file(static_path)

    @app.route('/setorial', methods=['GET'])
    def page_setorial():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'setorial.html'
        return send_file(static_path)

    @app.route('/home', methods=['GET'])
    def page_home():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'home.html'
        return send_file(static_path)

    @app.route('/oportunidades', methods=['GET'])
    def page_oportunidades():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'oportunidades.html'
        return send_file(static_path)

    @app.route('/oportunidades-b2b', methods=['GET'])
    def page_oportunidades_b2b():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'oportunidades-b2b.html'
        return send_file(static_path)

    @app.route('/', methods=['GET'])
    def root():
        # TEMPORÁRIO: Root serve direto o Dashboard (sem landing, sem login)
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'index.html'
        return send_file(static_path)

    @app.route('/app', methods=['GET'])
    # @login_required  # TEMPORÁRIO: Desabilitado para debug
    def app_dashboard():
        # /app serve o Dashboard (antigo index)
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'index.html'
        return send_file(static_path)

    @app.route('/login', methods=['GET'])
    def page_login():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'login.html'
        return send_file(static_path)

    # Rota antiga /landing pode redirecionar para / ou ser removida
    @app.route('/landing', methods=['GET'])
    def page_landing_legacy():
        return index()

    @app.route('/admin', methods=['GET'])
    def page_admin():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'admin.html'
        return send_file(static_path)

    @app.route('/recuperar-senha', methods=['GET'])
    def page_forgot_password():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'forgot_password.html'
        return send_file(static_path)
    
    @app.route('/reset-senha', methods=['GET'])
    def page_reset_password():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'reset_password.html'
        return send_file(static_path)

    @app.route('/termos', methods=['GET'])
    def page_terms():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'terms.html'
        return send_file(static_path)

    @app.route('/privacidade', methods=['GET'])
    def page_privacy():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'privacy.html'
        return send_file(static_path)

    @app.route('/contato', methods=['GET'])
    def page_contact():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'contact.html'
        return send_file(static_path)

    @app.route('/docs/api', methods=['GET'])
    def page_api_docs():
        static_path = Path(__file__).parent.parent / 'frontend' / 'static' / 'api_docs.html'
        return send_file(static_path)







    @app.route('/api', methods=['GET'])
    def api_info():
        """Informações da API"""
        return jsonify({
            "nome": "API de Análise de Dados da Receita",
            "versao": app.config['API_VERSION'],
            "status": "online",
            "documentacao": "/api/docs",
            "endpoints": {
                "consultas": {
                    "cnpj": "/api/consulta/cnpj/<cnpj>",
                    "palavra_chave": "/api/consulta/palavra_chave",
                    "socio": "/api/consulta/socio"
                },
                "analises": {
                    "setorial": "/api/analise/setorial",
                    "sugerir_cnaes": "/api/analise/cnaes/sugerir",
                    "scoring": {
                        "compatibilidade": "/api/scoring/compatibilidade",
                        "ranking": "/api/scoring/ranking",
                        "batch": "/api/scoring/batch",
                        "test": "/api/scoring/test"
                    }
                },
                "integracoes": {
                    "analise_360": "/api/integracoes/analise-360/<cnpj>",
                    "licitacoes": "/api/integracoes/licitacoes/cnpj/<cnpj>"
                }
            }
        })

    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check da aplicação"""
        from utils.utils_diagnostics import verificar_saude_sistema
        saude = verificar_saude_sistema()
        status_code = 200 if saude["status"] == "healthy" else 503
        return jsonify(saude), status_code

    @app.route('/api/status', methods=['GET'])
    def api_status():
        """Status detalhado do sistema"""
        from utils.utils_diagnostics import obter_metricas_sistema
        return jsonify(obter_metricas_sistema())

    @app.route('/api/routes', methods=['GET'])
    def api_routes():
        """Lista de rotas registradas (diagnóstico)"""
        rules = []
        try:
            for r in app.url_map.iter_rules():
                rules.append({ 'rule': str(r), 'endpoint': r.endpoint, 'methods': sorted(list(r.methods or [])) })
        except Exception as e:
            return jsonify({ 'erro': 'falha ao listar rotas', 'detalhes': str(e) }), 500
        return jsonify(sorted(rules, key=lambda x: x['rule']))

    @app.route('/api/docs', methods=['GET'])
    def api_docs():
        return (
            """
            <!doctype html>
            <html lang="pt-BR">
            <head>
              <meta charset="utf-8" />
              <meta name="viewport" content="width=device-width, initial-scale=1" />
              <title>Docs</title>
            </head>
            <body>
              <h2>Documentação</h2>
              <p>Use os endpoints da API listados na página inicial.</p>
            </body>
            </html>
            """
        )

    # try:
    #     import stripe
    #     stripe.api_key = os.getenv('STRIPE_SECRET_KEY')
    # except ImportError:
    #     pass

    # @app.route('/api/payments/config', methods=['GET'])
    # def payments_config():
    #     return jsonify({ 'publishableKey': os.getenv('STRIPE_PUBLISHABLE_KEY') })

    # @app.route('/api/payments/create-checkout-session', methods=['POST'])
    # def create_checkout_session():
    #     # if 'stripe' not in globals(): return jsonify({'erro': 'pagamentos desativados'}), 503
    #     data = request.get_json() or {}
    #     price_id = str(data.get('price_id') or '').strip()
    #     base_url = request.host_url.rstrip('/')
    #     success_url = data.get('success_url') or (base_url + '/?pagamento=ok')
    #     cancel_url = data.get('cancel_url') or (base_url + '/?pagamento=cancelado')
    #     if not price_id:
    #         return jsonify({ 'erro': 'price_id obrigatório' }), 400
    #     try:
    #         # session = stripe.checkout.Session.create(...)
    #         # return jsonify({ 'id': session.id, 'url': session.url })
    #         return jsonify({'erro': 'simulacao'}), 503
    #     except Exception:
    #         return jsonify({ 'erro': 'falha ao criar sessão' }), 500

    # @app.route('/api/payments/webhook', methods=['POST'])
    # def stripe_webhook():
    #     return jsonify({ 'ok': True })

    # Diagnóstico (somente em desenvolvimento)
    if app.config['DEBUG']:
        @app.route('/diagnostico/colunas', methods=['GET'])
        def diagnostico_colunas():
            """Diagnóstico de colunas dos arquivos Parquet"""
            from utils.utils_diagnostics import diagnosticar_colunas
            return diagnosticar_colunas()

    # Inicialização
    def inicializar():
        """Inicialização da aplicação"""
        logger.info("=== Inicializando aplicação ===")

        # Verifica arquivos necessários
        from utils.utils_validator import validar_arquivos_dados
        if not validar_arquivos_dados():
            logger.error("Arquivos de dados não encontrados!")

        # Pré-carrega dados essenciais
        from services.services_cache_service import pre_carregar_dados_essenciais
        pre_carregar_dados_essenciais()

        logger.info("=== Aplicação inicializada ===")

    inicializar()
    
    # Cria tabelas do banco de dados (dev only)
    with app.app_context():
        db.create_all()
        logger.info("✓ Tabelas do banco de dados verificadas/criadas")
        
    return app

if __name__ == '__main__':
    app = create_app(os.environ.get('FLASK_ENV', 'development'))

    logger.info(f"Iniciando servidor em {app.config['API_HOST']}:{app.config['API_PORT']}")
    logger.info(f"Debug mode: {app.config['DEBUG']}")

    app.run(
        host=app.config['API_HOST'],
        port=app.config['API_PORT'],
        debug=app.config['DEBUG'],
        threaded=True
    )

# ===================================================================
# ADICIONE NO SEU ARQUIVO DE ROTAS (routes.py ou app.py)
# ===================================================================

"""
@app.route('/api/analise/kpis/base', methods=['GET'])
def api_kpis_base():
    '''Retorna KPIs da base completa'''
    try:
        resultado = obter_kpis_base_completo()
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Erro no endpoint /api/analise/kpis/base: {e}")
        return jsonify({'erro': str(e)}), 500


@app.route('/api/analise/kpis/geral', methods=['GET'])
def api_kpis_geral():
    '''Retorna KPIs com filtros aplicados'''
    try:
        cnae = request.args.get('cnae')
        uf = request.args.get('uf')
        municipio = request.args.get('municipio')
        ano_min = request.args.get('ano_min')
        
        resultado = obter_kpis_geral_filtrado(
            cnae=cnae,
            uf=uf,
            municipio=municipio,
            ano_min=ano_min
        )
        
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Erro no endpoint /api/analise/kpis/geral: {e}")
        return jsonify({'erro': str(e)}), 500
"""
