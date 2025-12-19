"""
Tratamento de erros e validações
"""

from flask import jsonify
from functools import wraps
import logging

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Erro de validação de dados"""
    pass

class NotFoundError(Exception):
    """Erro de recurso não encontrado"""
    pass

class InternalServerError(Exception):
    """Erro interno do servidor"""
    pass

def handle_errors(f):
    """
    Decorator para tratamento de erros
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValidationError as e:
            logger.warning(f"ValidationError: {e}")
            return jsonify({"erro": str(e)}), 400
        except NotFoundError as e:
            logger.warning(f"NotFoundError: {e}")
            return jsonify({"erro": str(e)}), 404
        except Exception as e:
            logger.error(f"Erro inesperado: {e}", exc_info=True)
            return jsonify({"erro": "Erro interno do servidor"}), 500
    return wrapper

def register_error_handlers(app):
    """
    Registra handlers de erro globais
    """
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"erro": "Requisição inválida", "detalhes": str(e)}), 400

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"erro": "Recurso não encontrado"}), 404

    @app.errorhandler(500)
    def internal_error(e):
        logger.error(f"Erro interno: {e}", exc_info=True)
        return jsonify({"erro": "Erro interno do servidor"}), 500

def validar_lista_nao_vazia(lista: list, nome: str) -> None:
    """
    Valida se uma lista não está vazia
    """
    if not lista or not isinstance(lista, list):
        raise ValidationError(f"O parâmetro '{nome}' deve ser uma lista não vazia")
