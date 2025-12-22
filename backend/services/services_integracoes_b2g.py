"""
Serviço de Integrações Externas B2G (Sprint 7)
Webhooks e integrações com APIs externas
"""

import logging
from typing import Dict, List, Optional
import requests
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class IntegracoesB2GService:
    """Serviço para integrações externas"""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
    
    def registrar_webhook(
        self,
        usuario_id: int,
        url: str,
        eventos: List[str],
        secret: Optional[str] = None
    ) -> Dict:
        """
        Registra webhook para eventos B2G
        
        Args:
            usuario_id: ID do usuário
            url: URL do webhook
            eventos: Lista de eventos (nova_licitacao, prazo_proximo, etc)
            secret: Secret para assinatura HMAC
            
        Returns:
            Webhook registrado
        """
        try:
            if not self.db:
                return {}
            
            agora = datetime.now().isoformat()
            
            cursor = self.db.cursor()
            cursor.execute("""
                INSERT INTO webhooks_b2g (
                    usuario_id, url, eventos, secret, ativo, criado_em
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                usuario_id,
                url,
                json.dumps(eventos),
                secret,
                1,
                agora
            ))
            
            self.db.commit()
            webhook_id = cursor.lastrowid
            
            return {
                'id': webhook_id,
                'usuario_id': usuario_id,
                'url': url,
                'eventos': eventos,
                'ativo': True,
                'criado_em': agora
            }
            
        except Exception as e:
            logger.error(f"Erro ao registrar webhook: {e}")
            if self.db:
                self.db.rollback()
            return {}
    
    def disparar_webhook(
        self,
        webhook_id: int,
        evento: str,
        dados: Dict
    ) -> bool:
        """
        Dispara webhook para um evento
        
        Args:
            webhook_id: ID do webhook
            evento: Tipo de evento
            dados: Dados do evento
            
        Returns:
            True se enviado com sucesso
        """
        try:
            if not self.db:
                return False
            
            # Buscar webhook
            cursor = self.db.cursor()
            cursor.execute("""
                SELECT url, secret FROM webhooks_b2g
                WHERE id = ? AND ativo = 1
            """, (webhook_id,))
            
            row = cursor.fetchone()
            if not row:
                logger.warning(f"Webhook {webhook_id} não encontrado ou inativo")
                return False
            
            url, secret = row
            
            # Preparar payload
            payload = {
                'evento': evento,
                'timestamp': datetime.now().isoformat(),
                'dados': dados
            }
            
            # Enviar POST
            headers = {'Content-Type': 'application/json'}
            if secret:
                # TODO: Adicionar assinatura HMAC
                headers['X-Webhook-Secret'] = secret
            
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=10
            )
            
            sucesso = response.status_code in [200, 201, 202, 204]
            
            # Registrar tentativa
            cursor.execute("""
                UPDATE webhooks_b2g
                SET ultima_tentativa = ?,
                    ultimo_status = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), response.status_code, webhook_id))
            
            self.db.commit()
            
            if sucesso:
                logger.info(f"Webhook {webhook_id} disparado com sucesso")
            else:
                logger.warning(f"Webhook {webhook_id} falhou: {response.status_code}")
            
            return sucesso
            
        except Exception as e:
            logger.error(f"Erro ao disparar webhook: {e}")
            return False
    
    def sincronizar_pncp_realtime(self, filtros: Optional[Dict] = None) -> Dict:
        """
        Sincroniza dados do PNCP em tempo real
        
        Args:
            filtros: Filtros opcionais
            
        Returns:
            Resultado da sincronização
        """
        try:
            # Integração com API real do PNCP
            # TODO: Implementar chamada real
            
            logger.info("Sincronização PNCP iniciada")
            
            return {
                'sucesso': True,
                'total_novas': 0,
                'total_atualizadas': 0,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na sincronização PNCP: {e}")
            return {'sucesso': False, 'erro': str(e)}


# Adicionar tabelas na migration
"""
CREATE TABLE IF NOT EXISTS webhooks_b2g (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    eventos TEXT NOT NULL,
    secret TEXT,
    ativo INTEGER DEFAULT 1,
    criado_em TEXT NOT NULL,
    ultima_tentativa TEXT,
    ultimo_status INTEGER,
    FOREIGN KEY (usuario_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_webhooks_usuario
ON webhooks_b2g(usuario_id);
"""
