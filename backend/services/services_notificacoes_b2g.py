"""
Serviço de Notificações B2G
Gerencia criação, envio e marcação de notificações
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class NotificacoesB2GService:
    """Serviço para gerenciamento de notificações"""
    
    TIPOS_NOTIFICACAO = {
        'novo_match': 'Nova oportunidade com alto match',
        'prazo_proximo': 'Prazo de licitação próximo',
        'alerta_personalizado': 'Alerta personalizado disparado',
        'status_mudou': 'Status da licitação alterado',
        'favorito_atualizado': 'Licitação favoritada foi atualizada'
    }
    
    def __init__(self, db_connection=None):
        """
        Inicializa o serviço
        
        Args:
            db_connection: Conexão com banco de dados
        """
        self.db = db_connection
    
    def criar_notificacao(
        self,
        usuario_id: int,
        tipo: str,
        titulo: str,
        mensagem: str,
        dados: Optional[Dict] = None,
        link: Optional[str] = None
    ) -> Dict:
        """
        Cria uma nova notificação
        
        Args:
            usuario_id: ID do usuário
            tipo: Tipo da notificação
            titulo: Título
            mensagem: Mensagem
            dados: Dados adicionais (JSON)
            link: Link de ação
            
        Returns:
            Notificação criada
        """
        try:
            if not self.db:
                logger.error("Conexão com BD não disponível")
                return {}
            
            agora = datetime.now().isoformat()
            
            cursor = self.db.cursor()
            cursor.execute("""
                INSERT INTO notificacoes (
                    usuario_id, tipo, titulo, mensagem, dados, link,
                    lida, criado_em
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                usuario_id,
                tipo,
                titulo,
                mensagem,
                json.dumps(dados) if dados else None,
                link,
                0,  # não lida
                agora
            ))
            
            self.db.commit()
            notif_id = cursor.lastrowid
            
            logger.info(f"Notificação {notif_id} criada para usuário {usuario_id}")
            
            return {
                'id': notif_id,
                'usuario_id': usuario_id,
                'tipo': tipo,
                'titulo': titulo,
                'mensagem': mensagem,
                'dados': dados,
                'link': link,
                'lida': False,
                'criado_em': agora
            }
            
        except Exception as e:
            logger.error(f"Erro ao criar notificação: {e}", exc_info=True)
            if self.db:
                self.db.rollback()
            return {}
    
    def listar_notificacoes(
        self,
        usuario_id: int,
        apenas_nao_lidas: bool = False,
        limite: int = 50,
        offset: int = 0
    ) -> Dict:
        """
        Lista notificações do usuário
        
        Args:
            usuario_id: ID do usuário
            apenas_nao_lidas: Se True, retorna apenas não lidas
            limite: Limite de resultados
            offset: Offset para paginação
            
        Returns:
            Dict com notificações e metadados
        """
        try:
            if not self.db:
                return {'notificacoes': [], 'total': 0, 'nao_lidas': 0}
            
            cursor = self.db.cursor()
            
            # Query base
            if apenas_nao_lidas:
                where_clause = "WHERE usuario_id = ? AND lida = 0"
            else:
                where_clause = "WHERE usuario_id = ?"
            
            # Contar total
            cursor.execute(f"""
                SELECT COUNT(*) FROM notificacoes {where_clause}
            """, (usuario_id,))
            total = cursor.fetchone()[0]
            
            # Contar não lidas
            cursor.execute("""
                SELECT COUNT(*) FROM notificacoes
                WHERE usuario_id = ? AND lida = 0
            """, (usuario_id,))
            nao_lidas = cursor.fetchone()[0]
            
            # Buscar notificações
            cursor.execute(f"""
                SELECT id, tipo, titulo, mensagem, dados, link, lida, criado_em
                FROM notificacoes
                {where_clause}
                ORDER BY criado_em DESC
                LIMIT ? OFFSET ?
            """, (usuario_id, limite, offset))
            
            notificacoes = []
            for row in cursor.fetchall():
                notificacoes.append({
                    'id': row[0],
                    'tipo': row[1],
                    'titulo': row[2],
                    'mensagem': row[3],
                    'dados': json.loads(row[4]) if row[4] else None,
                    'link': row[5],
                    'lida': bool(row[6]),
                    'criado_em': row[7]
                })
            
            return {
                'notificacoes': notificacoes,
                'total': total,
                'nao_lidas': nao_lidas,
                'pagina': {
                    'limite': limite,
                    'offset': offset,
                    'tem_mais': (offset + limite) < total
                }
            }
            
        except Exception as e:
            logger.error(f"Erro ao listar notificações: {e}")
            return {'notificacoes': [], 'total': 0, 'nao_lidas': 0}
    
    def marcar_como_lida(self, notificacao_id: int, usuario_id: int) -> bool:
        """
        Marca notificação como lida
        
        Args:
            notificacao_id: ID da notificação
            usuario_id: ID do usuário (para validação)
            
        Returns:
            True se marcada com sucesso
        """
        try:
            if not self.db:
                return False
            
            cursor = self.db.cursor()
            cursor.execute("""
                UPDATE notificacoes
                SET lida = 1
                WHERE id = ? AND usuario_id = ?
            """, (notificacao_id, usuario_id))
            
            self.db.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Erro ao marcar notificação: {e}")
            if self.db:
                self.db.rollback()
            return False
    
    def marcar_todas_como_lidas(self, usuario_id: int) -> int:
        """
        Marca todas notificações do usuário como lidas
        
        Args:
            usuario_id: ID do usuário
            
        Returns:
            Número de notificações marcadas
        """
        try:
            if not self.db:
                return 0
            
            cursor = self.db.cursor()
            cursor.execute("""
                UPDATE notificacoes
                SET lida = 1
                WHERE usuario_id = ? AND lida = 0
            """, (usuario_id,))
            
            self.db.commit()
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Erro ao marcar todas: {e}")
            if self.db:
                self.db.rollback()
            return 0
    
    def deletar_notificacao(self, notificacao_id: int, usuario_id: int) -> bool:
        """
        Deleta uma notificação
        
        Args:
            notificacao_id: ID da notificação
            usuario_id: ID do usuário
            
        Returns:
            True se deletada
        """
        try:
            if not self.db:
                return False
            
            cursor = self.db.cursor()
            cursor.execute("""
                DELETE FROM notificacoes
                WHERE id = ? AND usuario_id = ?
            """, (notificacao_id, usuario_id))
            
            self.db.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Erro ao deletar notificação: {e}")
            if self.db:
                self.db.rollback()
            return False
    
    def criar_notificacao_prazo_proximo(
        self,
        usuario_id: int,
        licitacao: Dict,
        dias_restantes: int
    ) -> Dict:
        """
        Cria notificação de prazo próximo
        
        Args:
            usuario_id: ID do usuário
            licitacao: Dados da licitação
            dias_restantes: Dias restantes
            
        Returns:
            Notificação criada
        """
        urgencia = "🔥 URGENTE" if dias_restantes <= 3 else "⚠️ Atenção"
        
        titulo = f"{urgencia}: Prazo próximo - {dias_restantes} dias"
        mensagem = f"A licitação '{licitacao.get('titulo', 'N/A')[:60]}...' encerra em {dias_restantes} dias!"
        
        return self.criar_notificacao(
            usuario_id=usuario_id,
            tipo='prazo_proximo',
            titulo=titulo,
            mensagem=mensagem,
            dados={
                'licitacao_id': licitacao.get('id'),
                'dias_restantes': dias_restantes,
                'orgao': licitacao.get('orgao')
            },
            link=f"/b2g/licitacao/{licitacao.get('id')}"
        )
    
    def criar_notificacao_novo_match(
        self,
        usuario_id: int,
        licitacao: Dict,
        match_score: int
    ) -> Dict:
        """
        Cria notificação de novo match alto
        
        Args:
            usuario_id: ID do usuário
            licitacao: Dados da licitação
            match_score: Score de compatibilidade
            
        Returns:
            Notificação criada
        """
        titulo = f"🎯 Nova oportunidade com {match_score}% de match!"
        mensagem = f"Encontramos uma licitação altamente compatível: {licitacao.get('titulo', 'N/A')[:60]}..."
        
        return self.criar_notificacao(
            usuario_id=usuario_id,
            tipo='novo_match',
            titulo=titulo,
            mensagem=mensagem,
            dados={
                'licitacao_id': licitacao.get('id'),
                'match_score': match_score,
                'valor': licitacao.get('valor')
            },
            link=f"/b2g/licitacao/{licitacao.get('id')}"
        )
    
    def criar_notificacao_alerta(
        self,
        usuario_id: int,
        alerta_nome: str,
        total_matches: int
    ) -> Dict:
        """
        Cria notificação de alerta disparado
        
        Args:
            usuario_id: ID do usuário
            alerta_nome: Nome do alerta
            total_matches: Total de licitações encontradas
            
        Returns:
            Notificação criada
        """
        titulo = f"🔔 Alerta '{alerta_nome}' disparado"
        mensagem = f"Encontramos {total_matches} nova(s) licitação(ões) que atendem aos seus critérios!"
        
        return self.criar_notificacao(
            usuario_id=usuario_id,
            tipo='alerta_personalizado',
            titulo=titulo,
            mensagem=mensagem,
            dados={
                'alerta_nome': alerta_nome,
                'total_matches': total_matches
            },
            link="/b2g/alertas"
        )


# Função helper
def notificar_usuario(usuario_id: int, tipo: str, titulo: str, mensagem: str, db=None) -> Dict:
    """
    Função auxiliar para criar notificação
    
    Args:
        usuario_id: ID do usuário
        tipo: Tipo da notificação
        titulo: Título
        mensagem: Mensagem
        db: Conexão com banco
        
    Returns:
        Notificação criada
    """
    service = NotificacoesB2GService(db)
    return service.criar_notificacao(usuario_id, tipo, titulo, mensagem)
