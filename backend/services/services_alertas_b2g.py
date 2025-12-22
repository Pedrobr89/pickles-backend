"""
Serviço de Alertas B2G
Gerencia alertas personalizados e triggers automáticos
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
import re

logger = logging.getLogger(__name__)


class AlertasB2GService:
    """Serviço para gerenciamento de alertas de licitações"""
    
    def __init__(self, db_connection=None):
        """
        Inicializa o serviço
        
        Args:
            db_connection: Conexão com banco de dados
        """
        self.db = db_connection
    
    def criar_alerta(
        self,
        usuario_id: int,
        nome: str,
        criterios: Dict,
        frequencia: str = 'imediato',
        canais: List[str] = None
    ) -> Dict:
        """
        Cria um novo alerta personalizado
        
        Args:
            usuario_id: ID do usuário
            nome: Nome descritivo do alerta
            criterios: Critérios de filtragem
            frequencia: Frequência de notificação (imediato, diario, semanal)
            canais: Canais de comunicação (email, sms, push, in_app)
            
        Returns:
            Alerta criado
        """
        try:
            if not self.db:
                logger.error("Conexão com BD não disponível")
                return {}
            
            if canais is None:
                canais = ['in_app', 'email']
            
            # Validar critérios
            criterios_validados = self._validar_criterios(criterios)
            
            # Gerar query SQL de inserção
            agora = datetime.now().isoformat()
            
            cursor = self.db.cursor()
            cursor.execute("""
                INSERT INTO alertas_b2g (
                    usuario_id, nome, criterios, frequencia, canais,
                    ativo, criado_em, ultima_verificacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                usuario_id,
                nome,
                json.dumps(criterios_validados),
                frequencia,
                json.dumps(canais),
                1,  # ativo
                agora,
                agora
            ))
            
            self.db.commit()
            alerta_id = cursor.lastrowid
            
            logger.info(f"Alerta {alerta_id} criado para usuário {usuario_id}")
            
            return {
                'id': alerta_id,
                'usuario_id': usuario_id,
                'nome': nome,
                'criterios': criterios_validados,
                'frequencia': frequencia,
                'canais': canais,
                'ativo': True,
                'criado_em': agora
            }
            
        except Exception as e:
            logger.error(f"Erro ao criar alerta: {e}", exc_info=True)
            if self.db:
                self.db.rollback()
            return {}
    
    def listar_alertas(self, usuario_id: int, apenas_ativos: bool = True) -> List[Dict]:
        """
        Lista alertas do usuário
        
        Args:
            usuario_id: ID do usuário
            apenas_ativos: Se True, retorna apenas alertas ativos
            
        Returns:
            Lista de alertas
        """
        try:
            if not self.db:
                return []
            
            cursor = self.db.cursor()
            
            if apenas_ativos:
                cursor.execute("""
                    SELECT id, nome, criterios, frequencia, canais, ativo, 
                           criado_em, ultima_verificacao, total_disparos
                    FROM alertas_b2g
                    WHERE usuario_id = ? AND ativo = 1
                    ORDER BY criado_em DESC
                """, (usuario_id,))
            else:
                cursor.execute("""
                    SELECT id, nome, criterios, frequencia, canais, ativo,
                           criado_em, ultima_verificacao, total_disparos
                    FROM alertas_b2g
                    WHERE usuario_id = ?
                    ORDER BY criado_em DESC
                """, (usuario_id,))
            
            alertas = []
            for row in cursor.fetchall():
                alertas.append({
                    'id': row[0],
                    'nome': row[1],
                    'criterios': json.loads(row[2]) if row[2] else {},
                    'frequencia': row[3],
                    'canais': json.loads(row[4]) if row[4] else [],
                    'ativo': bool(row[5]),
                    'criado_em': row[6],
                    'ultima_verificacao': row[7],
                    'total_disparos': row[8] or 0
                })
            
            return alertas
            
        except Exception as e:
            logger.error(f"Erro ao listar alertas: {e}")
            return []
    
    def atualizar_alerta(
        self,
        alerta_id: int,
        usuario_id: int,
        dados: Dict
    ) -> bool:
        """
        Atualiza um alerta existente
        
        Args:
            alerta_id: ID do alerta
            usuario_id: ID do usuário (para validação)
            dados: Dados a atualizar
            
        Returns:
            True se atualizado com sucesso
        """
        try:
            if not self.db:
                return False
            
            campos_permitidos = ['nome', 'criterios', 'frequencia', 'canais', 'ativo']
            updates = []
            valores = []
            
            for campo in campos_permitidos:
                if campo in dados:
                    if campo in ['criterios', 'canais']:
                        updates.append(f"{campo} = ?")
                        valores.append(json.dumps(dados[campo]))
                    elif campo == 'ativo':
                        updates.append(f"{campo} = ?")
                        valores.append(1 if dados[campo] else 0)
                    else:
                        updates.append(f"{campo} = ?")
                        valores.append(dados[campo])
            
            if not updates:
                return True  # Nada para atualizar
            
            valores.extend([alerta_id, usuario_id])
            
            sql = f"""
                UPDATE alertas_b2g
                SET {', '.join(updates)}
                WHERE id = ? AND usuario_id = ?
            """
            
            cursor = self.db.cursor()
            cursor.execute(sql, valores)
            self.db.commit()
            
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Erro ao atualizar alerta: {e}")
            if self.db:
                self.db.rollback()
            return False
    
    def deletar_alerta(self, alerta_id: int, usuario_id: int) -> bool:
        """
        Delete um alerta
        
        Args:
            alerta_id: ID do alerta
            usuario_id: ID do usuário
            
        Returns:
            True se deletado
        """
        try:
            if not self.db:
                return False
            
            cursor = self.db.cursor()
            cursor.execute("""
                DELETE FROM alertas_b2g
                WHERE id = ? AND usuario_id = ?
            """, (alerta_id, usuario_id))
            
            self.db.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Erro ao deletar alerta: {e}")
            if self.db:
                self.db.rollback()
            return False
    
    def verificar_alertas(self, usuario_id: Optional[int] = None) -> List[Dict]:
        """
        Verifica alertas e retorna matches
        
        Args:
            usuario_id: Se especificado, verifica apenas deste usuário
            
        Returns:
            Lista de matches {alerta, licitacoes}
        """
        try:
            if not self.db:
                return []
            
            # Buscar alertas ativos para verificar
            cursor = self.db.cursor()
            
            if usuario_id:
                cursor.execute("""
                    SELECT id, usuario_id, nome, criterios, frequencia, 
                           canais, ultima_verificacao
                    FROM alertas_b2g
                    WHERE usuario_id = ? AND ativo = 1
                """, (usuario_id,))
            else:
                cursor.execute("""
                    SELECT id, usuario_id, nome, criterios, frequencia,
                           canais, ultima_verificacao
                    FROM alertas_b2g
                    WHERE ativo = 1
                """)
            
            alertas = cursor.fetchall()
            matches = []
            
            for alerta_row in alertas:
                alerta_id, uid, nome, criterios_json, freq, canais_json, ultima_verif = alerta_row
                
                criterios = json.loads(criterios_json) if criterios_json else {}
                canais = json.loads(canais_json) if canais_json else []
                
                # Verificar se deve disparar baseado na frequência
                if not self._deve_verificar(freq, ultima_verif):
                    continue
                
                # Simular busca de licitações (em produção, buscar do PNCP)
                licitacoes_match = self._buscar_licitacoes_por_criterios(criterios)
                
                if licitacoes_match:
                    matches.append({
                        'alerta_id': alerta_id,
                        'usuario_id': uid,
                        'nome_alerta': nome,
                        'criterios': criterios,
                        'canais': canais,
                        'total_matches': len(licitacoes_match),
                        'licitacoes': licitacoes_match[:10]  # Limitar preview
                    })
                    
                    # Atualizar última verificação
                    self._atualizar_ultima_verificacao(alerta_id)
            
            return matches
            
        except Exception as e:
            logger.error(f"Erro ao verificar alertas: {e}")
            return []
    
    def _validar_criterios(self, criterios: Dict) -> Dict:
        """Valida e normaliza critérios de alerta"""
        criterios_validos = {}
        
        # Campos permitidos
        campos_permitidos = {
            'palavras_chave': str,
            'valor_minimo': (int, float),
            'valor_maximo': (int, float),
            'ufs': list,
            'modalidades': list,
            'cnaes': list,
            'orgaos': list,
            'prazo_minimo_dias': int,
            'prazo_maximo_dias': int
        }
        
        for campo, tipo in campos_permitidos.items():
            if campo in criterios:
                valor = criterios[campo]
                
                # Validar tipo
                if isinstance(tipo, tuple):
                    if not isinstance(valor, tipo):
                        continue
                else:
                    if not isinstance(valor, tipo):
                        continue
                
                criterios_validos[campo] = valor
        
        return criterios_validos
    
    def _deve_verificar(self, frequencia: str, ultima_verificacao: str) -> bool:
        """Verifica se deve disparar alerta baseado na frequência"""
        try:
            if frequencia == 'imediato':
                return True
            
            if not ultima_verificacao:
                return True
            
            ultima = datetime.fromisoformat(ultima_verificacao)
            agora = datetime.now()
            delta = agora - ultima
            
            if frequencia == 'diario':
                return delta.total_seconds() >= 24 * 3600
            elif frequencia == 'semanal':
                return delta.total_seconds() >= 7 * 24 * 3600
            
            return True
            
        except Exception:
            return True
    
    def _buscar_licitacoes_por_criterios(self, criterios: Dict) -> List[Dict]:
        """
        Busca licitações que atendem aos critérios
        
        NOTA: Em produção, integrar com PNCP ou banco de licitações
        """
        # Simulação - retornar lista vazia por enquanto
        # TODO: Implementar busca real
        return []
    
    def _atualizar_ultima_verificacao(self, alerta_id: int):
        """Atualiza timestamp da última verificação"""
        try:
            if not self.db:
                return
            
            cursor = self.db.cursor()
            cursor.execute("""
                UPDATE alertas_b2g
                SET ultima_verificacao = ?,
                    total_disparos = COALESCE(total_disparos, 0) + 1
                WHERE id = ?
            """, (datetime.now().isoformat(), alerta_id))
            
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Erro ao atualizar verificação: {e}")


# Função helper
def criar_alerta_usuario(usuario_id: int, nome: str, criterios: Dict, db=None) -> Dict:
    """
    Função auxiliar para criar alerta
    
    Args:
        usuario_id: ID do usuário
        nome: Nome do alerta
        criterios: Critérios de filtragem
        db: Conexão com banco
        
    Returns:
        Alerta criado
    """
    service = AlertasB2GService(db)
    return service.criar_alerta(usuario_id, nome, criterios)
