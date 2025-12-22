"""
Serviço de Análise de Parcerias B2G (Sprint 5)
Matching de empresas complementares e sugestão de consórcios
"""

import logging
from typing import Dict, List, Optional
import sqlite3

logger = logging.getLogger(__name__)


class ParceriasB2GService:
    """Serviço para análise de parcerias e consórcios"""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
    
    def buscar_parceiros_complementares(
        self,
        empresa_cnpj: str,
        licitacao: Dict,
        limite: int = 10
    ) -> List[Dict]:
        """
        Busca empresas complementares para consórcio
        
        Args:
            empresa_cnpj: CNPJ da empresa principal
            licitacao: Dados da licitação
            limite: Máximo de resultados
            
        Returns:
            Lista de empresas parceiras potenciais
        """
        try:
            # Critérios de complementaridade
            valor_licitacao = licitacao.get('valor', 0)
            uf_licitacao = licitacao.get('uf')
            
            parceiros = []
            
            # Simular busca (em produção, buscar no banco)
            # Critérios: porte adequado, mesma UF ou próxima, CNAEs complementares
            
            for i in range(min(limite, 5)):
                parceiros.append({
                    'cnpj': f'00000000{i:06d}00',
                    'razao_social': f'Empresa Parceira {i+1}',
                    'uf': uf_licitacao,
                    'porte': 'Pequena' if i % 2 == 0 else 'Média',
                    'score_complementaridade': 85 - (i * 5),
                    'motivo': self._gerar_motivo_parceria(i),
                    'capacidade_tecnica': True,
                    'historico_consorcios': i * 2
                })
            
            return parceiros
            
        except Exception as e:
            logger.error(f"Erro ao buscar parceiros: {e}")
            return []
    
    def _gerar_motivo_parceria(self, idx: int) -> str:
        """Gera motivo de complementaridade"""
        motivos = [
            'CNAE complementar ao objeto da licitação',
            'Capacidade técnica em área específica',
            'Experiência prévia com o órgão licitante',
            'Certificações relevantes',
            'Estrutura logística na região'
        ]
        return motivos[idx % len(motivos)]
    
    def analisar_viabilidade_consorcio(
        self,
        empresas: List[str],
        licitacao: Dict
    ) -> Dict:
        """
        Analisa viabilidade de consórcio entre empresas
        
        Args:
            empresas: Lista de CNPJs
            licitacao: Dados da licitação
            
        Returns:
            Análise de viabilidade
        """
        try:
            valor_licitacao = licitacao.get('valor', 0)
            
            # Análise simplificada
            score_viabilidade = 80  # Base
            
            # Fatores que aumentam viabilidade
            if len(empresas) >= 2 and len(empresas) <= 4:
                score_viabilidade += 10
            
            if valor_licitacao > 1000000:
                score_viabilidade += 5
            
            vantagens = [
                'Combinação de capacidades técnicas',
                'Divisão de riscos financeiros',
                'Maior competitividade na proposta',
                'Atendimento a requisitos mínimos'
            ]
            
            riscos = [
                'Necessidade de acordo formal',
                'Compartilhamento de responsabilidades',
                'Coordenação entre empresas'
            ]
            
            return {
                'viavel': score_viabilidade >= 70,
                'score': score_viabilidade,
                'total_empresas': len(empresas),
                'valor_licitacao': valor_licitacao,
                'vantagens': vantagens,
                'riscos': riscos,
                'recomendacao': 'Consórcio recomendado' if score_viabilidade >= 80 else 'Avaliar com cuidado'
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar consórcio: {e}")
            return {'viavel': False, 'score': 0}
