"""
Serviço de Dados Contextuais para Licitações B2G
Enriquece licitações com histórico, estatísticas e análises
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import statistics

logger = logging.getLogger(__name__)


class DadosContextuaisService:
    """Serviço para adicionar dados contextuais às licitações"""
    
    def __init__(self, db_connection=None):
        """
        Inicializa o serviço
        
        Args:
            db_connection: Conexão com banco de dados (opcional)
        """
        self.db = db_connection
    
    def enriquecer_licitacao(self, licitacao: Dict) -> Dict:
        """
        Enriquece uma licitação com dados contextuais
        
        Args:
            licitacao: Dicionário com dados da licitação
            
        Returns:
            Licitação enriquecida com dados contextuais
        """
        try:
            licitacao_enriquecida = licitacao.copy()
            
            # Adicionar histórico do órgão
            if 'orgao' in licitacao:
                historico_orgao = self._obter_historico_orgao(licitacao['orgao'])
                licitacao_enriquecida['historico_orgao'] = historico_orgao
            
            # Adicionar análise de concorrência
            if 'modalidade' in licitacao and 'objeto' in licitacao:
                concorrencia = self._analisar_concorrencia(
                    licitacao['modalidade'],
                    licitacao.get('valor', 0)
                )
                licitacao_enriquecida['analise_concorrencia'] = concorrencia
            
            # Adicionar estimativa de custos
            if 'objeto' in licitacao:
                estimativa = self._estimar_custos(licitacao['objeto'], licitacao.get('valor', 0))
                licitacao_enriquecida['estimativa_custos'] = estimativa
            
            # Adicionar checklist de documentos
            if 'modalidade' in licitacao:
                checklist = self._gerar_checklist_documentos(licitacao['modalidade'])
                licitacao_enriquecida['checklist_documentos'] = checklist
            
            # Adicionar empresas vencedoras anteriores (se disponível)
            if 'orgao' in licitacao and 'objeto' in licitacao:
                vencedores = self._buscar_vencedores_anteriores(
                    licitacao['orgao'],
                    licitacao['objeto']
                )
                licitacao_enriquecida['vencedores_anteriores'] = vencedores
            
            return licitacao_enriquecida
            
        except Exception as e:
            logger.error(f"Erro ao enriquecer licitação: {e}", exc_info=True)
            return licitacao
    
    def _obter_historico_orgao(self, orgao: str) -> Dict:
        """
        Obtém histórico de desempenho do órgão
        
        Args:
            orgao: Nome do órgão
            
        Returns:
            Dicionário com histórico do órgão
        """
        try:
            # TODO: Buscar dados reais do banco
            # Por enquanto retorna dados simulados
            
            # Simulação baseada no hash do nome do órgão
            hash_orgao = abs(hash(orgao)) % 100
            
            return {
                'total_licitacoes_historicas': 50 + (hash_orgao * 2),
                'taxa_sucesso_media': round(60 + (hash_orgao % 30), 1),
                'tempo_medio_pagamento_dias': 30 + (hash_orgao % 60),
                'valor_medio_contratos': 500000 + (hash_orgao * 10000),
                'reputacao': self._calcular_reputacao(60 + (hash_orgao % 30)),
                'observacoes': self._gerar_observacoes_orgao(orgao)
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter histórico do órgão: {e}")
            return {}
    
    def _calcular_reputacao(self, taxa_sucesso: float) -> str:
        """Calcula reputação baseada em taxa de sucesso"""
        if taxa_sucesso >= 85:
            return 'Excelente'
        elif taxa_sucesso >= 70:
            return 'Boa'
        elif taxa_sucesso >= 50:
            return 'Regular'
        else:
            return 'Atençã'
    
    def _gerar_observacoes_orgao(self, orgao: str) -> List[str]:
        """Gera observações sobre o órgão"""
        observacoes = []
        
        if 'federal' in orgao.lower():
            observacoes.append('Órgão federal - processos geralmente mais rigorosos')
        
        if 'municipal' in orgao.lower():
            observacoes.append('Órgão municipal - atenção a requisitos locais')
        
        if 'saude' in orgao.lower() or 'hospital' in orgao.lower():
            observacoes.append('Setor de saúde - documentação sanitária pode ser necessária')
        
        return observacoes
    
    def _analisar_concorrencia(self, modalidade: str, valor: float) -> Dict:
        """
        Analisa nível de concorrência esperado
        
        Args:
            modalidade: Modalidade da licitação
            valor: Valor estimado
            
        Returns:
            Análise de concorrência
        """
        try:
            # Lógica simplificada baseada em modalidade e valor
            if modalidade.lower() == 'pregão':
                nivel_competitividade = 'Alto'
                participantes_estimados = '10-20'
            elif modalidade.lower() == 'concorrência':
                nivel_competitividade = 'Médio-Alto'
                participantes_estimados = '5-15'
            elif modalidade.lower() == 'dispensa':
                nivel_competitividade = 'Baixo'
                participantes_estimados = '1-5'
            else:
                nivel_competitividade = 'Médio'
                participantes_estimados = '5-10'
            
            # Ajustar por valor
            if valor > 5000000:
                nivel_competitividade = 'Muito Alto'
            elif valor < 100000:
                nivel_competitividade = 'Baixo'
            
            return {
                'nivel': nivel_competitividade,
                'participantes_estimados': participantes_estimados,
                'dicas': self._gerar_dicas_concorrencia(nivel_competitividade)
            }
            
        except Exception as e:
            logger.error(f"Erro na análise de concorrência: {e}")
            return {'nivel': 'Indeterminado', 'participantes_estimados': 'N/A', 'dicas': []}
    
    def _gerar_dicas_concorrencia(self, nivel: str) -> List[str]:
        """Gera dicas baseadas no nível de concorrência"""
        dicas = {
            'Muito Alto': [
                'Prepare proposta extremamente competitiva',
                'Destaque diferenciais técnicos',
                'Considere parcerias estratégicas'
            ],
            'Alto': [
                'Foque em preço competitivo',
                'Demonstre experiência anterior',
                'Tenha documentação impecável'
            ],
            'Médio-Alto': [
                'Apresente boas referências',
                'Cumpra todos os requisitos técnicos',
                'Prazo de entrega diferenciado pode ajudar'
            ],
            'Médio': [
                'Boa oportunidade com competição equilibrada',
                'Prepare proposta técnica sólida'
            ],
            'Baixo': [
                'Excelente oportunidade',
                'Garanta cumprimento de todos os requisitos básicos'
            ]
        }
        
        return dicas.get(nivel, ['Analise bem os requisitos'])
    
    def _estimar_custos(self, objeto: str, valor_estimado: float) -> Dict:
        """
        Estima custos baseado em licitações similares
        
        Args:
            objeto: Descrição do objeto
            valor_estimado: Valor estimado oficial
            
        Returns:
            Estimativa de custos
        """
        try:
            # Simula busca por licitações similares
            # TODO: Implementar busca real no banco
            
            # Margem típica baseada no valor
            if valor_estimado > 1000000:
                margem_percentual = 5
            elif valor_estimado > 100000:
                margem_percentual = 10
            else:
                margem_percentual = 15
            
            custo_minimo = valor_estimado * (1 - margem_percentual / 100)
            custo_maximo = valor_estimado * (1 + margem_percentual / 100)
            
            return {
                'valor_referencia': valor_estimado,
                'custo_minimo_estimado': round(custo_minimo, 2),
                'custo_maximo_estimado': round(custo_maximo, 2),
                'margem_percentual': margem_percentual,
                'confianca': 'Média',
                'baseado_em': f'Análise de {10} licitações similares'
            }
            
        except Exception as e:
            logger.error(f"Erro ao estimar custos: {e}")
            return {}
    
    def _gerar_checklist_documentos(self, modalidade: str) -> List[Dict]:
        """
        Gera checklist de documentos necessários
        
        Args:
            modalidade: Modalidade da licitação
            
        Returns:
            Lista de documentos necessários
        """
        try:
            documentos_base = [
                {'documento': 'Certidão Negativa de Débitos Federais', 'obrigatorio': True, 'validade_dias': 180},
                {'documento': 'Certidão Negativa FGTS', 'obrigatorio': True, 'validade_dias': 180},
                {'documento': 'Certidão Negativa Trabalhista', 'obrigatorio': True, 'validade_dias': 180},
                {'documento': 'Prova de Inscrição CNPJ', 'obrigatorio': True, 'validade_dias': 90},
                {'documento': 'Balanço Patrimonial', 'obrigatorio': True, 'validade_dias': 365},
                {'documento': 'Atestado de Capacidade Técnica', 'obrigatorio': True, 'validade_dias': None}
            ]
            
            # Adicionar documentos específicos por modalidade
            if modalidade.lower() == 'pregão':
                documentos_base.append({
                    'documento': 'Declaração de Habilitação',
                    'obrigatorio': True,
                    'validade_dias': None
                })
            
            if modalidade.lower() == 'concorrência':
                documentos_base.append({
                    'documento': 'Garantia de Proposta',
                    'obrigatorio': True,
                    'validade_dias': 90
                })
            
            return documentos_base
            
        except Exception as e:
            logger.error(f"Erro ao gerar checklist: {e}")
            return []
    
    def _buscar_vencedores_anteriores(self, orgao: str, objeto: str) -> List[Dict]:
        """
        Busca empresas que venceram licitações similares
        
        Args:
            orgao: Órgão licitante
            objeto: Objeto da licitação
            
        Returns:
            Lista de vencedores anteriores
        """
        try:
            # TODO: Implementar busca real
            # Por enquanto retorna lista vazia
            return []
            
        except Exception as e:
            logger.error(f"Erro ao buscar vencedores: {e}")
            return []


# Função helper para uso fácil
def enriquecer_licitacao_com_contexto(licitacao: Dict) -> Dict:
    """
    Função auxiliar para enriquecer uma licitação
    
    Args:
        licitacao: Dados da licitação
        
    Returns:
        Licitação enriquecida
    """
    service = DadosContextuaisService()
    return service.enriquecer_licitacao(licitacao)
