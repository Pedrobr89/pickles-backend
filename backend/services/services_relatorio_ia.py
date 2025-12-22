"""
Serviço de Relatórios com IA para Licitações B2G
Gera relatórios executivos e insights automatizados
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class RelatorioIAService:
    """Serviço para geração de relatórios e insights com IA"""
    
    def __init__(self):
        """Inicializa o serviço"""
        pass
    
    def gerar_relatorio_executivo(
        self,
        empresa_data: Dict,
        licitacoes: List[Dict],
        match_scores: List[Dict]
    ) -> Dict:
        """
        Gera relatório executivo automático
        
        Args:
            empresa_data: Dados da empresa
            licitacoes: Lista de licitações analisadas
            match_scores: Scores de match calculados
            
        Returns:
            Relatório executivo estruturado
        """
        try:
            # Análise geral
            total_oportunidades = len(licitacoes)
            oportunidades_altas = len([m for m in match_scores if m.get('score', 0) >= 80])
            oportunidades_medias = len([m for m in match_scores if 60 <= m.get('score', 0) < 80])
            
            valor_total = sum(l.get('valor', 0) for l in licitacoes)
            valor_alto_match = sum(
                l.get('valor', 0) for i, l in enumerate(licitacoes)
                if i < len(match_scores) and match_scores[i].get('score', 0) >= 80
            )
            
            # Análise por região
            analise_regional = self._analisar_distribuicao_regional(licitacoes)
            
            # Análise por modalidade
            analise_modalidade = self._analisar_distribuicao_modalidade(licitacoes)
            
            # Recomendações prioritárias
            recomendacoes = self._gerar_recomendacoes(
                empresa_data,
                licitacoes,
                match_scores
            )
            
            # Score de viabilidade geral
            score_viabilidade = self._calcular_score_viabilidade(
                empresa_data,
                oportunidades_altas,
                valor_alto_match
            )
            
            relatorio = {
                'gerado_em': datetime.now().isoformat(),
                'empresa': {
                    'cnpj': empresa_data.get('cnpj'),
                    'razao_social': empresa_data.get('razao_social'),
                    'cnae': empresa_data.get('cnae')
                },
                'resumo_executivo': {
                    'total_oportunidades': total_oportunidades,
                    'oportunidades_alta_compatibilidade': oportunidades_altas,
                    'oportunidades_media_compatibilidade': oportunidades_medias,
                    'valor_total_mercado': valor_total,
                    'valor_oportunidades_prioritarias': valor_alto_match,
                    'percentual_aproveitamento': round(
                        (oportunidades_altas / total_oportunidades * 100) if total_oportunidades > 0 else 0,
                        1
                    )
                },
                'analise_regional': analise_regional,
                'analise_modalidade': analise_modalidade,
                'score_viabilidade': score_viabilidade,
                'recomendacoes_prioritarias': recomendacoes,
                'proximos_passos': self._gerar_proximos_passos(oportunidades_altas, oportunidades_medias)
            }
            
            return relatorio
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório executivo: {e}", exc_info=True)
            return {'erro': str(e)}
    
    def gerar_sugestoes_acao(
        self,
        licitacao: Dict,
        match_data: Dict,
        empresa_data: Dict
    ) -> List[Dict]:
        """
        Gera sugestões de ação personalizadas para uma licitação
        
        Args:
            licitacao: Dados da licitação
            match_data: Dados do match
            empresa_data: Dados da empresa
            
        Returns:
            Lista de sugestões de ação
        """
        try:
            sugestoes = []
            score = match_data.get('score', 0)
            componentes = match_data.get('componentes', {})
            
            # Análise de CNAE
            if componentes.get('cnae', {}).get('score', 0) < 50:
                sugestoes.append({
                    'prioridade': 'alta',
                    'categoria': 'cnae',
                    'acao': 'Considere parceria',
                    'detalhes': 'O CNAE da empresa tem baixa compatibilidade. Busque parceiro com CNAE mais adequado.',
                    'impacto': 'Aumenta chances em 40%'
                })
            
            # Análise de porte
            if componentes.get('porte', {}).get('score', 0) < 50:
                sugestoes.append({
                    'prioridade': 'média',
                    'categoria': 'porte',
                    'acao': 'Avalie consórcio',
                    'detalhes': 'Valor da licitação pode estar fora da faixa ideal para seu porte.',
                    'impacto': 'Viabiliza participação'
                })
            
            # Análise geográfica
            if componentes.get('geografia', {}).get('score', 0) < 60:
                sugestoes.append({
                    'prioridade': 'baixa',
                    'categoria': 'logistica',
                    'acao': 'Planeje logística',
                    'detalhes': 'Licitação em região distante. Considere custos de deslocamento e entrega.',
                    'impacto': 'Evita surpresas'
                })
            
            # Prazo
            dias_restantes = self._calcular_dias_restantes(licitacao.get('prazo'))
            if dias_restantes and dias_restantes <= 7:
                sugestoes.append({
                    'prioridade': 'urgente',
                    'categoria': 'prazo',
                    'acao': 'AÇÃO IMEDIATA NECESSÁRIA',
                    'detalhes': f'Apenas {dias_restantes} dias restantes. Priorize preparação de documentos.',
                    'impacto': 'Crítico'
                })
            
            # Score geral
            if score >= 80:
                sugestoes.append({
                    'prioridade': 'alta',
                    'categoria': 'oportunidade',
                    'acao': 'PRIORIZE esta licitação',
                    'detalhes': 'Excelente compatibilidade. Alta chance de sucesso.',
                    'impacto': 'Recomendado participar'
                })
            
            return sugestoes
            
        except Exception as e:
            logger.error(f"Erro ao gerar sugestões: {e}")
            return []
    
    def _analisar_distribuicao_regional(self, licitacoes: List[Dict]) -> Dict:
        """Analisa distribuição regional das oportunidades"""
        try:
            distribuicao = {}
            for lic in licitacoes:
                uf = lic.get('uf', 'N/A')
                if uf not in distribuicao:
                    distribuicao[uf] = {'quantidade': 0, 'valor_total': 0}
                
                distribuicao[uf]['quantidade'] += 1
                distribuicao[uf]['valor_total'] += lic.get('valor', 0)
            
            # Ordenar por quantidade
            top_ufs = sorted(
                distribuicao.items(),
                key=lambda x: x[1]['quantidade'],
                reverse=True
            )[:5]
            
            return {
                'distribuicao_completa': distribuicao,
                'top_5_ufs': [
                    {
                        'uf': uf,
                        'quantidade': dados['quantidade'],
                        'valor_total': dados['valor_total']
                    }
                    for uf, dados in top_ufs
                ]
            }
            
        except Exception as e:
            logger.error(f"Erro na análise regional: {e}")
            return {}
    
    def _analisar_distribuicao_modalidade(self, licitacoes: List[Dict]) -> Dict:
        """Analisa distribuição por modalidade"""
        try:
            distribuicao = {}
            for lic in licitacoes:
                modalidade = lic.get('modalidade', 'N/A')
                if modalidade not in distribuicao:
                    distribuicao[modalidade] = {'quantidade': 0, 'valor_total': 0}
                
                distribuicao[modalidade]['quantidade'] += 1
                distribuicao[modalidade]['valor_total'] += lic.get('valor', 0)
            
            return {
                'por_modalidade': distribuicao,
                'modalidade_mais_frequente': max(
                    distribuicao.items(),
                    key=lambda x: x[1]['quantidade']
                )[0] if distribuicao else 'N/A'
            }
            
        except Exception as e:
            logger.error(f"Erro na análise de modalidade: {e}")
            return {}
    
    def _calcular_score_viabilidade(
        self,
        empresa_data: Dict,
        oportunidades_altas: int,
        valor_alto_match: float
    ) -> Dict:
        """Calcula score de viabilidade geral"""
        try:
            # Fatores de viabilidade
            fator_quantidade = min(oportunidades_altas / 10 * 100, 100)  # Ideal: 10+ oportunidades
            fator_valor = min(valor_alto_match / 1000000 * 100, 100)  # Ideal: 1M+
            
            # Tempo de atividade (se disponível)
            fator_experiencia = 75  # Valor padrão
            
            if 'data_abertura' in empresa_data:
                try:
                    data_abertura = datetime.strptime(str(empresa_data['data_abertura']), '%Y%m%d')
                    anos = (datetime.now() - data_abertura).days / 365.25
                    fator_experiencia = min(anos / 5 * 100, 100)  # Ideal: 5+ anos
                except:
                    pass
            
            score_final = (fator_quantidade * 0.4 + fator_valor * 0.3 + fator_experiencia * 0.3)
            
            return {
                'score': round(score_final, 1),
                'classificacao': self._classificar_viabilidade(score_final),
                'componentes': {
                    'quantidade_oportunidades': round(fator_quantidade, 1),
                    'valor_potencial': round(fator_valor, 1),
                    'experiencia_empresa': round(fator_experiencia, 1)
                },
                'recomendacao': self._recomendar_por_viabilidade(score_final)
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular viabilidade: {e}")
            return {'score': 0, 'classificacao': 'Indeterminado'}
    
    def _classificar_viabilidade(self, score: float) -> str:
        """Classifica viabilidade baseada no score"""
        if score >= 80:
            return 'Excelente'
        elif score >= 60:
            return 'Boa'
        elif score >= 40:
            return 'Regular'
        else:
            return 'Baixa'
    
    def _recomendar_por_viabilidade(self, score: float) -> str:
        """Gera recomendação baseada na viabilidade"""
        if score >= 80:
            return 'Mercado B2G altamente promissor para sua empresa. Invista em capacitação.'
        elif score >= 60:
            return 'Boas oportunidades disponíveis. Foque nas de maior match.'
        elif score >= 40:
            return 'Mercado moderado. Considere parcerias para aumentar competitividade.'
        else:
            return 'Poucas oportunidades compatíveis atualmente. Monitore novas licitações.'
    
    def _gerar_recomendacoes(
        self,
        empresa_data: Dict,
        licitacoes: List[Dict],
        match_scores: List[Dict]
    ) -> List[str]:
        """Gera recomendações prioritárias"""
        recomendacoes = []
        
        # Top 3 licitações
        top_matches = sorted(
            [(i, m) for i, m in enumerate(match_scores)],
            key=lambda x: x[1].get('score', 0),
            reverse=True
        )[:3]
        
        for idx, match in top_matches:
            if idx < len(licitacoes):
                lic = licitacoes[idx]
                recomendacoes.append(
                    f"Priorize: {lic.get('titulo', 'N/A')[:80]}... "
                    f"(Match: {match.get('score', 0)}%)"
                )
        
        return recomendacoes
    
    def _gerar_proximos_passos(self, oportunidades_altas: int, oportunidades_medias: int) -> List[str]:
        """Gera lista de próximos passos recomendados"""
        passos = []
        
        if oportunidades_altas > 0:
            passos.append(f"1. Analise detalhadamente as {oportunidades_altas} oportunidades de alta compatibilidade")
            passos.append("2. Prepare documentação necessária para licitações prioritárias")
            passos.append("3. Configure alertas para novas oportunidades similares")
        
        if oportunidades_medias > 0:
            passos.append(f"4. Avalie viabilidade das {oportunidades_medias} oportunidades de média compatibilidade")
            passos.append("5. Considere parcerias para oportunidades fora do CNAE principal")
        
        passos.append("6. Monitore prazos de submissão regularmente")
        
        return passos
    
    def _calcular_dias_restantes(self, prazo: Optional[str]) -> Optional[int]:
        """Calcula dias restantes até o prazo"""
        try:
            if not prazo:
                return None
            
            data_prazo = datetime.fromisoformat(prazo.replace('Z', '+00:00'))
            dias = (data_prazo - datetime.now()).days
            return max(0, dias)
            
        except:
            return None


# Função helper
def gerar_relatorio_licitacoes(empresa_data: Dict, licitacoes: List[Dict], match_scores: List[Dict]) -> Dict:
    """
    Função auxiliar para gerar relatório
    
    Args:
        empresa_data: Dados da empresa
        licitacoes: Lista de licitações
        match_scores: Scores de match
        
    Returns:
        Relatório executivo
    """
    service = RelatorioIAService()
    return service.gerar_relatorio_executivo(empresa_data, licitacoes, match_scores)
