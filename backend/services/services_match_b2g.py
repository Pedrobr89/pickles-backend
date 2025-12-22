"""
Serviço de Cálculo de Match Inteligente para Oportunidades B2G
Calcula compatibilidade entre empresa e licitação baseado em múltiplos fatores
"""

import logging
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MatchB2GCalculator:
    """Calcula score de match entre empresa e licitação"""
    
    # Pesos para cada componente do match
    PESO_CNAE = 0.40  # 40% - Compatibilidade de CNAE
    PESO_PORTE = 0.30  # 30% - Adequação de porte
    PESO_GEOGRAFIA = 0.20  # 20% - Proximidade geográfica
    PESO_HISTORICO = 0.10  # 10% - Histórico da empresa
    
    # Mapeamento de UFs para regiões
    REGIOES = {
        'norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
        'nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'centro_oeste': ['DF', 'GO', 'MT', 'MS'],
        'sudeste': ['ES', 'MG', 'RJ', 'SP'],
        'sul': ['PR', 'RS', 'SC']
    }
    
    # Faixas de valor por porte (em R$)
    FAIXAS_PORTE = {
        '01': {'nome': 'MEI', 'min': 0, 'max': 81000},
        '03': {'nome': 'ME', 'min': 0, 'max': 360000},
        '05': {'nome': 'EPP', 'min': 360000, 'max': 4800000},
        '00': {'nome': 'Não Informado', 'min': 0, 'max': float('inf')},
        '09': {'nome': 'Demais', 'min': 4800000, 'max': float('inf')}
    }
    
    def __init__(self):
        self.cache_cnaes = {}  # Cache de descrições CNAE
    
    def calcular_match(
        self,
        empresa_data: Dict,
        licitacao_data: Dict
    ) -> Dict:
        """
        Calcula match entre empresa e licitação
        
        Args:
            empresa_data: Dados da empresa (cnpj, cnae, porte, uf, etc)
            licitacao_data: Dados da licitação (objeto, valor, uf, etc)
            
        Returns:
            Dict com score, explicação e componentes
        """
        try:
            # Calcular cada componente
            score_cnae, exp_cnae = self._calcular_score_cnae(
                empresa_data, licitacao_data
            )
            
            score_porte, exp_porte = self._calcular_score_porte(
                empresa_data, licitacao_data
            )
            
            score_geo, exp_geo = self._calcular_score_geografia(
                empresa_data, licitacao_data
            )
            
            score_hist, exp_hist = self._calcular_score_historico(
                empresa_data
            )
            
            # Calcular score final ponderado
            score_final = (
                score_cnae * self.PESO_CNAE +
                score_porte * self.PESO_PORTE +
                score_geo * self.PESO_GEOGRAFIA +
                score_hist * self.PESO_HISTORICO
            )
            
            # Arredondar para inteiro
            score_final = int(round(score_final))
            
            # Classificar match
            classificacao = self._classificar_match(score_final)
            
            # Montar explicação completa
            explicacao = self._gerar_explicacao_completa(
                score_final,
                exp_cnae,
                exp_porte,
                exp_geo,
                exp_hist
            )
            
            return {
                'score': score_final,
                'classificacao': classificacao,
                'explicacao': explicacao,
                'componentes': {
                    'cnae': {
                        'score': int(round(score_cnae)),
                        'peso': self.PESO_CNAE,
                        'explicacao': exp_cnae
                    },
                    'porte': {
                        'score': int(round(score_porte)),
                        'peso': self.PESO_PORTE,
                        'explicacao': exp_porte
                    },
                    'geografia': {
                        'score': int(round(score_geo)),
                        'peso': self.PESO_GEOGRAFIA,
                        'explicacao': exp_geo
                    },
                    'historico': {
                        'score': int(round(score_hist)),
                        'peso': self.PESO_HISTORICO,
                        'explicacao': exp_hist
                    }
                },
                'chance_sucesso': self._estimar_chance_sucesso(score_final, empresa_data, licitacao_data)
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular match: {e}", exc_info=True)
            return {
                'score': 0,
                'classificacao': 'baixo',
                'explicacao': 'Não foi possível calcular o match',
                'componentes': {},
                'chance_sucesso': 'indeterminado'
            }
    
    def _calcular_score_cnae(
        self,
        empresa_data: Dict,
        licitacao_data: Dict
    ) -> Tuple[float, str]:
        """Calcula compatibilidade de CNAE"""
        try:
            cnae_empresa = str(empresa_data.get('cnae', '')).strip()
            objeto_licitacao = str(licitacao_data.get('objeto', '')).lower()
            
            if not cnae_empresa or not objeto_licitacao:
                return 0, "CNAE ou objeto da licitação não disponível"
            
            # Pegar descrição do CNAE (simplificado - idealmente virá do banco)
            cnae_descricao = str(empresa_data.get('cnae_descricao', '')).lower()
            
            # Normalizar textos
            objeto_norm = self._normalizar_texto(objeto_licitacao)
            cnae_norm = self._normalizar_texto(cnae_descricao)
            
            # Extrair palavras-chave relevantes
            palavras_cnae = [p for p in cnae_norm.split() if len(p) >= 4]
            palavras_objeto = [p for p in objeto_norm.split() if len(p) >= 4]
            
            if not palavras_cnae:
                return 30, "CNAE sem descrição - match básico aplicado"
            
            # Contar matches de palavras
            matches = sum(1 for p in palavras_cnae if p in objeto_norm)
            score = min(100, (matches / len(palavras_cnae)) * 100)
            
            # Boost se código CNAE aparece no objeto
            cnae_code = cnae_empresa.replace('-', '').replace('.', '')
            if cnae_code[:2] in objeto_norm or cnae_code[:4] in objeto_norm:
                score = min(100, score + 20)
            
            # Gerar explicação
            if score >= 70:
                exp = f"Alta compatibilidade - {matches} palavras-chave do CNAE encontradas"
            elif score >= 40:
                exp = f"Compatibilidade moderada - {matches} termos relacionados"
            else:
                exp = "Baixa compatibilidade de CNAE com objeto da licitação"
            
            return score, exp
            
        except Exception as e:
            logger.error(f"Erro em _calcular_score_cnae: {e}")
            return 0, "Erro ao calcular CNAE"
    
    def _calcular_score_porte(
        self,
        empresa_data: Dict,
        licitacao_data: Dict
    ) -> Tuple[float, str]:
        """Calcula adequação de porte da empresa vs valor da licitação"""
        try:
            porte_codigo = str(empresa_data.get('porte_da_empresa', '00')).strip()
            valor_licitacao = float(licitacao_data.get('valorTotalEstimado', 0) or 
                                   licitacao_data.get('valorTotal', 0) or 0)
            
            if valor_licitacao <= 0:
                return 50, "Valor da licitação não disponível"
            
            # Pegar faixa do porte
            faixa = self.FAIXAS_PORTE.get(porte_codigo, self.FAIXAS_PORTE['00'])
            
            # Calcular adequação
            if valor_licitacao < faixa['min']:
                # Licitação pequena demais para o porte
                score = 60
                exp = f"Valor abaixo do esperado para {faixa['nome']}"
            elif valor_licitacao > faixa['max']:
                # Licitação grande demais
                score = 40
                exp = f"Valor alto para {faixa['nome']} - considere parceria"
            else:
                # Dentro da faixa ideal
                score = 100
                exp = f"Valor compatível com porte {faixa['nome']}"
            
            return score, exp
            
        except Exception as e:
            logger.error(f"Erro em _calcular_score_porte: {e}")
            return 50, "Erro ao calcular porte"
    
    def _calcular_score_geografia(
        self,
        empresa_data: Dict,
        licitacao_data: Dict
    ) -> Tuple[float, str]:
        """Calcula proximidade geográfica"""
        try:
            uf_empresa = str(empresa_data.get('uf', '')).upper().strip()
            uf_licitacao = str(licitacao_data.get('uf', '') or 
                             licitacao_data.get('ufSigla', '')).upper().strip()
            
            if not uf_empresa or not uf_licitacao:
                return 50, "Localização não disponível"
            
            # Mesmo UF = máximo score
            if uf_empresa == uf_licitacao:
                return 100, f"Mesma UF ({uf_empresa})"
            
            # Mesma região = score médio
            regiao_empresa = self._get_regiao(uf_empresa)
            regiao_licitacao = self._get_regiao(uf_licitacao)
            
            if regiao_empresa == regiao_licitacao:
                return 70, f"Mesma região ({regiao_empresa})"
            
            # Regiões diferentes = score baixo
            return 40, f"Regiões diferentes ({regiao_empresa} vs {regiao_licitacao})"
            
        except Exception as e:
            logger.error(f"Erro em _calcular_score_geografia: {e}")
            return 50, "Erro ao calcular geografia"
    
    def _calcular_score_historico(
        self,
        empresa_data: Dict
    ) -> Tuple[float, str]:
        """Calcula score baseado em histórico da empresa"""
        try:
            # Tempo de atividade
            data_abertura = empresa_data.get('data_de_inicio_atividade')
            if data_abertura:
                try:
                    # Formato: YYYYMMDD
                    data_str = str(data_abertura)
                    data = datetime.strptime(data_str, '%Y%m%d')
                    anos_atividade = (datetime.now() - data).days / 365.25
                    
                    if anos_atividade >= 5:
                        score_tempo = 100
                        exp_tempo = f"{int(anos_atividade)} anos de experiência"
                    elif anos_atividade >= 2:
                        score_tempo = 70
                        exp_tempo = f"{int(anos_atividade)} anos - empresa estabelecida"
                    else:
                        score_tempo = 40
                        exp_tempo = f"{int(anos_atividade)} anos - empresa recente"
                except:
                    score_tempo = 50
                    exp_tempo = "Tempo de atividade não determinado"
            else:
                score_tempo = 50
                exp_tempo = "Data de abertura não disponível"
            
            return score_tempo, exp_tempo
            
        except Exception as e:
            logger.error(f"Erro em _calcular_score_historico: {e}")
            return 50, "Erro ao calcular histórico"
    
    def _normalizar_texto(self, texto: str) -> str:
        """Normaliza texto removendo caracteres especiais"""
        try:
            texto = str(texto).lower()
            texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
            texto = re.sub(r'\s+', ' ', texto).strip()
            return texto
        except:
            return ''
    
    def _get_regiao(self, uf: str) -> str:
        """Retorna região do UF"""
        for regiao, ufs in self.REGIOES.items():
            if uf in ufs:
                return regiao
        return 'desconhecida'
    
    def _classificar_match(self, score: int) -> str:
        """Classifica match em categorias"""
        if score >= 80:
            return 'alto'
        elif score >= 60:
            return 'medio'
        elif score >= 40:
            return 'baixo'
        else:
            return 'muito_baixo'
    
    def _estimar_chance_sucesso(
        self,
        score: int,
        empresa_data: Dict,
        licitacao_data: Dict
    ) -> str:
        """Estima chance de sucesso na licitação"""
        if score >= 80:
            return 'alta'
        elif score >= 60:
            return 'media'
        elif score >= 40:
            return 'baixa'
        else:
            return 'muito_baixa'
    
    def _gerar_explicacao_completa(
        self,
        score_final: int,
        exp_cnae: str,
        exp_porte: str,
        exp_geo: str,
        exp_hist: str
    ) -> str:
        """Gera explicação completa do match"""
        try:
            explicacao = f"Match de {score_final}%\n\n"
            explicacao += f"📋 CNAE: {exp_cnae}\n"
            explicacao += f"💼 Porte: {exp_porte}\n"
            explicacao += f"📍 Localização: {exp_geo}\n"
            explicacao += f"📅 Histórico: {exp_hist}"
            
            return explicacao
        except:
            return f"Match de {score_final}%"


# Função auxiliar para facilitar uso
def calcular_match_licitacao(empresa_data: Dict, licitacao_data: Dict) -> Dict:
    """
    Wrapper function para calcular match
    
    Args:
        empresa_data: Dict com dados da empresa
        licitacao_data: Dict com dados da licitação
        
    Returns:
        Dict com resultado do match
    """
    calculator = MatchB2GCalculator()
    return calculator.calcular_match(empresa_data, licitacao_data)
