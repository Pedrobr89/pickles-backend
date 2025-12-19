# scoring_engine.py
"""
Motor de pontuação para análise de compatibilidade B2B.
Avalia a compatibilidade entre prestador e potencial cliente/oportunidade.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import re
import math


# ========================================
# ENUMS E CONSTANTES
# ========================================

class SituacaoCadastral(Enum):
    """Situações cadastrais possíveis."""
    ATIVA = "ATIVA"
    SUSPENSA = "SUSPENSA"
    INAPTA = "INAPTA"
    BAIXADA = "BAIXADA"
    NULA = "NULA"
    CANCELADA = "CANCELADA"


class Porte(Enum):
    """Portes de empresa."""
    MEI = 1
    MICRO = 2
    PEQUENO = 3
    MEDIO = 4
    GRANDE = 5


class ClassificacaoScore(Enum):
    """Classificação do score final."""
    MUITO_ALTA = "Muito Alta"
    ALTA = "Alta"
    MEDIA = "Média"
    BAIXA = "Baixa"
    MUITO_BAIXA = "Muito Baixa"


# Mapeamento de estados vizinhos (completo)
ESTADOS_VIZINHOS = {
    'AC': ['AM', 'RO'],
    'AL': ['PE', 'SE', 'BA'],
    'AP': ['PA'],
    'AM': ['RR', 'PA', 'MT', 'RO', 'AC'],
    'BA': ['SE', 'AL', 'PE', 'PI', 'TO', 'GO', 'MG', 'ES'],
    'CE': ['RN', 'PB', 'PE', 'PI'],
    'DF': ['GO', 'MG'],
    'ES': ['BA', 'MG', 'RJ'],
    'GO': ['TO', 'BA', 'MG', 'MS', 'MT', 'DF'],
    'MA': ['PA', 'TO', 'PI'],
    'MT': ['RO', 'AM', 'PA', 'TO', 'GO', 'MS'],
    'MS': ['MT', 'GO', 'MG', 'SP', 'PR'],
    'MG': ['BA', 'ES', 'RJ', 'SP', 'MS', 'GO', 'DF'],
    'PA': ['AP', 'MA', 'TO', 'MT', 'AM', 'RR'],
    'PB': ['RN', 'CE', 'PE'],
    'PR': ['SP', 'MS', 'SC'],
    'PE': ['PB', 'CE', 'PI', 'BA', 'AL'],
    'PI': ['CE', 'MA', 'TO', 'BA', 'PE'],
    'RJ': ['ES', 'MG', 'SP'],
    'RN': ['PB', 'CE'],
    'RS': ['SC'],
    'RO': ['AC', 'AM', 'MT'],
    'RR': ['AM', 'PA'],
    'SC': ['PR', 'RS'],
    'SP': ['MG', 'RJ', 'PR', 'MS'],
    'SE': ['AL', 'BA'],
    'TO': ['MA', 'PI', 'BA', 'GO', 'MT', 'PA']
}

# Regiões do Brasil
REGIOES = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
    'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
    'Sul': ['PR', 'SC', 'RS']
}


# ========================================
# IMPORTAÇÃO DE FUNÇÕES CNAE
# ========================================

try:
    from .cnae_mapping_config import get_cnae_targets, is_concorrente, get_potencial_score
    CNAE_MAPPING_DISPONIVEL = True
except ImportError:
    CNAE_MAPPING_DISPONIVEL = False
    def get_cnae_targets(cnae): 
        return {'clientes_primarios': [], 'clientes_secundarios': []}
    def is_concorrente(cnae1, cnae2): 
        return False
    def get_potencial_score(cnae1, cnae2): 
        return ("Baixo", 0)


# ========================================
# DATACLASSES
# ========================================

@dataclass
class CriterioScore:
    """Resultado de avaliação de um critério."""
    nome: str
    peso: float
    score: float  # 0-100
    contribuicao: float  # score * peso
    detalhes: Optional[str] = None


@dataclass
class ResultadoScore:
    """Resultado final da avaliação de score."""
    score_total: float  # 0-100
    classificacao: str
    detalhes: Dict[str, Any]
    criterios: List[CriterioScore]
    alertas: List[str]


@dataclass
class PerfilPesos:
    """Perfil de pesos para diferentes tipos de análise."""
    nome: str
    cnae: float
    localizacao: float
    porte: float
    capital_social: float
    experiencia: float
    certidoes: float
    
    def __post_init__(self):
        """Valida que a soma dos pesos = 1.0"""
        total = sum([
            self.cnae, self.localizacao, self.porte,
            self.capital_social, self.experiencia, self.certidoes
        ])
        if not math.isclose(total, 1.0, rel_tol=1e-5):
            raise ValueError(f"Soma dos pesos deve ser 1.0, obtido: {total}")


# ========================================
# PERFIS DE PESOS PRÉ-DEFINIDOS
# ========================================

PERFIS_PESOS = {
    'default': PerfilPesos(
        nome='Padrão',
        cnae=0.35,
        localizacao=0.20,
        porte=0.15,
        capital_social=0.15,
        experiencia=0.10,
        certidoes=0.05
    ),
    'licitacao_publica': PerfilPesos(
        nome='Licitação Pública',
        cnae=0.30,
        localizacao=0.15,
        porte=0.10,
        capital_social=0.15,
        experiencia=0.10,
        certidoes=0.20  # Certidões mais importantes
    ),
    'b2b_privado': PerfilPesos(
        nome='B2B Privado',
        cnae=0.40,  # Fit de mercado mais importante
        localizacao=0.15,
        porte=0.15,
        capital_social=0.20,  # Capacidade de pagamento
        experiencia=0.10,
        certidoes=0.00  # Menos relevante
    ),
    'projeto_local': PerfilPesos(
        nome='Projeto Local',
        cnae=0.25,
        localizacao=0.40,  # Proximidade crítica
        porte=0.10,
        capital_social=0.10,
        experiencia=0.10,
        certidoes=0.05
    )
}


# ========================================
# MOTOR DE SCORING
# ========================================

class ScoringEngine:
    """
    Motor de cálculo de score de compatibilidade.
    
    Avalia a compatibilidade entre um prestador de serviços e uma
    oportunidade comercial (cliente potencial, licitação, etc).
    """
    
    def __init__(self, perfil: str = 'default'):
        """
        Inicializa o motor com um perfil de pesos.
        
        Args:
            perfil: Nome do perfil ('default', 'licitacao_publica', etc)
        """
        if perfil not in PERFIS_PESOS:
            raise ValueError(f"Perfil '{perfil}' não existe. Disponíveis: {list(PERFIS_PESOS.keys())}")
        
        self.perfil = PERFIS_PESOS[perfil]
        self.alertas = []
        
    def get_score(self, empresa: Dict[str, Any], edital: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula o score de compatibilidade (compatibilidade reversa mantida).
        
        Args:
            empresa: Dados da empresa prestadora
            edital: Dados da oportunidade/cliente potencial
            
        Returns:
            Dict com score_final e detalhamento
        """
        resultado = self.calcular_score(empresa, edital)
        
        return {
            'score_final': resultado.score_total,
            'detalhes': resultado.detalhes,
            'alertas': resultado.alertas
        }
    
    def calcular_score(self, empresa: Dict[str, Any], edital: Dict[str, Any]) -> ResultadoScore:
        """
        Calcula score detalhado com todos os critérios.
        
        Args:
            empresa: Dados da empresa prestadora
            edital: Dados da oportunidade/cliente potencial
            
        Returns:
            ResultadoScore com análise completa
        """
        self.alertas = []
        criterios = []
        
        try:
            # 1. CNAE - Compatibilidade de setor
            score_cnae, detalhes_cnae = self._avaliar_cnae(
                empresa.get('cnae_fiscal', ''),
                edital.get('cnae_relacionado', '')
            )
            criterios.append(CriterioScore(
                nome='cnae',
                peso=self.perfil.cnae,
                score=score_cnae * 100,
                contribuicao=score_cnae * self.perfil.cnae * 100,
                detalhes=detalhes_cnae
            ))
            
            # 2. Localização - Proximidade geográfica
            score_loc, detalhes_loc = self._avaliar_localizacao(
                empresa.get('uf', ''),
                empresa.get('municipio', ''),
                edital.get('uf', ''),
                edital.get('municipio', '')
            )
            criterios.append(CriterioScore(
                nome='localizacao',
                peso=self.perfil.localizacao,
                score=score_loc * 100,
                contribuicao=score_loc * self.perfil.localizacao * 100,
                detalhes=detalhes_loc
            ))
            
            # 3. Porte - Compatibilidade de tamanho
            score_porte, detalhes_porte = self._avaliar_porte(
                empresa.get('porte', ''),
                edital.get('porte_preferencial', [])
            )
            criterios.append(CriterioScore(
                nome='porte',
                peso=self.perfil.porte,
                score=score_porte * 100,
                contribuicao=score_porte * self.perfil.porte * 100,
                detalhes=detalhes_porte
            ))
            
            # 4. Capital Social - Capacidade financeira
            score_capital, detalhes_capital = self._avaliar_capital_social(
                empresa.get('capital_social', 0),
                edital.get('valorEstimado', 0)
            )
            criterios.append(CriterioScore(
                nome='capital_social',
                peso=self.perfil.capital_social,
                score=score_capital * 100,
                contribuicao=score_capital * self.perfil.capital_social * 100,
                detalhes=detalhes_capital
            ))
            
            # 5. Experiência - Tempo no mercado
            score_exp, detalhes_exp = self._avaliar_experiencia(
                empresa.get('data_abertura', ''),
                edital.get('exige_experiencia', False)
            )
            criterios.append(CriterioScore(
                nome='experiencia',
                peso=self.perfil.experiencia,
                score=score_exp * 100,
                contribuicao=score_exp * self.perfil.experiencia * 100,
                detalhes=detalhes_exp
            ))
            
            # 6. Certidões - Regularidade fiscal
            score_cert, detalhes_cert = self._avaliar_certidoes(
                empresa.get('situacao_cadastral', ''),
                edital.get('exige_certidoes', False)
            )
            criterios.append(CriterioScore(
                nome='certidoes',
                peso=self.perfil.certidoes,
                score=score_cert * 100,
                contribuicao=score_cert * self.perfil.certidoes * 100,
                detalhes=detalhes_cert
            ))
            
            # Calcular score final ponderado
            score_final = sum(c.contribuicao for c in criterios)
            score_final = min(100, max(0, score_final))
            
            # Classificar score
            classificacao = self._classificar_score(score_final)
            
            # Montar detalhes
            detalhes = {
                criterio.nome: {
                    'score': criterio.score,
                    'peso': criterio.peso,
                    'contribuicao': criterio.contribuicao,
                    'detalhes': criterio.detalhes
                }
                for criterio in criterios
            }
            
            return ResultadoScore(
                score_total=score_final,
                classificacao=classificacao.value,
                detalhes=detalhes,
                criterios=criterios,
                alertas=self.alertas
            )
            
        except Exception as e:
            self.alertas.append(f"Erro no cálculo: {str(e)}")
            return ResultadoScore(
                score_total=0,
                classificacao=ClassificacaoScore.MUITO_BAIXA.value,
                detalhes={},
                criterios=[],
                alertas=self.alertas
            )
    
    def _classificar_score(self, score: float) -> ClassificacaoScore:
        """Classifica o score final."""
        if score >= 85:
            return ClassificacaoScore.MUITO_ALTA
        elif score >= 70:
            return ClassificacaoScore.ALTA
        elif score >= 50:
            return ClassificacaoScore.MEDIA
        elif score >= 30:
            return ClassificacaoScore.BAIXA
        else:
            return ClassificacaoScore.MUITO_BAIXA
    
    def _limpar_cnae(self, cnae: str) -> str:
        """Remove caracteres não numéricos do CNAE."""
        return ''.join(c for c in str(cnae) if c.isdigit())
    
    def _avaliar_cnae(self, cnae_empresa: str, cnae_edital: str) -> Tuple[float, str]:
        """
        Avalia compatibilidade entre CNAEs.
        
        Lógica:
        - Usa mapeamento de CNAEs complementares (se disponível)
        - CNAEs concorrentes = score baixo
        - CNAEs complementares = score alto
        - Mesmo setor (2 dígitos) = possível concorrência
        
        Returns:
            (score 0-1, descrição)
        """
        if not cnae_empresa or not cnae_edital:
            self.alertas.append("CNAE faltando - usando score neutro")
            return 0.5, "CNAE não informado"
        
        cnae_emp = self._limpar_cnae(cnae_empresa)
        cnae_edit = self._limpar_cnae(cnae_edital)
        
        if not cnae_emp or not cnae_edit:
            return 0.5, "CNAE inválido"
        
        # Usar mapeamento oficial se disponível
        if CNAE_MAPPING_DISPONIVEL:
            # Verificar se é concorrente
            if is_concorrente(cnae_emp, cnae_edit):
                self.alertas.append("Cliente é concorrente direto")
                return 0.1, "Concorrente direto"
            
            # Calcular potencial de complementaridade
            tipo_potencial, score_potencial = get_potencial_score(cnae_emp, cnae_edit)
            
            if score_potencial > 0:
                return score_potencial, f"Cliente {tipo_potencial}"
        
        # Fallback: comparação simples por setor
        if cnae_emp[:2] == cnae_edit[:2]:
            self.alertas.append("Mesmo setor - possível concorrência")
            return 0.3, "Mesmo setor"
        
        if cnae_emp[:1] == cnae_edit[:1]:
            return 0.6, "Seção relacionada"
        
        return 0.5, "Setores diferentes"
    
    def _get_regiao(self, uf: str) -> Optional[str]:
        """Retorna a região do estado."""
        for regiao, estados in REGIOES.items():
            if uf in estados:
                return regiao
        return None
    
    def _avaliar_localizacao(self, uf_empresa: str, municipio_empresa: str,
                            uf_edital: str, municipio_edital: str) -> Tuple[float, str]:
        """
        Avalia proximidade geográfica.
        
        Returns:
            (score 0-1, descrição)
        """
        if not uf_empresa or not uf_edital:
            return 0.5, "Localização não informada"
        
        uf_emp = uf_empresa.upper()
        uf_edit = uf_edital.upper()
        
        # Mesmo município = excelente
        if (uf_emp == uf_edit and municipio_empresa and municipio_edital 
            and municipio_empresa.lower() == municipio_edital.lower()):
            return 1.0, "Mesmo município"
        
        # Mesmo estado = muito bom
        if uf_emp == uf_edit:
            return 0.8, "Mesmo estado"
        
        # Estados vizinhos = bom
        vizinhos = ESTADOS_VIZINHOS.get(uf_emp, [])
        if uf_edit in vizinhos:
            return 0.5, "Estado vizinho"
        
        # Mesma região = aceitável
        regiao_emp = self._get_regiao(uf_emp)
        regiao_edit = self._get_regiao(uf_edit)
        
        if regiao_emp and regiao_edit and regiao_emp == regiao_edit:
            return 0.3, f"Mesma região ({regiao_emp})"
        
        # Regiões diferentes = baixo
        return 0.15, "Regiões diferentes"
    
    def _normalizar_porte(self, porte: str) -> Optional[Porte]:
        """Converte string para enum Porte."""
        try:
            porte_upper = porte.upper().strip()
            
            # Mapeamentos comuns
            mapeamento = {
                'MEI': Porte.MEI,
                'MICRO': Porte.MICRO,
                'MICRO EMPRESA': Porte.MICRO,
                'PEQUENO': Porte.PEQUENO,
                'PEQUENA': Porte.PEQUENO,
                'EPP': Porte.PEQUENO,
                'MEDIO': Porte.MEDIO,
                'MÉDIA': Porte.MEDIO,
                'GRANDE': Porte.GRANDE
            }
            
            return mapeamento.get(porte_upper)
        except:
            return None
    
    def _avaliar_porte(self, porte_empresa: str, 
                      portes_preferenciais: List[str]) -> Tuple[float, str]:
        """
        Avalia compatibilidade de porte.
        
        Lógica: Cliente de porte maior ou igual = melhor capacidade de pagamento
        
        Returns:
            (score 0-1, descrição)
        """
        porte_emp = self._normalizar_porte(porte_empresa)
        
        if not porte_emp:
            return 0.5, "Porte não informado"
        
        # Se há preferência específica
        if portes_preferenciais:
            portes_pref = [self._normalizar_porte(p) for p in portes_preferenciais]
            portes_pref = [p for p in portes_pref if p is not None]
            
            if porte_emp in portes_pref:
                return 1.0, "Porte atende preferência"
            
            # Calcular distância da preferência
            if portes_pref:
                nivel_emp = porte_emp.value
                nivel_pref_min = min(p.value for p in portes_pref)
                diferenca = abs(nivel_emp - nivel_pref_min)
                
                if diferenca <= 1:
                    return 0.7, "Porte próximo da preferência"
                else:
                    return 0.4, "Porte distante da preferência"
        
        # Sem preferência: clientes maiores = melhor
        nivel = porte_emp.value
        
        if nivel >= 4:  # Médio/Grande
            return 0.9, "Cliente de grande porte"
        elif nivel == 3:  # Pequeno
            return 0.7, "Cliente de médio porte"
        else:  # MEI/Micro
            return 0.5, "Cliente de pequeno porte"
    
    def _normalizar_valor(self, valor: float) -> float:
        """Normaliza valor usando escala logarítmica."""
        if valor <= 0:
            return 0
        return math.log10(valor + 1)
    
    def _avaliar_capital_social(self, capital_empresa: float, 
                               valor_estimado: float) -> Tuple[float, str]:
        """
        Avalia capacidade financeira do PRESTADOR para executar o projeto.
        
        Compara capital social do prestador com valor estimado da oportunidade.
        
        Returns:
            (score 0-1, descrição)
        """
        if capital_empresa <= 0:
            self.alertas.append("Capital social não declarado")
            return 0.3, "Capital não declarado"
        
        # Se não há valor estimado, avaliar apenas o capital absoluto
        if valor_estimado <= 0:
            if capital_empresa >= 5_000_000:
                return 0.9, "Alto capital social"
            elif capital_empresa >= 1_000_000:
                return 0.8, "Bom capital social"
            elif capital_empresa >= 500_000:
                return 0.7, "Capital adequado"
            elif capital_empresa >= 100_000:
                return 0.6, "Capital moderado"
            else:
                return 0.5, "Capital limitado"
        
        # Comparar com valor estimado (geralmente 10-20% do valor)
        razao = capital_empresa / valor_estimado
        
        if razao >= 0.20:  # 20%+ do valor
            return 1.0, "Capital muito adequado ao projeto"
        elif razao >= 0.15:
            return 0.9, "Capital adequado ao projeto"
        elif razao >= 0.10:
            return 0.8, "Capital suficiente"
        elif razao >= 0.05:
            return 0.6, "Capital no limite"
        else:
            self.alertas.append("Capital social pode ser insuficiente para o projeto")
            return 0.3, "Capital insuficiente"
    
    def _parse_data(self, data_str: str) -> Optional[datetime]:
        """Tenta fazer parse de data em vários formatos."""
        if not data_str:
            return None
        
        formatos = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%Y%m%d',
            '%d-%m-%Y',
            '%Y/%m/%d'
        ]
        
        for fmt in formatos:
            try:
                return datetime.strptime(data_str[:10], fmt)
            except:
                continue
        
        return None
    
    def _avaliar_experiencia(self, data_abertura: str, 
                            exige_experiencia: bool) -> Tuple[float, str]:
        """
        Avalia tempo de mercado da empresa PRESTADORA.
        
        Returns:
            (score 0-1, descrição)
        """
        if not data_abertura:
            if exige_experiencia:
                self.alertas.append("Data de abertura não informada")
                return 0.3, "Experiência não verificável"
            return 0.5, "Data não informada"
        
        data_inicio = self._parse_data(data_abertura)
        
        if not data_inicio:
            if exige_experiencia:
                self.alertas.append("Data de abertura inválida")
                return 0.3, "Data inválida"
            return 0.5, "Data inválida"
        
        anos = (datetime.now() - data_inicio).days / 365.25
        
        if anos < 0:
            self.alertas.append("Data de abertura no futuro!")
            return 0.0, "Data inválida"
        
        # Empresas muito jovens podem ter restrições
        if anos >= 15:
            return 1.0, f"{int(anos)} anos - muito experiente"
        elif anos >= 10:
            return 0.95, f"{int(anos)} anos - experiente"
        elif anos >= 5:
            return 0.85, f"{int(anos)} anos - consolidada"
        elif anos >= 3:
            return 0.7, f"{int(anos)} anos - estabelecida"
        elif anos >= 2:
            return 0.6, f"{int(anos)} anos - em crescimento"
        elif anos >= 1:
            return 0.5, f"{int(anos)} anos - recente"
        else:
            if exige_experiencia:
                self.alertas.append("Empresa muito nova para requisitos")
                return 0.2, "Menos de 1 ano"
            return 0.4, "Menos de 1 ano"
    
    def _normalizar_situacao(self, situacao: str) -> Optional[SituacaoCadastral]:
        """Converte string para enum SituacaoCadastral."""
        try:
            situacao_upper = situacao.upper().strip()
            return SituacaoCadastral(situacao_upper)
        except:
            return None
    
    def _avaliar_certidoes(self, situacao_cadastral: str, 
                          exige_certidoes: bool) -> Tuple[float, str]:
        """
        Avalia situação cadastral da empresa PRESTADORA.
        
        Returns:
            (score 0-1, descrição)
        """
        if not situacao_cadastral:
            if exige_certidoes:
                self.alertas.append("Situação cadastral não verificável")
                return 0.3, "Situação não informada"
            return 0.5, "Situação não informada"
        
        situacao = self._normalizar_situacao(situacao_cadastral)
        
        if situacao == SituacaoCadastral.ATIVA:
            return 1.0, "Empresa ativa"
        
        # Situações críticas
        if situacao in [SituacaoCadastral.BAIXADA, SituacaoCadastral.CANCELADA]:
            self.alertas.append("Empresa não está ativa!")
            return 0.0, f"Empresa {situacao.value}"
        
        if situacao in [SituacaoCadastral.SUSPENSA, SituacaoCadastral.INAPTA]:
            self.alertas.append(f"Situação irregular: {situacao.value}")
            if exige_certidoes:
                return 0.1, f"Empresa {situacao.value}"
            return 0.3, f"Empresa {situacao.value}"
        
        # Situação desconhecida
        if exige_certidoes:
            self.alertas.append("Situação cadastral não reconhecida")
            return 0.3, "Situação desconhecida"
        
        return 0.5, "Situação desconhecida"


# ========================================
# FUNÇÕES AUXILIARES
# ========================================

def analisar_compatibilidade_lote(empresas: List[Dict], 
                                  edital: Dict,
                                  perfil: str = 'default',
                                  top_n: Optional[int] = None) -> List[Dict]:
    """
    Analisa compatibilidade de múltiplas empresas com um edital.
    
    Args:
        empresas: Lista de empresas para analisar
        edital: Dados do edital/oportunidade
        perfil: Perfil de pesos a usar
        top_n: Retornar apenas os N melhores (None = todos)
        
    Returns:
        Lista de empresas com scores calculados, ordenada por score
    """
    engine = ScoringEngine(perfil=perfil)
    resultados = []
    
    for empresa in empresas:
        try:
            resultado = engine.calcular_score(empresa, edital)
            resultados.append({
                'empresa': empresa,
                'score': resultado.score_total,
                'classificacao': resultado.classificacao,
                'detalhes': resultado.detalhes,
                'alertas': resultado.alertas
            })
        except Exception as e:
            resultados.append({
                'empresa': empresa,
                'score': 0,
                'classificacao': 'Erro',
                'erro': str(e)
            })
    
    # Ordenar por score (maior primeiro)
    resultados_ordenados = sorted(resultados, key=lambda x: x['score'], reverse=True)
    
    if top_n:
        return resultados_ordenados[:top_n]
    
    return resultados_ordenados


def comparar_perfis(empresa: Dict, edital: Dict) -> Dict[str, ResultadoScore]:
    """
    Compara todos os perfis de peso disponíveis.
    
    Args:
        empresa: Dados da empresa
        edital: Dados da oportunidade
    
    Returns:
        Dicionário com resultados para cada perfil
    """
    resultados = {}
    perfis = ['default', 'agressivo', 'conservador', 'compliance']
    
    for p in perfis:
        try:
            engine = ScoringEngine(perfil=p)
            resultados[p] = engine.calcular_score(empresa, edital)
        except Exception:
            pass
            
    return resultados