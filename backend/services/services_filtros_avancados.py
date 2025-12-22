"""
Serviço de Filtros Avançados para Licitações B2G
Gerencia filtros complexos, geográficos e salvos
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import math

logger = logging.getLogger(__name__)


class FiltrosAvancadosService:
    """Serviço para filtros avançados de licitações"""
    
    # Coordenadas de capitais brasileiras para cálculo de raio
    CAPITAIS = {
        'SP': (-23.5505, -46.6333),
        'RJ': (-22.9068, -43.1729),
        'MG': (-19.9167, -43.9345),
        'RS': (-30.0346, -51.2177),
        'PR': (-25.4284, -49.2733),
        'SC': (-27.5954, -48.5480),
        'BA': (-12.9714, -38.5014),
        'PE': (-8.0476, -34.8770),
        'CE': (-3.7172, -38.5433),
        'PA': (-1.4558, -48.5039),
        'GO': (-16.6869, -49.2648),
        'AM': (-3.1190, -60.0217),
        'MA': (-2.5387, -44.2825),
        'PB': (-7.1195, -34.8450),
        'RN': (-5.7945, -35.2110),
        'AL': (-9.6658, -35.7350),
        'SE': (-10.9472, -37.0731),
        'PI': (-5.0892, -42.8019),
        'MT': (-15.6014, -56.0979),
        'MS': (-20.4697, -54.6201),
        'AC': (-9.9758, -67.8243),
        'RO': (-8.7612, -63.9039),
        'RR': (2.8235, -60.6758),
        'AP': (0.0389, -51.0664),
        'TO': (-10.1753, -48.2982),
        'DF': (-15.8267, -47.9218),
        'ES': (-20.3155, -40.3128)
    }
    
    REGIOES = {
        'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
        'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
        'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
        'Sul': ['PR', 'RS', 'SC']
    }
    
    def __init__(self, db_connection=None):
        """
        Inicializa o serviço
        
        Args:
            db_connection: Conexão com banco de dados
        """
        self.db = db_connection
    
    def aplicar_filtros(
        self,
        licitacoes: List[Dict],
        filtros: Dict
    ) -> List[Dict]:
        """
        Aplica múltiplos filtros em uma lista de licitações
        
        Args:
            licitacoes: Lista de licitações
            filtros: Dicionário de filtros
            
        Returns:
            Lista filtrada
        """
        try:
            resultado = licitacoes.copy()
            
            # Filtro geográfico
            if 'geografico' in filtros:
                resultado = self._filtrar_geografico(resultado, filtros['geografico'])
            
            # Filtro por valor
            if 'valor' in filtros:
                resultado = self._filtrar_valor(resultado, filtros['valor'])
            
            # Filtro por prazo
            if 'prazo' in filtros:
                resultado = self._filtrar_prazo(resultado, filtros['prazo'])
            
            # Filtro por modalidade
            if 'modalidades' in filtros:
                resultado = self._filtrar_modalidades(resultado, filtros['modalidades'])
            
            # Filtro por CNAE
            if 'cnaes' in filtros:
                resultado = self._filtrar_cnaes(resultado, filtros['cnaes'])
            
            # Filtro por órgão
            if 'orgaos' in filtros:
                resultado = self._filtrar_orgaos(resultado, filtros['orgaos'])
            
            # Filtro por palavras-chave
            if 'palavras_chave' in filtros:
                resultado = self._filtrar_palavras_chave(resultado, filtros['palavras_chave'])
            
            return resultado
            
        except Exception as e:
            logger.error(f"Erro ao aplicar filtros: {e}", exc_info=True)
            return licitacoes
    
    def _filtrar_geografico(
        self,
        licitacoes: List[Dict],
        config: Dict
    ) -> List[Dict]:
        """
        Aplica filtro geográfico
        
        Args:
            licitacoes: Lista de licitações
            config: Configuração do filtro (tipo, params)
            
        Returns:
            Lista filtrada
        """
        tipo = config.get('tipo')
        
        if tipo == 'ufs':
            ufs = config.get('ufs', [])
            return [l for l in licitacoes if l.get('uf') in ufs]
        
        elif tipo == 'regiao':
            regiao = config.get('regiao')
            ufs_regiao = self.REGIOES.get(regiao, [])
            return [l for l in licitacoes if l.get('uf') in ufs_regiao]
        
        elif tipo == 'raio':
            centro_uf = config.get('centro_uf')
            raio_km = config.get('raio_km', 100)
            
            if centro_uf not in self.CAPITAIS:
                return licitacoes
            
            centro_coords = self.CAPITAIS[centro_uf]
            resultado = []
            
            for lic in licitacoes:
                lic_uf = lic.get('uf')
                if lic_uf and lic_uf in self.CAPITAIS:
                    lic_coords = self.CAPITAIS[lic_uf]
                    distancia = self._calcular_distancia(centro_coords, lic_coords)
                    
                    if distancia <= raio_km:
                        resultado.append({**lic, 'distancia_km': round(distancia, 2)})
            
            return resultado
        
        return licitacoes
    
    def _filtrar_valor(
        self,
        licitacoes: List[Dict],
        config: Dict
    ) -> List[Dict]:
        """Filtra por faixa de valor"""
        valor_min = config.get('minimo', 0)
        valor_max = config.get('maximo', float('inf'))
        
        return [
            l for l in licitacoes
            if valor_min <= l.get('valor', 0) <= valor_max
        ]
    
    def _filtrar_prazo(
        self,
        licitacoes: List[Dict],
        config: Dict
    ) -> List[Dict]:
        """Filtra por prazo"""
        tipo = config.get('tipo')
        
        if tipo == 'dias_restantes':
            dias_min = config.get('minimo', 0)
            dias_max = config.get('maximo', 365)
            
            resultado = []
            for lic in licitacoes:
                dias = self._calcular_dias_restantes(lic.get('prazo'))
                if dias is not None and dias_min <= dias <= dias_max:
                    resultado.append(lic)
            
            return resultado
        
        elif tipo == 'range_datas':
            data_inicio = config.get('data_inicio')
            data_fim = config.get('data_fim')
            
            # TODO: Implementar filtro por range de datas
            return licitacoes
        
        return licitacoes
    
    def _filtrar_modalidades(
        self,
        licitacoes: List[Dict],
        modalidades: List[str]
    ) -> List[Dict]:
        """Filtra por modalidades"""
        return [
            l for l in licitacoes
            if l.get('modalidade') in modalidades
        ]
    
    def _filtrar_cnaes(
        self,
        licitacoes: List[Dict],
        cnaes: List[str]
    ) -> List[Dict]:
        """Filtra por CNAEs"""
        # TODO: Implementar matchde CNAE com objeto da licitação
        return licitacoes
    
    def _filtrar_orgaos(
        self,
        licitacoes: List[Dict],
        orgaos: List[str]
    ) -> List[Dict]:
        """Filtra por órgãos"""
        orgaos_lower = [o.lower() for o in orgaos]
        
        return [
            l for l in licitacoes
            if any(org in l.get('orgao', '').lower() for org in orgaos_lower)
        ]
    
    def _filtrar_palavras_chave(
        self,
        licitacoes: List[Dict],
        palavras: str
    ) -> List[Dict]:
        """Filtra por palavras-chave no título/objeto"""
        palavras_list = palavras.lower().split()
        
        return [
            l for l in licitacoes
            if any(
                palavra in l.get('titulo', '').lower() or
                palavra in l.get('objeto', '').lower()
                for palavra in palavras_list
            )
        ]
    
    def _calcular_distancia(
        self,
        coord1: Tuple[float, float],
        coord2: Tuple[float, float]
    ) -> float:
        """
        Calcula distância entre duas coordenadas em km (Haversine)
        
        Args:
            coord1: (lat, lon)
            coord2: (lat, lon)
            
        Returns:
            Distância em km
        """
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        
        # Raio da Terra em km
        R = 6371
        
        # Converter para radianos
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        # Fórmula de Haversine
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) *
             math.sin(dlon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def _calcular_dias_restantes(self, prazo: Optional[str]) -> Optional[int]:
        """Calcula dias restantes até o prazo"""
        try:
            if not prazo:
                return None
            
            data_prazo = datetime.fromisoformat(prazo.replace('Z', '+00:00'))
            dias = (data_prazo - datetime.now()).days
            return max(0, dias)
            
        except Exception:
            return None
    
    def salvar_filtro(
        self,
        usuario_id: int,
        nome: str,
        filtros: Dict
    ) -> Dict:
        """
        Salva um conjunto de filtros para reutilização
        
        Args:
            usuario_id: ID do usuário
            nome: Nome do filtro salvo
            filtros: Configuração dos filtros
            
        Returns:
            Filtro salvo
        """
        try:
            if not self.db:
                return {}
            
            agora = datetime.now().isoformat()
            
            cursor = self.db.cursor()
            cursor.execute("""
                INSERT INTO filtros_salvos_b2g (
                    usuario_id, nome, configuracao, criado_em
                ) VALUES (?, ?, ?, ?)
            """, (
                usuario_id,
                nome,
                json.dumps(filtros),
                agora
            ))
            
            self.db.commit()
            filtro_id = cursor.lastrowid
            
            return {
                'id': filtro_id,
                'usuario_id': usuario_id,
                'nome': nome,
                'configuracao': filtros,
                'criado_em': agora
            }
            
        except Exception as e:
            logger.error(f"Erro ao salvar filtro: {e}")
            if self.db:
                self.db.rollback()
            return {}
    
    def listar_filtros_salvos(self, usuario_id: int) -> List[Dict]:
        """Lista filtros salvos do usuário"""
        try:
            if not self.db:
                return []
            
            cursor = self.db.cursor()
            cursor.execute("""
                SELECT id, nome, configuracao, criado_em
                FROM filtros_salvos_b2g
                WHERE usuario_id = ?
                ORDER BY criado_em DESC
            """, (usuario_id,))
            
            filtros = []
            for row in cursor.fetchall():
                filtros.append({
                    'id': row[0],
                    'nome': row[1],
                    'configuracao': json.loads(row[2]) if row[2] else {},
                    'criado_em': row[3]
                })
            
            return filtros
            
        except Exception as e:
            logger.error(f"Erro ao listar filtros salvos: {e}")
            return []
    
    def gerar_dados_mapa(self, licitacoes: List[Dict]) -> Dict:
        """
        Gera dados formatados para exibição em mapa
        
        Args:
            licitacoes: Lista de licitações
            
        Returns:
            Dados do mapa (pontos, clusters, heatmap)
        """
        try:
            pontos = []
            
            for lic in licitacoes:
                uf = lic.get('uf')
                if uf and uf in self.CAPITAIS:
                    lat, lon = self.CAPITAIS[uf]
                    
                    pontos.append({
                        'id': lic.get('id'),
                        'titulo': lic.get('titulo', 'N/A')[:60],
                        'orgao': lic.get('orgao', 'N/A'),
                        'valor': lic.get('valor', 0),
                        'match': lic.get('match', 0),
                        'prazo': lic.get('prazo'),
                        'uf': uf,
                        'coordenadas': {'lat': lat, 'lon': lon}
                    })
            
            # Agrupar por UF para estatísticas
            por_uf = {}
            for ponto in pontos:
                uf = ponto['uf']
                if uf not in por_uf:
                    por_uf[uf] = {
                        'uf': uf,
                        'total': 0,
                        'valor_total': 0,
                        'coordenadas': ponto['coordenadas']
                    }
                
                por_uf[uf]['total'] += 1
                por_uf[uf]['valor_total'] += ponto['valor']
            
            return {
                'pontos': pontos,
                'clusters': list(por_uf.values()),
                'total_pontos': len(pontos),
                'centro_mapa': self._calcular_centro(pontos)
            }
            
        except Exception as e:
            logger.error(f"Erro ao gerar dados de mapa: {e}")
            return {'pontos': [], 'clusters': [], 'total_pontos': 0}
    
    def _calcular_centro(self, pontos: List[Dict]) -> Dict:
        """Calcula centro geométrico dos pontos"""
        if not pontos:
            # Centro do Brasil
            return {'lat': -14.235, 'lon': -51.9253}
        
        lats = [p['coordenadas']['lat'] for p in pontos]
        lons = [p['coordenadas']['lon'] for p in pontos]
        
        return {
            'lat': sum(lats) / len(lats),
            'lon': sum(lons) / len(lons)
        }


# Adicionar tabela de filtros salvos na migration
"""
CREATE TABLE IF NOT EXISTS filtros_salvos_b2g (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario_id INTEGER NOT NULL,
    nome TEXT NOT NULL,
    configuracao TEXT NOT NULL,
    criado_em TEXT NOT NULL,
    FOREIGN KEY (usuario_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_filtros_salvos_usuario 
ON filtros_salvos_b2g(usuario_id);
"""
