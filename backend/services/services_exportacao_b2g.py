"""
Serviço de Exportação de Dados B2G
Gera arquivos Excel, PDF e CSV com licitações
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import json
import io

logger = logging.getLogger(__name__)


class ExportacaoB2GService:
    """Serviço para exportação de licitações em diferentes formatos"""
    
    def __init__(self):
        """Inicializa o serviço"""
        pass
    
    def exportar_excel(
        self,
        licitacoes: List[Dict],
        incluir_match: bool = True,
        incluir_dados_contextuais: bool = False
    ) -> bytes:
        """
        Exporta licitações para Excel
        
        Args:
            licitacoes: Lista de licitações
            incluir_match: Se inclui dados de match
            incluir_dados_contextuais: Se inclui histórico, concorrência, etc
            
        Returns:
            Bytes do arquivo Excel
        """
        try:
            import pandas as pd
            from io import BytesIO
            
            # Preparar dados
            dados = []
            for lic in licitacoes:
                row = {
                    'ID': lic.get('id', 'N/A'),
                    'Título': lic.get('titulo', 'N/A'),
                    'Órgão': lic.get('orgao', 'N/A'),
                    'UF': lic.get('uf', 'N/A'),
                    'Modalidade': lic.get('modalidade', 'N/A'),
                    'Valor (R$)': lic.get('valor', 0),
                    'Prazo': lic.get('prazo', 'N/A'),
                    'Situação': lic.get('situacao', 'Aberta')
                }
                
                if incluir_match:
                    row['Match (%)'] = lic.get('match', 0)
                    row['Classificação'] = lic.get('match_classificacao', 'N/A')
                
                if incluir_dados_contextuais:
                    historico = lic.get('historico_orgao', {})
                    row['Taxa Sucesso Órgão (%)'] = historico.get('taxa_sucesso_media', 'N/A')
                    row['Tempo Pagamento (dias)'] = historico.get('tempo_medio_pagamento_dias', 'N/A')
                
                dados.append(row)
            
            # Criar DataFrame
            df = pd.DataFrame(dados)
            
            # Exportar para Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Licitações', index=False)
                
                # Formatar colunas
                worksheet = writer.sheets['Licitações']
                
                # Ajustar largura das colunas
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)
            
            output.seek(0)
            return output.read()
            
        except ImportError:
            logger.error("pandas ou openpyxl não instalados")
            return b''
        except Exception as e:
            logger.error(f"Erro ao exportar Excel: {e}", exc_info=True)
            return b''
    
    def exportar_csv(
        self,
        licitacoes: List[Dict],
        separador: str = ';',
        incluir_match: bool = True
    ) -> str:
        """
        Exporta licitações para CSV
        
        Args:
            licitacoes: Lista de licitações
            separador: Separador de colunas
            incluir_match: Se inclui dados de match
            
        Returns:
            String CSV
        """
        try:
            import csv
            from io import StringIO
            
            output = StringIO()
            
            # Definir colunas
            colunas = [
                'ID', 'Título', 'Órgão', 'UF', 'Modalidade',
                'Valor (R$)', 'Prazo', 'Situação'
            ]
            
            if incluir_match:
                colunas.extend(['Match (%)', 'Classificação'])
            
            writer = csv.DictWriter(output, fieldnames=colunas, delimiter=separador)
            writer.writeheader()
            
            for lic in licitacoes:
                row = {
                    'ID': lic.get('id', 'N/A'),
                    'Título': lic.get('titulo', 'N/A'),
                    'Órgão': lic.get('orgao', 'N/A'),
                    'UF': lic.get('uf', 'N/A'),
                    'Modalidade': lic.get('modalidade', 'N/A'),
                    'Valor (R$)': lic.get('valor', 0),
                    'Prazo': lic.get('prazo', 'N/A'),
                    'Situação': lic.get('situacao', 'Aberta')
                }
                
                if incluir_match:
                    row['Match (%)'] = lic.get('match', 0)
                    row['Classificação'] = lic.get('match_classificacao', 'N/A')
                
                writer.writerow(row)
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Erro ao exportar CSV: {e}")
            return ''
    
    def exportar_pdf(
        self,
        licitacoes: List[Dict],
        titulo_relatorio: str = 'Relatório de Licitações B2G',
        incluir_resumo: bool = True
    ) -> bytes:
        """
        Exporta licitações para PDF
        
        Args:
            licitacoes: Lista de licitações
            titulo_relatorio: Título do relatório
            incluir_resumo: Se inclui página de resumo
            
        Returns:
            Bytes do arquivo PDF
        """
        try:
            from fpdf import FPDF
            
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            
            # Página de título
            pdf.add_page()
            pdf.set_font('Arial', 'B', 24)
            pdf.cell(0, 20, titulo_relatorio, ln=True, align='C')
            
            pdf.set_font('Arial', '', 12)
            pdf.cell(0, 10, f'Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}', ln=True, align='C')
            pdf.cell(0, 10, f'Total de licitações: {len(licitacoes)}', ln=True, align='C')
            pdf.ln(10)
            
            # Resumo
            if incluir_resumo:
                pdf.set_font('Arial', 'B', 16)
                pdf.cell(0, 10, 'Resumo Executivo', ln=True)
                pdf.set_font('Arial', '', 11)
                
                valor_total = sum(l.get('valor', 0) for l in licitacoes)
                match_medio = sum(l.get('match', 0) for l in licitacoes) / len(licitacoes) if licitacoes else 0
                
                pdf.cell(0, 8, f'Valor Total: R$ {valor_total:,.2f}', ln=True)
                pdf.cell(0, 8, f'Match Médio: {match_medio:.1f}%', ln=True)
                pdf.ln(10)
            
            # Lista de licitações
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, 'Licitações', ln=True)
            
            for idx, lic in enumerate(licitacoes[:50], 1):  # Limitar a 50
                pdf.add_page()
                
                pdf.set_font('Arial', 'B', 12)
                pdf.cell(0, 8, f"{idx}. {lic.get('titulo', 'N/A')[:80]}", ln=True)
                
                pdf.set_font('Arial', '', 10)
                pdf.cell(0, 6, f"Órgão: {lic.get('orgao', 'N/A')[:60]}", ln=True)
                pdf.cell(0, 6, f"UF: {lic.get('uf', 'N/A')} | Modalidade: {lic.get('modalidade', 'N/A')}", ln=True)
                pdf.cell(0, 6, f"Valor: R$ {lic.get('valor', 0):,.2f}", ln=True)
                pdf.cell(0, 6, f"Prazo: {lic.get('prazo', 'N/A')}", ln=True)
                
                match = lic.get('match', 0)
                if match > 0:
                    pdf.set_font('Arial', 'B', 10)
                    pdf.cell(0, 6, f"Match: {match}% - {lic.get('match_classificacao', 'N/A')}", ln=True)
                
                pdf.ln(5)
            
            # Gerar PDF
            return pdf.output(dest='S').encode('latin-1')
            
        except ImportError:
            logger.error("fpdf não instalado")
            return b''
        except Exception as e:
            logger.error(f"Erro ao gerar PDF: {e}", exc_info=True)
            return b''
    
    def gerar_relatorio_detalhado(
        self,
        licitacoes: List[Dict],
        empresa_data: Dict,
        match_scores: List[Dict]
    ) -> Dict:
        """
        Gera relatório detalhado com análises
        
        Args:
            licitacoes: Lista de licitações
            empresa_data: Dados da empresa
            match_scores: Scores de match
            
        Returns:
            Relatório estruturado
        """
        try:
            # Análises
            total = len(licitacoes)
            valor_total = sum(l.get('valor', 0) for l in licitacoes)
            
            # Por UF
            por_uf = {}
            for lic in licitacoes:
                uf = lic.get('uf', 'N/A')
                if uf not in por_uf:
                    por_uf[uf] = {'total': 0, 'valor': 0}
                por_uf[uf]['total'] += 1
                por_uf[uf]['valor'] += lic.get('valor', 0)
            
            # Por modalidade
            por_modalidade = {}
            for lic in licitacoes:
                mod = lic.get('modalidade', 'N/A')
                if mod not in por_modalidade:
                    por_modalidade[mod] = {'total': 0, 'valor': 0}
                por_modalidade[mod]['total'] += 1
                por_modalidade[mod]['valor'] += lic.get('valor', 0)
            
            # Top licitações por match
            top_matches = sorted(
                [(l, l.get('match', 0)) for l in licitacoes],
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            return {
                'gerado_em': datetime.now().isoformat(),
                'empresa': empresa_data,
                'resumo': {
                    'total_oportunidades': total,
                    'valor_total': valor_total,
                    'valor_medio': valor_total / total if total > 0 else 0
                },
                'distribuicao_uf': por_uf,
                'distribuicao_modalidade': por_modalidade,
                'top_10_matches': [
                    {
                        'titulo': l.get('titulo'),
                        'match': m,
                        'valor': l.get('valor')
                    }
                    for l, m in top_matches
                ]
            }
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório detalhado: {e}")
            return {}


# Função helper
def exportar_para_excel(licitacoes: List[Dict], incluir_match: bool = True) -> bytes:
    """
    Função auxiliar para exportar para Excel
    
    Args:
        licitacoes: Lista de licitações
        incluir_match: Se inclui match
        
    Returns:
        Bytes do Excel
    """
    service = ExportacaoB2GService()
    return service.exportar_excel(licitacoes, incluir_match)
