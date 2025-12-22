"""
Migration: Add B2G Enhancement Tables
Cria tabelas para favoritos, alertas e notificações B2G
"""

import sqlite3
from pathlib import Path

def run_migration(db_path: str = 'backend/users.db'):
    """Executa migration para criar tabelas B2G"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        print("🚀 Iniciando migration B2G...")
        
        # Tabela de Favoritos
        print("  📌 Criando tabela licitacoes_favoritas...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS licitacoes_favoritas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                licitacao_id TEXT NOT NULL,
                licitacao_data TEXT,
                notas TEXT,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
                UNIQUE(usuario_id, licitacao_id)
            )
        ''')
        
        # Índices para favoritos
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_favoritos_usuario 
            ON licitacoes_favoritas(usuario_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_favoritos_licitacao 
            ON licitacoes_favoritas(licitacao_id)
        ''')
        
        # Tabela de Alertas B2G
        print("  🔔 Criando tabela alertas_b2g...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alertas_b2g (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                nome TEXT NOT NULL,
                tipo TEXT NOT NULL, -- 'busca_salva', 'alto_match', 'prazo', 'orgao'
                criterios TEXT, -- JSON com critérios
                ativo BOOLEAN DEFAULT TRUE,
                ultima_execucao TIMESTAMP,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_alertas_usuario 
            ON alertas_b2g(usuario_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_alertas_ativo 
            ON alertas_b2g(ativo)
        ''')
        
        # Tabela de Notificações
        print("  📬 Criando tabela notificacoes...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notificacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                tipo TEXT NOT NULL, -- 'nova_licitacao', 'prazo', 'match_alto'
                titulo TEXT NOT NULL,
                mensagem TEXT,
                lida BOOLEAN DEFAULT FALSE,
                link TEXT,
                metadados TEXT, -- JSON
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                lida_em TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_notif_usuario 
            ON notificacoes(usuario_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_notif_lida 
            ON notificacoes(lida)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_notif_criado 
            ON notificacoes(criado_em DESC)
        ''')
        
        # Tabela de Histórico de Visualizações
        print("  👁️ Criando tabela licitacoes_visualizadas...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS licitacoes_visualizadas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                licitacao_id TEXT NOT NULL,
                titulo TEXT,
                visualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_visualizadas_usuario 
            ON licitacoes_visualizadas(usuario_id)
        ''')
        
        # Tabela de Cache de Match
        print("  💾 Criando tabela match_cache...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT NOT NULL,
                licitacao_id TEXT NOT NULL,
                score INTEGER NOT NULL,
                classificacao TEXT,
                explicacao TEXT,
                componentes TEXT, -- JSON
                calculado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expira_em TIMESTAMP,
                UNIQUE(cnpj, licitacao_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_match_cache_cnpj 
            ON match_cache(cnpj)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_match_cache_expira 
            ON match_cache(expira_em)
        ''')
        
        # Tabela de Preferências de Usuário B2G
        print("  ⚙️ Criando tabela preferencias_b2g...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS preferencias_b2g (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL UNIQUE,
                notificacoes_email BOOLEAN DEFAULT TRUE,
                notificacoes_push BOOLEAN DEFAULT TRUE,
                frequencia_alertas TEXT DEFAULT 'diario', -- 'tempo_real', 'diario', 'semanal'
                ufs_interesse TEXT, -- JSON array
                modalidades_interesse TEXT, -- JSON array
                faixa_valor_min REAL,
                faixa_valor_max REAL,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
            )
        ''')
        
        # Commit das alterações
        conn.commit()
        print("✅ Migration concluída com sucesso!")
        
        # Verificar tabelas criadas
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND (
                name LIKE 'licitacoes_%' OR 
                name LIKE 'alertas_%' OR 
                name = 'notificacoes' OR
                name = 'match_cache' OR
                name LIKE 'preferencias_%'
            )
            ORDER BY name
        ''')
        
        tabelas = cursor.fetchall()
        print(f"\n📋 Tabelas criadas/verificadas ({len(tabelas)}):")
        for tabela in tabelas:
            print(f"   ✓ {tabela[0]}")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro na migration: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    # Executar migration
    db_path = Path(__file__).parent.parent / 'users.db'
    run_migration(str(db_path))
