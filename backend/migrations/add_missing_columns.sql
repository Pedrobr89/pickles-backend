-- Migration SQL para adicionar colunas faltantes na tabela users
-- Execute este SQL no seu banco PostgreSQL

-- Adicionar colunas relacionadas a subscription (se não existirem)
ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR(120);
ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_status VARCHAR(50) DEFAULT 'free';
ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_queries INTEGER DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_query_date DATE;

-- Adicionar colunas relacionadas a recuperação de senha (se não existirem)
ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token_expiry TIMESTAMP;

-- Adicionar github_id se não existir  
ALTER TABLE users ADD COLUMN IF NOT EXISTS github_id VARCHAR(100) UNIQUE;

-- Adicionar is_admin se não existir
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;

-- Verificar estrutura final
SELECT column_name, data_type, character_maximum_length 
FROM information_schema.columns 
WHERE table_name = 'users'
ORDER BY ordinal_position;
