-- src/main/resources/data.sql
-- Solo se ejecutará si las tablas no existen o están vacías

-- Crear secuencias si no existen
CREATE SEQUENCE IF NOT EXISTS customer_transactions_id_seq;
CREATE SEQUENCE IF NOT EXISTS daily_summaries_id_seq;

-- Crear tablas si no existen
CREATE TABLE IF NOT EXISTS customer_transactions (
    id BIGINT PRIMARY KEY DEFAULT nextval('customer_transactions_id_seq'),
    transaction_id VARCHAR(50) UNIQUE,
    customer_id VARCHAR(50),
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    transaction_date DATE,
    status VARCHAR(20) DEFAULT 'PENDING',
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_summaries (
    id BIGINT PRIMARY KEY DEFAULT nextval('daily_summaries_id_seq'),
    summary_date DATE,
    customer_id VARCHAR(50),
    total_amount DECIMAL(10,2),
    transaction_count INTEGER,
    currency VARCHAR(3),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    job_execution_id VARCHAR(100)
);

-- Crear índices si no existen
CREATE INDEX IF NOT EXISTS idx_customer_transactions_status
ON customer_transactions(status);

CREATE INDEX IF NOT EXISTS idx_daily_summaries_date
ON daily_summaries(summary_date);