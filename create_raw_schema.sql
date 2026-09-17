-- Schema dedicado para a camada raw (dados brutos, quase 1:1 com a fonte).
-- Separar por schema (em vez de só por nome de tabela) deixa explícito,
-- desde já, que dbt vai criar schemas irmãos depois: staging, marts.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.bcb_series (
    id                    BIGSERIAL PRIMARY KEY,
    series_name           TEXT NOT NULL,
    data_referencia       DATE NOT NULL,
    valor                 NUMERIC NOT NULL,
    extraction_timestamp  TIMESTAMPTZ NOT NULL,
    source_file           TEXT NOT NULL,
    inserted_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Chave única que viabiliza o UPSERT idempotente: uma série só pode
    -- ter um valor por data. Reprocessar o mesmo dia atualiza a linha
    -- existente em vez de criar uma duplicata.
    CONSTRAINT uq_bcb_series_nome_data UNIQUE (series_name, data_referencia)
);

-- Índice para acelerar consultas futuras filtrando por série (comum
-- quando o dbt for construir os models de staging).
CREATE INDEX IF NOT EXISTS idx_bcb_series_series_name
    ON raw.bcb_series (series_name);

COMMENT ON TABLE raw.bcb_series IS
    'Camada raw: valores das séries do BCB, quase 1:1 com o JSON original da API. Sem transformação de negócio aplicada.';