-- Model de staging: limpeza leve sobre raw.bcb_series.
--
-- O que fazemos aqui (e SÓ isso):
--   - Garantir tipos de dado explícitos
--   - Nomes de coluna consistentes
--   - Nenhuma lógica de negócio, nenhum cálculo, nenhum filtro por
--     regra de negócio — isso vive na camada de marts (Dia 14).

with source as (

    select * from {{ source('raw', 'bcb_series') }}

),

renomeado as (

    select
        series_name                    as nome_serie,
        data_referencia                as data_referencia,
        valor::numeric(18, 6)          as valor,
        extraction_timestamp::timestamptz as extraido_em,
        source_file                    as arquivo_origem

    from source

)

select * from renomeado