-- Tabela fato: um valor por indicador por data.
--
-- Chaves de junção com as dimensões (não são FKs formais no Postgres,
-- só uma convenção que o dbt não impõe automaticamente — vamos validar
-- essa relação com testes no Dia 15):
--   indicador_id  -> dim_indicador.series_name
--   data          -> dim_data.data

select
    nome_serie          as indicador_id,
    data_referencia      as data,
    valor,
    extraido_em,
    arquivo_origem

from {{ ref('stg_bcb_series') }}