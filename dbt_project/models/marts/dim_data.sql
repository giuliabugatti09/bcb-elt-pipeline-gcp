-- Dimensão de calendário: uma linha por dia, cobrindo o intervalo de
-- datas efetivamente presente nos dados (não um range arbitrário fixo).
-- Pré-calcula atributos de data para simplificar queries analíticas
-- (evita recalcular "é fim de semana?", "qual trimestre?" toda vez).

with limites as (

    select
        min(data_referencia) as data_min,
        max(data_referencia) as data_max
    from {{ ref('stg_bcb_series') }}

),

calendario as (

    select
        generate_series(data_min, data_max, interval '1 day')::date as data
    from limites

)

select
    data,
    extract(year from data)::int      as ano,
    extract(month from data)::int     as mes,
    extract(day from data)::int       as dia,
    extract(quarter from data)::int   as trimestre,
    -- ISO: 1 = segunda-feira, 7 = domingo
    extract(isodow from data)::int    as dia_semana_numero,
    to_char(data, 'TMDay')            as nome_dia_semana,
    to_char(data, 'TMMonth')          as nome_mes,
    (extract(isodow from data) in (6, 7)) as e_fim_de_semana

from calendario