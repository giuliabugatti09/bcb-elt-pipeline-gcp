"""
DAG: bcb_extracao_incremental

Responsável por rodar diariamente a extração incremental (últimos N
valores) das séries do Banco Central configuradas em SERIES_BCB.

Esta DAG não contém lógica de negócio — ela apenas orquestra chamadas
para o código já testado em src/extractors/bcp_extractor.py. Essa
separação (orquestração vs. lógica) facilita testar o código de
extração isoladamente, sem precisar do Airflow rodando.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extractors.bcp_extractor import SERIES_BCB, run_extraction

default_args = {
    "owner": "gfbugatti",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bcb_extracao_incremental",
    default_args=default_args,
    description="Extrai diariamente os valores mais recentes das séries do BCB (SGS)",
    # Roda em dias úteis (seg-sex) às 12h — a API do BCB atualiza os
    # dados do dia útil anterior/atual ao longo da manhã.
    schedule_interval="0 12 * * 1-5",
    start_date=datetime(2026, 9, 1),
    # catchup=False: não queremos que o Airflow tente "recuperar" execuções
    # de datas passadas entre start_date e hoje. Cada execução deve
    # representar o dia em que ela realmente rodou.
    catchup=False,
    tags=["bcb", "extracao", "incremental"],
) as dag:

    # Uma task por série: se a extração do dólar falhar, IPCA e Selic
    # continuam normalmente, e só a task do dólar é re-tentada.
    for series_name in SERIES_BCB:
        PythonOperator(
            task_id=f"extrair_{series_name}",
            python_callable=run_extraction,
            op_kwargs={"series_name": series_name, "ultimos_n": 20},
        )