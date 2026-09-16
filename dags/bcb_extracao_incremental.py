"""
DAG: bcb_extracao_incremental

Responsável por rodar diariamente a extração incremental (últimos N
valores) das séries do Banco Central configuradas em SERIES_BCB.

Esta DAG não contém lógica de negócio — ela apenas orquestra chamadas
para o código já testado em src/extractors/bcp_extractor.py. Essa
separação (orquestração vs. lógica) facilita testar o código de
extração isoladamente, sem precisar do Airflow rodando.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from src.extractors.bcp_extractor import SERIES_BCB, run_extraction

logger = logging.getLogger(__name__)


def alertar_falha_task(context: dict) -> None:
    """
    Callback disparado automaticamente quando uma task esgota todos os
    retries e falha definitivamente.

    Hoje isso só loga de forma estruturada (visível nos logs da task
    e do scheduler). Em um ambiente de produção real, este é o lugar
    onde entraria uma chamada a um webhook do Slack, um envio de email
    via EmailOperator, ou uma integração com PagerDuty/Opsgenie.

    O parâmetro 'context' é injetado automaticamente pelo Airflow e
    contém tudo sobre a execução: qual task, qual DAG, qual tentativa,
    a exceção que causou a falha, etc.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logger.error(
        f"[ALERTA] Task '{task_instance.task_id}' da DAG "
        f"'{task_instance.dag_id}' falhou definitivamente após "
        f"{task_instance.try_number} tentativa(s). "
        f"Execution date: {context['execution_date']}. "
        f"Erro: {exception}"
    )
    # TODO (produção): enviar notificação real, por exemplo:
    # requests.post(SLACK_WEBHOOK_URL, json={"text": mensagem})


default_args = {
    "owner": "gfbugatti",
    # Retries no nível do Airflow: reservados para falhas maiores
    # (instabilidade prolongada, não apenas um timeout pontual).
    # O retry rápido de falhas transitórias já acontece dentro do
    # próprio código de extração (_fetch_with_retry, 3 tentativas
    # com backoff de poucos segundos). Por isso mantemos este número
    # baixo aqui — não queremos multiplicar retries desnecessariamente.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_falha_task,
    # SLA: se uma task demorar mais que isso para concluir (contando
    # a partir do horário agendado da DAG), o Airflow registra um
    # "SLA miss" — visível em Browse -> SLA Misses na UI. Isso não
    # é uma falha, é um alerta de degradação de performance.
    "sla": timedelta(minutes=10),
}

# Lida da Airflow Variable "bcb_ultimos_n", configurável pela UI
# (Admin -> Variables) sem precisar alterar/redeployar este código.
# default_var garante que a DAG não quebra caso a Variable ainda
# não tenha sido criada (ex: ambiente novo, primeira subida).
ULTIMOS_N = int(Variable.get("bcb_ultimos_n", default_var=20))

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
    #
    # Nota de idempotência: mesmo que uma task seja re-executada (por
    # retry automático ou re-trigger manual), o resultado final é o
    # mesmo, porque save_raw_json() sempre sobrescreve o arquivo do
    # dia corrente em vez de anexar dados. Rodar 1x ou 5x no mesmo dia
    # produz o mesmo estado final — é isso que nos permite confiar
    # nos retries sem medo de duplicar ou corromper dados.
    for series_name in SERIES_BCB:
        PythonOperator(
            task_id=f"extrair_{series_name}",
            python_callable=run_extraction,
            op_kwargs={"series_name": series_name, "ultimos_n": ULTIMOS_N},
        )