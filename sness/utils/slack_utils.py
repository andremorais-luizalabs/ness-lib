from ..config.config import HOOK
from airflow import configuration
from airflow.operators.slack_operator import SlackAPIPostOperator


def slack_failed_task(context):
    link = '<{base_url}/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}|logs>'.format(
        base_url=configuration.get('webserver', 'BASE_URL'),
        dag_id = context['dag'].dag_id,
        task_id = context['task_instance'].task_id,
        execution_date = context['ts'])
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel="#data-engineer-datena",
        token=HOOK,
        text=':red_circle: Failure on: ' + str(context['dag']) + '\nRun ID: ' \
             + str(context['run_id']) +'\nTask: ' + str(context['task_instance']) \
             + '\nSee ' + link + ' to debug')
    return failed_alert.execute(context=context)