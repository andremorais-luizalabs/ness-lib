from ..config.config import HOOK
from airflow import configuration
from airflow.operators.slack_operator import SlackAPIPostOperator


def slack_failed_task(context):
    link = '<{base_url}/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}|logs>'.format(
        base_url=configuration.get('webserver', 'BASE_URL'),
        dag_id=context['dag'].dag_id,
        task_id=context['task_instance'].task_id,
        execution_date=context['ts'])
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel="#data-engineer-datena",
        token=HOOK,
        icon_url='https://abrilveja.files.wordpress.com/2018/04/jose-luiz-datena-20150723-001.jpg'
        text=
        '''
        :red_circle: Failure on: {dag} \n
        RunID: {run_id} \n
        Task: {task_instance} \n
        See {link} to debug
        '''.format(
                    dag=str(context['dag']),
                    run_id=str(context['run_id']),
                    task_instance=str(context['task_instance']),
                    link=link)
)
    return failed_alert.execute(context=context)
