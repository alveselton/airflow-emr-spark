import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# Pode definir um dicionário de parametros para que seja enviado ao construtor
default_args = {
	'owner': 'airflow',
	'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
	dag_id='save_date_in_file_txt',
	default_args=default_args,
	schedule_interval=timedelta(days=1),
	dagrun_timeout=timedelta(minutes=60),
	tags=['print_date_file']
)

# Imprime a data na saida padrao
t1 = BashOperator(
	task_id = 'print_date',
	bash_command = 'date',
	dag=dag
)

# Cria a pasta tmp caso ela não exista
t2 = BashOperator(
	task_id='make_directory',
	bash_command='mkdir -p /home/elton/Documents/EMR/tmp',
	dag=dag	
)

t3 = BashOperator(
	task_id='sleep',
	bash_command='sleep 5',
	retries=3,
	dag=dag
)

t4 = BashOperator(
	task_id='save_date',
	bash_command='date > /home/elton/Documents/EMR/tmp/date_output.txt',
	retries=3,
	dag=dag
)

t1 >> t2 >> t3 >> t4
