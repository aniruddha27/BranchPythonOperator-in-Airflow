from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
import random
import pandas as pd

# default arguments
default_args = {
	'owner': 'aniruddha',
	'depends_on_past': False,
	'start_date': days_ago(2),
	'retries': 2,
	'retry_delay': timedelta(seconds=10),
	'schedule_interval': '*/2 * * * *',
	'execution_timeout': timedelta(seconds=30)
}

# Function to simulate ML model. Generates random accuracy.
def dummy_lead_score_generator():
	
	random_user_id = ''.join([str(random.choice(range(1,10))) for i in range(0, 5)])
	random_accuracy = random.randrange(0, 10)/10

	with open('/home/aniruddha/Documents/dummy_lead.txt', 'w') as f:
		f.write(str(random_user_id) + ' ' + str(random_accuracy))

	return


# Function run when lead score is above threshold
def dummy_potential_lead_process():
	
	# list of sales officials
	sales_official_list = ['Karan', 'Arjun', 'Jay', 'Veeru', 'Jaya', 'Lakshmi']
	
	# list of preffered time slots on which users can be contacted
	user_preferred_time_slot = {
								 1: '10:00-10:30 AM',
								 2: '10:30-11:00 AM',
								 3: '11:00-11:30 AM',
								 4: '11:30-12:00 PM',
								 5: '14:00-14:30 PM',
								 6: '14:30-15:00 PM'
								}

	value = ''

	with open('/home/aniruddha/Documents/dummy_lead.txt', 'r') as f:
		value = f.read()

	# picking a random sales official
	random_sales_official = random.choice(sales_official_list)
	
	# picking a random time slot
	random_time_slot = user_preferred_time_slot[random.randrange(0, 7)]

	user_id = value.split()[0]

	lead_score = value.split()[1]

	# with open('/home/aniruddha/Documents/potential_lead.txt', 'w') as f:
	# 	f.write(user_id + ' ' lead_score + ' ' + random_sales_official + ' ' + random_time_slot)
	
	return


# Function run when lead score is below threshold
def dummy_rejected_lead_process():
	
	with open('/home/aniruddha/Documents/dummy_lead.txt', 'r') as f:
		value = f.read()

	user_id = value.split()[0]

	lead_score = value.split()[1]

	with open('/home/aniruddha/Documents/rejected_lead.txt', 'w') as f:
		f.write(user_id + ' ' + lead_score)
	
	return


# branch operator function
def lead_score_validator_branch():
	
	value = ''

	with open('/home/aniruddha/Documents/dummy_lead.txt', 'r') as f:
		value = f.read()

	lead_score = float(value.split()[1])

	if lead_score >= 0.65:
		return 'potential_lead_process'
	else:
		return 'rejected_lead_process'

# DAG
leads_dag = DAG(
		'sample_lead_dag',
		default_args = default_args,
		description = 'DAG to act of potential leads',
		schedule_interval = '*/2 * * * *'
)

task1 = PythonOperator(
			task_id = 'lead_score_generator',
			python_callable = dummy_lead_score_generator,
			dag = leads_dag
)

task2 = BranchPythonOperator(
			task_id = 'lead_score_validator_branch',
			python_callable = lead_score_validator_branch,
			trigger_rule = 'one_success',
			dag = leads_dag
)


task3 = PythonOperator(
			task_id = 'potential_lead_process',
			python_callable = dummy_potential_lead_process,
			dag = leads_dag
)

task4 = PythonOperator(
			task_id = 'rejected_lead_process',
			python_callable = dummy_rejected_lead_process,
			dag = leads_dag
)

task5 = DummyOperator(
			task_id = 'reporting',
			dag = leads_dag
)

task1 >> task2 >> [task3, task4] >> task5
