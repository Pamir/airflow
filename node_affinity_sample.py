from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,

}

dag = DAG(
    "node_affinity_sample", default_args=default_args
)

# Define a helper function to generate affinity rules
def get_affinity_rules(label_key, label_value):
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": label_key,
                                "operator": "NotIn",
                                "values": [label_value],
                            }
                        ]
                    }
                ]
            }
        }
    }

# Define a task that runs on a node with the "type=fast" label
def task1_func():
    print("Hello from task1!")

task1 = PythonOperator(
    task_id="task1",
    python_callable=task1_func,
    executor_config={
        "KubernetesExecutor": {
            "affinity": get_affinity_rules("type", "slow"),
            "memory_request": "64Mi",
            "memory_limit": "128Mi",
        }
    },
    labels={"env": "prod"},
    dag=dag,
)

# Define a task that runs on a node with the "type=slow" label
def task2_func():
    print("Hello from task2!")

task2 = PythonOperator(
    task_id="task2",
    python_callable=task2_func,
    executor_config={
        "KubernetesExecutor": {
            "affinity": get_affinity_rules("type", "fast"),
            "memory_request": "64Mi",
            "memory_limit": "128Mi",
        }
    },
    labels={"env": "prod"},
    dag=dag,
)

# Set the order of task execution
task1 >> task2
