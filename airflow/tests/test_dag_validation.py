import pytest
from airflow.models import DagBag


@pytest.fixture(params=["airflow/dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_requires_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        error_msg = "No tag for DAG {dag_name}".format(dag_name=dag)
        assert dag.tags, error_msg


def test_requires_descripcion(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        error_msg = "No description for DAG {dag_name}".format(dag_name=dag)
        assert dag.description, error_msg


def test_two_retries(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        error_msg = "Retries not set to 2 for DAG {dag_name}".format(dag_name=dag)
        assert dag.default_args["retries"] == 2, error_msg
