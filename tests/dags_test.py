from pprint import pprint
from triggerflow.dags import DAG, DAGRun

dag = DAG.import_from_json('ExampleDag.json')
pprint(dag.get_json_eventsource())

