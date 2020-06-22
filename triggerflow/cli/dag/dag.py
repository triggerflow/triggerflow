import click
import traceback

from ...dags.dag import DAG
from ...cache import TriggerflowCache


@click.group()
def dag():
    """Manage Triggerflow DAGs"""
    pass


@click.command()
def list():
    """List built DAGs"""
    for dag in TriggerflowCache.list_dir('dag'):
        try:
            click.echo(dag.rsplit('.')[0])
        except Exception:
            continue


@click.command()
@click.argument('dag_file', type=click.File('r'))
def build(dag_file):
    """Evaluate a file containing a DAG definition and build the DAG object"""
    code = dag_file.read()

    code_vars = {}
    try:
        exec(code, {}, code_vars)
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "DAG build failed -- {}".format(str(e)))
        return

    dag_instances = [instance for var_name, instance in code_vars.items() if isinstance(instance, DAG)]
    if not dag_instances:
        click.echo(click.style("Error: ", fg='red', bold=True) + "No instance of DAG found in script file")
        return
    elif len(dag_instances) != 1:
        click.echo(click.style("Error: ", fg='red', bold=True) + "More than one instance of DAG found in script "
                                                                 "-- please instantiate only one DAG object")
        return

    dag = dag_instances.pop()
    dag.save()
    click.echo(click.style("Ok: ", fg='green', bold=True) + "Dag \"{}\" built".format(dag.dag_id))


@click.command()
@click.argument('dag_name', type=click.STRING)
def run(dag_name):
    """Run a DAG"""
    try:
        dag = DAG(dag_id=dag_name).load()
        dagrun = dag.run()
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "Could not run {} -- {}".format(dag_name, str(e)))
        traceback.print_exc()
        return

    click.echo(click.style("Ok: ", fg='green', bold=True) + "Dag run id: {}".format(dagrun.dagrun_id))


dag.add_command(list)
dag.add_command(build)
dag.add_command(run)
