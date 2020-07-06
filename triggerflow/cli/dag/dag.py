import click
import traceback

from ...dags.dag import DAG
from ...dags.dagrun import DAGRun
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
    """Evaluate a file containing a DAG definition and build the DAG object."""
    code = dag_file.read()

    try:
        click.echo("Evaluating DAG from file...")
        exec(code, globals(), locals())
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "DAG build failed -- {}".format(str(e)))
        click.echo(click.style(traceback.format_exc(), fg='red'))
        return

    dag_instances = [instance for var_name, instance in locals().items() if isinstance(instance, DAG)]
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
    """Run a DAG."""
    try:
        dag = DAG(dag_id=dag_name).load()
        dagrun = dag.run()
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "Could not run {} -- {}".format(dag_name, str(e)))
        click.echo(click.style(traceback.format_exc(), fg='red'))
        return

    click.echo(click.style("Ok: ", fg='green', bold=True) + "Dag run id: {}".format(dagrun.dagrun_id))


@click.command()
@click.argument('dagrun_id', type=click.STRING)
def result(dagrun_id):
    """Retrieve a DAG run result."""
    try:
        dag_run = DAGRun.load_run(dagrun_id=dagrun_id)
        res = dag_run.result()
        click.echo(click.style("Result: ", fg='green', bold=True) + str(res))
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "Could not retrieve result from {} -- {}".format(dagrun_id, str(e)))
        click.echo(click.style(traceback.format_exc(), fg='red'))
        return


dag.add_command(list)
dag.add_command(build)
dag.add_command(run)
dag.add_command(result)
