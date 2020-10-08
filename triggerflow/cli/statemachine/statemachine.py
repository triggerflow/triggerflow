import click
import json

from ...statemachine import StateMachine, trigger_statemachine


@click.group()
def statemachine():
    """Manage Triggerflow DAGs"""
    pass


@click.command()
@click.argument('statemachine_file', type=click.File('r'))
def deploy(statemachine_file):
    """Evaluate a file containing a Amazon Step Functions state machine definition and deploy """
    try:
        sm = StateMachine.string(json.loads(statemachine_file.read()))
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "{}".format(repr(e)))
        return

    click.echo(click.style("Ok: ", fg='green', bold=True) + "State machine run id: {}".format(sm.run_id))


@click.command()
@click.argument('run_id', type=click.STRING)
def trigger(run_id):
    """Manually trigger the execution of a deployed statemachine"""
    try:
        trigger_statemachine(run_id)
    except Exception as e:
        click.echo(click.style("Error: ", fg='red', bold=True) + "{}".format(repr(e)))
        return

    click.echo(click.style("Ok: ", fg='green', bold=True) + "State machine {} started execution".format(run_id))


statemachine.add_command(deploy)
statemachine.add_command(trigger)
