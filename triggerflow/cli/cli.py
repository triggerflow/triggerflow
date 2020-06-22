import click

from .dag import dag as dag_commands
from .statemachine import statemachine as statemachine_commands


@click.group()
def entry_point():
    pass


entry_point.add_command(dag_commands.dag)
entry_point.add_command(statemachine_commands.statemachine)

if __name__ == '__main__':
    entry_point()
