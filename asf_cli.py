#!/usr/bin/env python3
import click
import json
import asf.client as client


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command('deploy')
@click.argument('state_machine_json', type=click.File('r'))
def cli_deploy(dag_json):
    client.asf2triggers(json.loads(dag_json.read()))


@cli.command('run')
@click.argument('dagrun_id', type=str)
def cli_run(dagrun_id):
    raise NotImplementedError()


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
