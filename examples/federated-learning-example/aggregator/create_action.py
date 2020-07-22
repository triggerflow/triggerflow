import click
import logging
from triggerflow.libs.ibm_cloudfunctions_client import CloudFunctionsClient

@click.command()
@click.option('--endpoint', type=str)
@click.option('--namespace', type=str)
@click.option('--api-key', type=str)
def main(endpoint, namespace, api_key):
    client = CloudFunctionsClient(
        endpoint=endpoint,
        namespace=namespace,
        api_key=api_key
        )

    with open('aggregator.py', 'r') as f:
        code = f.read()

    package = 'triggerflow-example'
    client.create_package(package)

    action_name = 'aggregator'
    client.create_action(
        package=package,
        action_name=,
        image_name='dhak/fedlearn-aggregator',
        code=code,
        is_binary=False,
        memory=128
        )

    url = '{}/api/v1/namespaces/{}/actions/{}/{}'.format(
        endpoint,
        namespace,
        package,
        action_name
    )
    print('url:', url)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    main()