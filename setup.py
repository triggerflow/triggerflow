from setuptools import setup, find_packages

setup(
    name='triggerflow',
    version='0.1.1',
    url='https://github.com/triggerflow',
    author='Triggerflow Team',
    description='Event-based Orchestration of Serverless Workflows',
    author_email='cloudlab@urv.cat',
    packages=find_packages(),
    install_requires=[
        'psutil', 'gevent', 'pika==0.13.1', 'flask',
        'PyYAML', 'confluent-kafka', 'dill', 'jsonpath_ng',
        'requests', 'python-dateutil', 'docker', 'redis', 'boto3',
        'click', 'graphviz', 'arnparse'
    ],    
    include_package_data=True,
    entry_points='''
        [console_scripts]
        triggerflow=triggerflow.cli.cli:entry_point
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
