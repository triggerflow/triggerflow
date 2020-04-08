from setuptools import setup, find_packages

setup(
    name='eventprocessor',
    version='0.1.1',
    url='https://github.com/eventprocessor',
    author='eventprocessor Team',
    description='Event-based Orchestration of Serverless Workflows',
    author_email='cloudlab@urv.cat',
    packages=find_packages(),
    install_requires=[
        'psutil', 'gevent', 'cloudant', 'pika==0.13.1', 'flask',
        'PyYAML', 'confluent_kafka', 'dill', 'jsonpath_ng',
        'requests', 'python-dateutil', 'docker', 'redis', 'boto3'
    ],    
    include_package_data=True,
    entry_points='''
        [console_scripts]
        eventprocessor=eventprocessor.cli:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
