from setuptools import setup


setup(
        name='raii_logging',
        version='1.0.0',
        packages=[
            'wcpan',
            'wcpan.worker',
        ],
        install_requires=[
            'tornado',
            'wcpan.logger',
        ])
