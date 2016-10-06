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
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.5',
        ])
