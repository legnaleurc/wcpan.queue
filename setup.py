from setuptools import setup


setup(
        name='wcpan.worker',
        version='1.0.0.dev1',
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
