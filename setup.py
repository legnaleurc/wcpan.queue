import os.path as op

from setuptools import setup


with open(op.join(op.dirname(__file__), './README.rst')) as fin:
    long_description = fin.read()

setup(
        name='wcpan.worker',
        version='3.1.1',
        description='An asynchronous task queue with priority support.',
        long_description=long_description,
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        url='https://github.com/legnaleurc/wcpan.worker',
        packages=[
            'wcpan.worker',
        ],
        python_requires='>= 3.7',
        install_requires=[
            'async-timeout ~= 3.0.0',
            'wcpan.logger ~= 1.2.3',
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.7',
        ])
