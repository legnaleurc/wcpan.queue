from setuptools import setup


setup(
        name='wcpan.worker',
        version='1.0.0.dev2',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        url='https://github.com/legnaleurc/wcpan.worker',
        packages=[
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
