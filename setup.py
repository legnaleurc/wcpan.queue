from distutils.core import setup


setup(name='raii_logging',
      version='1.0.0',
      packages=['raii_logging'],
      install_requires=[
        'tornado',
        'git+https://github.com/legnaleurc/raii_logging.git#egg=raii_logging',
      ])
