from distutils.core import setup
setup(name='pshell',
      version='1.0',
      description='Python SFTP-like client for Mediaflux',
      author='Sean Fleming',
      author_email='sean.fleming@pawsey.org.au',
      url='https://bitbucket.org/datapawsey/mfclient',
      packages=['data'],
      py_modules=['pshell','mfclient'],
      package_data={'data': ['.mf_config'],},
      )
