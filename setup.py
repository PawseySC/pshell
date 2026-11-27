from distutils.core import setup
setup(name='pshell',
      version='1.0',
      description='Python SFTP-like client for Mediaflux',
      author='Sean Fleming',
      author_email='sean.fleming@pawsey.org.au',
      url='https://bitbucket.org/datapawsey/mfclient',
      license='Apache-2.0',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
      ],
      packages=['data'],
      py_modules=['pshell','parser', 'mfclient', 's3client', 'util'],
      )
