"""Setup.py module for the workflow's worker utilities.
All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.
This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution.
"""

# pytype: skip-file

import subprocess
from distutils.command.build import build as _build  # type: ignore

import setuptools


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
  """A build command class that will be invoked during package install.
  The package built using the current setup.py will be staged and later
  installed in the worker using `pip install package'. This class will be
  instantiated during install for this specific scenario and will trigger
  running the custom commands specified.
  """
  sub_commands = _build.sub_commands + [('CustomCommands', None)]


# Some custom command to run during setup. The command is not essential for this
# workflow. It is used here as an example. Each command will spawn a child
# process. Typically, these commands will include steps to install non-Python
# packages. For instance, to install a C++-based library libjpeg62 the following
# two commands will have to be added:
#
#     ['apt-get', 'update'],
#     ['apt-get', '--assume-yes', 'install', 'libjpeg62'],
#
# First, note that there is no need to use the sudo command because the setup
# script runs with appropriate access.
# Second, if apt-get tool is used then the first command needs to be 'apt-get
# update' so the tool refreshes itself and initializes links to download
# repositories.  Without this initial step the other apt-get install commands
# will fail with package not found errors. Note also --assume-yes option which
# shortcuts the interactive confirmation.
#
# Note that in this example custom commands will run after installing required
# packages. If you have a PyPI package that depends on one of the custom
# commands, move installation of the dependent package to the list of custom
# commands, e.g.:
#
#     ['pip', 'install', 'my_package'],
#
# TODO(BEAM-3237): Output from the custom commands are missing from the logs.
# The output of custom commands (including failures) will be logged in the
# worker-startup log.
CUSTOM_COMMANDS = [['echo', 'Custom command worked!']]


class CustomCommands(setuptools.Command):
  """A setuptools Command class able to run arbitrary commands."""
  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def RunCustomCommand(self, command_list):
    print('Running command: %s' % command_list)
    p = subprocess.Popen(
        command_list,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    # Can use communicate(input='y\n'.encode()) if the command run requires
    # some confirmation.
    stdout_data, _ = p.communicate()
    print('Command output: %s' % stdout_data)
    if p.returncode != 0:
      raise RuntimeError(
          'Command %s failed: exit code: %s' % (command_list, p.returncode))

  def run(self):
    for command in CUSTOM_COMMANDS:
      self.RunCustomCommand(command)


# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    'numpy',
    'apache-beam',
    'fastavro',
    'google-api-core',
    'google-api-python-client',
    'google-apitools',
    'google-auth',
    'google-auth-httplib2',
    'google-cloud-bigquery',
    'google-cloud-bigtable',
    'google-cloud-core',
    'google-cloud-datastore',
    'google-cloud-dlp',
    'google-cloud-firestore',
    'google-cloud-language',
    'google-cloud-pubsub',
    'google-cloud-recommendations-ai',
    'google-cloud-secret-manager',
    'google-cloud-spanner',
    'google-cloud-storage',
    'google-cloud-videointelligence',
    'google-cloud-vision',
    'google-crc32c',
    'google-resumable-media',
    'googleapis-common-protos',
    'grpc-google-iam-v1',
    'grpcio',
    'grpcio-gcp',
    'hdfs',
    'httplib2',
    'numpy',
    'pandas',
    'proto-plus',
    'protobuf',
    'pyarrow',
    'pydantic',
    'pydot',
    'requests',
    'pydantic[dotenv]',
]

setuptools.setup(
    name='mpi_hackdef',
    author='Vincent Brandon',
    author_email='vbrandon@utah.gov',
    url='https://github.com/frellnick/mpi_hackdev',
    version='0.0.1',
    description='MPI repository workflow package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    })