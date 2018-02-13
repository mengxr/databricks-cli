# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click

from databricks_cli.click_types import ClusterIdClickType, MutuallyExclusiveOption
from databricks_cli.configure.config import require_config
from databricks_cli.libraries.api import all_cluster_statuses, cluster_status, install_libraries, \
    uninstall_libraries
from databricks_cli.utils import CONTEXT_SETTINGS, eat_exceptions, pretty_format
from databricks_cli.version import print_version_callback, version


def _all_cluster_statuses():
    click.echo(pretty_format(all_cluster_statuses()))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get the status of all libraries.')
@require_config
@eat_exceptions # noqa
def all_cluster_statuses_cli():
    """
    Get the status of all libraries on all clusters. A status will be available for all libraries
    installed on this cluster via the API or the libraries UI as well as libraries set to be
    installed on all clusters via the libraries UI. If a library has been set to be installed on
    all clusters, is_library_for_all_clusters will be true,
    even if the library was also installed on this specific cluster.
    """
    _all_cluster_statuses()


def _cluster_status(cluster_id):
    click.echo(pretty_format(cluster_status(cluster_id)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get the status of all libraries for a specified cluster.')
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@require_config
@eat_exceptions # noqa
def cluster_status_cli(cluster_id):
    """
    Get the status of all libraries on all clusters. A status will be available for all libraries
    installed on this cluster via the API or the libraries UI as well as libraries set to be
    installed on all clusters via the libraries UI. If a library has been set to be installed on
    all clusters, is_library_for_all_clusters will be true,
    even if the library was also installed on this specific cluster.
    """
    _cluster_status(cluster_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Alias of `all-cluster-statuses` or `cluster-status`.')
@click.option('--cluster-id', type=ClusterIdClickType(), default=None,
              help=ClusterIdClickType.help)
@require_config
@eat_exceptions # noqa
def list_cli(cluster_id):
    """
    If the option --cluster-id is provided, then all libraries on that cluster will be listed,
    (cluster-status). If the option --cluster-id is omitted, then all libraries on all clusters
    will be listed (all-cluster-statuses).
    """
    if cluster_id is not None:
        _cluster_status(cluster_id)
    else:
        _all_cluster_statuses()


INSTALL_OPTIONS = ['jar', 'egg', 'maven-coordinates', 'pypi-package', 'cran-package']
JAR_HELP = 'JAR uploaded to dbfs or s3.'
EGG_HELP = 'Egg uploaded to dbfs or s3.'
MAVEN_COORDINATES_HELP = """
Maven coordinates in the form of GroupId:ArtifactId:Version (i.e. org.jsoup:jsoup:1.7.2).
"""
MAVEN_REPO_HELP = """
Maven repo to install the Maven package from. If omitted, both Maven 
Repository and Spark Packages are searched.
"""
MAVEN_EXCLUSION_HELP = """
List of dependences to exclude. For example: ["slf4j:slf4j", "*:hadoop-client"].
"""
PYPI_PACKAGE_HELP = """
The name of the pypi package to install. An optional exact version
specification is also supported. Examples: "simplejson" and "simplejson==3.8.0".
"""
PYPI_REPO_HELP = """
The repository where the package can be found. If not specified, the default pip index is used.
"""
CRAN_PACKAGE_HELP = """
The name of the CRAN package to install.
"""
CRAN_REPO_HELP = """
The repository where the package can be found. If not specified, the default CRAN repo is used.
"""


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Install libraries on a cluster.')
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--jar', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=JAR_HELP)
@click.option('--egg', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=EGG_HELP)
@click.option('--maven-coordinates', cls=MutuallyExclusiveOption,
              mutually_exclusive=INSTALL_OPTIONS, help=MAVEN_COORDINATES_HELP)
@click.option('--maven-repo', help=MAVEN_REPO_HELP)
@click.option('--maven-exclusion', multiple=True, help=MAVEN_EXCLUSION_HELP)
@click.option('--pypi-package', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=PYPI_PACKAGE_HELP)
@click.option('--pypi-repo', help=PYPI_REPO_HELP)
@click.option('--cran-package', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=CRAN_PACKAGE_HELP)
@click.option('--cran-repo', help=CRAN_REPO_HELP)
@require_config
@eat_exceptions # noqa
def install_cli(cluster_id, jar, egg, maven_coordinates, maven_repo, maven_exclusion, pypi_package, # noqa
                pypi_repo, cran_package, cran_repo):
    """
    Install libraries on a cluster. Libraries must be first uploaded to dbfs or s3
    (see `dbfs cp -h`). Unlike the API, only one library can be installed for each execution of
    `databricks libraries install`.

    Only provide one of [--jar, --egg, --maven-coordinates, --pypi-package, --cran-package].
    """
    maven_exclusion = list(maven_exclusion)
    libraries = []
    if jar is not None:
        libraries.append({'jar': jar})
    elif egg is not None:
        libraries.append({'egg': egg})
    elif maven_coordinates is not None:
        maven_library = {'maven': {'coordinates': maven_coordinates}}
        if maven_repo is not None:
            maven_library['maven']['repo'] = maven_repo
        if len(maven_exclusion) > 0:
            maven_library['maven']['exclusions'] = maven_exclusion
        libraries.append(maven_library)
    elif pypi_package is not None:
        pypi_library = {'pypi': {'package': pypi_package}}
        if pypi_repo is not None:
            pypi_library['pypi']['repo'] = pypi_repo
        libraries.append(pypi_library)
    elif cran_package is not None:
        cran_library = {'cran': {'package': cran_package}}
        if cran_repo is not None:
            cran_library['cran']['repo'] = cran_repo
        libraries.append(cran_library)
    assert len(libraries) == 1, 'Should have one library to install.'
    install_libraries(cluster_id, libraries)


def _uninstall_cli_exit_help(cluster_id):
    click.echo("WARNING: Uninstalling libraries requires a cluster restart.")
    click.echo("databricks clusters restart --cluster-id {}".format(cluster_id))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Uninstall libraries on a cluster.')
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--all', is_flag=True, default=False, help='If set, uninstall all libraries.')
@click.option('--jar', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=JAR_HELP)
@click.option('--egg', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=EGG_HELP)
@click.option('--maven-coordinates', cls=MutuallyExclusiveOption,
              mutually_exclusive=INSTALL_OPTIONS, help=MAVEN_COORDINATES_HELP)
@click.option('--maven-repo', help=MAVEN_REPO_HELP)
@click.option('--maven-exclusion', multiple=True, help=MAVEN_EXCLUSION_HELP)
@click.option('--pypi-package', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=PYPI_PACKAGE_HELP)
@click.option('--pypi-repo', help=PYPI_REPO_HELP)
@click.option('--cran-package', cls=MutuallyExclusiveOption, mutually_exclusive=INSTALL_OPTIONS,
              help=CRAN_PACKAGE_HELP)
@click.option('--cran-repo', help=CRAN_REPO_HELP)
@require_config
@eat_exceptions # noqa
def uninstall_cli(cluster_id, all, jar, egg, maven_coordinates, maven_repo, maven_exclusion, # noqa
                  pypi_package, pypi_repo, cran_package, cran_repo):
    """
    Mark libraries on a cluster to be uninstalled. Libraries which are marked to be uninstalled
    will stay attached until the cluster is restarted. (see `databricks clusters restart -h`).
    """
    if all:
        library_statuses = _cluster_status(cluster_id).get('library_statuses', [])
        libraries = [l_status['library'] for l_status in library_statuses]
        uninstall_libraries(cluster_id, libraries)
        _uninstall_cli_exit_help(cluster_id)
        return

    libraries = []
    maven_exclusion = list(maven_exclusion)
    if jar is not None:
        libraries.append({'jar': jar})
    elif egg is not None:
        libraries.append({'egg': egg})
    elif maven_coordinates is not None:
        maven_library = {'maven': {'coordinates': maven_coordinates}}
        if maven_repo is not None:
            maven_library['maven']['repo'] = maven_repo
        if len(maven_exclusion) > 0:
            maven_library['maven']['exclusions'] = maven_exclusion
        libraries.append(maven_library)
    elif pypi_package is not None:
        pypi_library = {'pypi': {'package': pypi_package}}
        if pypi_repo is not None:
            pypi_library['pypi']['repo'] = pypi_repo
        libraries.append(pypi_library)
    elif cran_package is not None:
        cran_library = {'cran': {'package': cran_package}}
        if cran_repo is not None:
            cran_library['cran']['repo'] = cran_repo
        libraries.append(cran_library)
    assert len(libraries) == 1, 'Should have one library to uninstall.'
    uninstall_libraries(cluster_id, libraries)
    _uninstall_cli_exit_help(cluster_id)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with jobs.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@require_config
@eat_exceptions
def libraries_group():
    """
    Utility to interact with libraries.

    This is a wrapper around the libraries API
    (https://docs.databricks.com/api/latest/libraries.html).
    """
    pass


libraries_group.add_command(list_cli, name='list')
libraries_group.add_command(all_cluster_statuses_cli, name='all-cluster-statuses')
libraries_group.add_command(cluster_status_cli, name='cluster-status')
libraries_group.add_command(install_cli, name='install')
libraries_group.add_command(uninstall_cli, name='uninstall')
