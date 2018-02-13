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

from click import ParamType, Option, UsageError


class OutputClickType(ParamType):
    name = 'FORMAT'
    help = 'can be "JSON" or "TABLE". Set to TABLE by default.'

    def convert(self, value, param, ctx):
        if value is None:
            return value
        if value.lower() != 'json' and value.lower() != 'table':
            raise RuntimeError('output must be "json" or "table"')
        return value

    @classmethod
    def is_json(cls, value):
        return value is not None and value.lower() == 'json'

    @classmethod
    def is_table(cls, value):
        return value is not None and value.lower() == 'table'


class JsonClickType(ParamType):
    name = 'JSON'

    @classmethod
    def help(cls, endpoint):
        return 'JSON string to POST to {}.'.format(endpoint)


class JobIdClickType(ParamType):
    name = 'JOB_ID'
    help = 'Can be found in the URL at https://*.cloud.databricks.com/#job/$JOB_ID.'


class RunIdClickType(ParamType):
    name = 'RUN_ID'


class ClusterIdClickType(ParamType):
    name = 'CLUSTER_ID'
    help = ('Can be found in the URL at '
            'https://*.cloud.databricks.com/#/setting/clusters/$CLUSTER_ID/configuration.')


class MutuallyExclusiveOption(Option):
    """
    Adapted from https://gist.github.com/jacobtolar/fb80d5552a9a9dfc32b12a829fa21c0c.
    """
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = kwargs.pop('mutually_exclusive', [])
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        cleaned_opts = set([o.replace('_', '-') for o in opts.keys()])
        cleaned_name = self.name.replace('_', '-')
        if len(set(self.mutually_exclusive).intersection(cleaned_opts)) > 1:
            other_opts = [opt for opt in self.mutually_exclusive if opt != cleaned_name]
            raise UsageError(
                'Illegal usage: `{}` is mutually exclusive with '
                'arguments {}.'.format(cleaned_name, other_opts)
            )
        elif set(self.mutually_exclusive).isdisjoint(cleaned_opts):
            missing_options = ['--{}'.format(o) for o in self.mutually_exclusive]
            raise UsageError('Missing one of {}.'.format(missing_options))

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)
