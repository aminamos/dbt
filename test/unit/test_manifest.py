import unittest
from unittest import mock

import copy
from datetime import datetime

import dbt.flags
from dbt import tracking
from dbt.contracts.graph.manifest import Manifest, ManifestMetadata
from dbt.contracts.graph.parsed import ParsedNode, DependsOn, NodeConfig
from dbt.contracts.graph.compiled import CompiledNode
from dbt.node_types import NodeType
import freezegun


REQUIRED_PARSED_NODE_KEYS = frozenset({
    'alias', 'empty', 'tags', 'config', 'unique_id', 'refs', 'sources',
    'depends_on', 'database', 'schema', 'name', 'resource_type',
    'package_name', 'root_path', 'path', 'original_file_path', 'raw_sql',
    'docrefs', 'description', 'columns', 'fqn', 'build_path', 'patch_path',
    'index',
})

REQUIRED_COMPILED_NODE_KEYS = frozenset(REQUIRED_PARSED_NODE_KEYS | {
    'compiled', 'extra_ctes_injected', 'extra_ctes', 'compiled_sql',
    'injected_sql', 'wrapped_sql'
})


class ManifestTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        })

        self.nested_nodes = {
            'model.snowplow.events': ParsedNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.snowplow.events',
                fqn=['snowplow', 'events'],
                empty=False,
                package_name='snowplow',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.events': ParsedNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.root.events',
                fqn=['root', 'events'],
                empty=False,
                package_name='root',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.dep': ParsedNode(
                name='dep',
                database='dbt',
                schema='analytics',
                alias='dep',
                resource_type=NodeType.Model,
                unique_id='model.root.dep',
                fqn=['root', 'dep'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.nested': ParsedNode(
                name='nested',
                database='dbt',
                schema='analytics',
                alias='nested',
                resource_type=NodeType.Model,
                unique_id='model.root.nested',
                fqn=['root', 'nested'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.dep']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.sibling': ParsedNode(
                name='sibling',
                database='dbt',
                schema='analytics',
                alias='sibling',
                resource_type=NodeType.Model,
                unique_id='model.root.sibling',
                fqn=['root', 'sibling'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.multi': ParsedNode(
                name='multi',
                database='dbt',
                schema='analytics',
                alias='multi',
                resource_type=NodeType.Model,
                unique_id='model.root.multi',
                fqn=['root', 'multi'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.nested', 'model.root.sibling']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
        }
        for node in self.nested_nodes.values():
            node.validate(node.to_dict())

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        manifest = Manifest(nodes={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'macros': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {},
                'disabled': [],
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        serialized = manifest.writable_manifest().to_dict()
        self.assertEqual(serialized['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['docs'], {})
        self.assertEqual(serialized['disabled'], [])
        parent_map = serialized['parent_map']
        child_map = serialized['child_map']
        # make sure there aren't any extra/missing keys.
        self.assertEqual(set(parent_map), set(nodes))
        self.assertEqual(set(child_map), set(nodes))
        self.assertEqual(
            parent_map['model.root.sibling'],
            ['model.root.events']
        )
        self.assertEqual(
            parent_map['model.root.nested'],
            ['model.root.dep']
        )
        self.assertEqual(
            parent_map['model.root.dep'],
            ['model.root.events']
        )
        # order doesn't matter.
        self.assertEqual(
            set(parent_map['model.root.multi']),
            set(['model.root.nested', 'model.root.sibling'])
        )
        self.assertEqual(
            parent_map['model.root.events'],
            [],
        )
        self.assertEqual(
            parent_map['model.snowplow.events'],
            [],
        )

        self.assertEqual(
            child_map['model.root.sibling'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.nested'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.dep'],
            ['model.root.nested']
        )
        self.assertEqual(
            child_map['model.root.multi'],
            []
        )
        self.assertEqual(
            set(child_map['model.root.events']),
            set(['model.root.dep', 'model.root.sibling'])
        )
        self.assertEqual(
            child_map['model.snowplow.events'],
            []
        )

    def test__to_flat_graph(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        flat_graph = manifest.to_flat_graph()
        flat_nodes = flat_graph['nodes']
        self.assertEqual(set(flat_graph), set(['nodes', 'macros']))
        self.assertEqual(flat_graph['macros'], {})
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        for node in flat_nodes.values():
            self.assertEqual(frozenset(node), REQUIRED_PARSED_NODE_KEYS)

    @mock.patch.object(tracking, 'active_user')
    def test_get_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        mock_user.do_not_track = True
        config = mock.MagicMock()
        # md5 of 'test'
        config.hashed_name.return_value = '098f6bcd4621d373cade4e832627b4f6'
        self.assertEqual(
            Manifest.get_metadata(config),
            ManifestMetadata(
                project_id='098f6bcd4621d373cade4e832627b4f6',
                user_id='cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                send_anonymous_usage_stats=False,
            )
        )

    @mock.patch.object(tracking, 'active_user')
    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test_no_nodes_with_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        mock_user.do_not_track = True
        config = mock.MagicMock()
        # md5 of 'test'
        config.hashed_name.return_value = '098f6bcd4621d373cade4e832627b4f6'
        manifest = Manifest(nodes={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            config=config)
        metadata = {
            'project_id': '098f6bcd4621d373cade4e832627b4f6',
            'user_id': 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
            'send_anonymous_usage_stats': False,
        }
        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'macros': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {
                    'project_id': '098f6bcd4621d373cade4e832627b4f6',
                    'user_id': 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                    'send_anonymous_usage_stats': False,
                },
                'disabled': [],
            }
        )

    def test_get_resource_fqns_empty(self):
        manifest = Manifest(nodes={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        self.assertEqual(manifest.get_resource_fqns(), {})

    def test_get_resource_fqns(self):
        nodes = copy.copy(self.nested_nodes)
        nodes['seed.root.seed'] = ParsedNode(
            name='seed',
            database='dbt',
            schema='analytics',
            alias='seed',
            resource_type='seed',
            unique_id='seed.root.seed',
            fqn=['root', 'seed'],
            empty=False,
            package_name='root',
            refs=[['events']],
            sources=[],
            depends_on=DependsOn(),
            config=self.model_config,
            tags=[],
            path='seed.csv',
            original_file_path='seed.csv',
            root_path='',
            raw_sql='-- csv --'
        )
        manifest = Manifest(nodes=nodes, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        expect = {
            'models': frozenset([
                ('snowplow', 'events'),
                ('root', 'events'),
                ('root', 'dep'),
                ('root', 'nested'),
                ('root', 'sibling'),
                ('root', 'multi'),
            ]),
            'seeds': frozenset([('root', 'seed')]),
        }
        resource_fqns = manifest.get_resource_fqns()
        self.assertEqual(resource_fqns, expect)


class MixedManifestTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        })

        self.nested_nodes = {
            'model.snowplow.events': CompiledNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.snowplow.events',
                fqn=['snowplow', 'events'],
                empty=False,
                package_name='snowplow',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter',
                compiled=True,
                compiled_sql='also does not matter',
                extra_ctes_injected=True,
                injected_sql=None,
                extra_ctes=[]
            ),
            'model.root.events': CompiledNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.root.events',
                fqn=['root', 'events'],
                empty=False,
                package_name='root',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter',
                compiled=True,
                compiled_sql='also does not matter',
                extra_ctes_injected=True,
                injected_sql='and this also does not matter',
                extra_ctes=[]
            ),
            'model.root.dep': ParsedNode(
                name='dep',
                database='dbt',
                schema='analytics',
                alias='dep',
                resource_type=NodeType.Model,
                unique_id='model.root.dep',
                fqn=['root', 'dep'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.nested': ParsedNode(
                name='nested',
                database='dbt',
                schema='analytics',
                alias='nested',
                resource_type=NodeType.Model,
                unique_id='model.root.nested',
                fqn=['root', 'nested'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.dep']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.sibling': ParsedNode(
                name='sibling',
                database='dbt',
                schema='analytics',
                alias='sibling',
                resource_type=NodeType.Model,
                unique_id='model.root.sibling',
                fqn=['root', 'sibling'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.multi': ParsedNode(
                name='multi',
                database='dbt',
                schema='analytics',
                alias='multi',
                resource_type=NodeType.Model,
                unique_id='model.root.multi',
                fqn=['root', 'multi'],
                empty=False,
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.nested', 'model.root.sibling']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
        }

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        manifest = Manifest(nodes={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'macros': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {},
                'disabled': [],
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        serialized = manifest.writable_manifest().to_dict()
        self.assertEqual(serialized['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['disabled'], [])
        parent_map = serialized['parent_map']
        child_map = serialized['child_map']
        # make sure there aren't any extra/missing keys.
        self.assertEqual(set(parent_map), set(nodes))
        self.assertEqual(set(child_map), set(nodes))
        self.assertEqual(
            parent_map['model.root.sibling'],
            ['model.root.events']
        )
        self.assertEqual(
            parent_map['model.root.nested'],
            ['model.root.dep']
        )
        self.assertEqual(
            parent_map['model.root.dep'],
            ['model.root.events']
        )
        # order doesn't matter.
        self.assertEqual(
            set(parent_map['model.root.multi']),
            set(['model.root.nested', 'model.root.sibling'])
        )
        self.assertEqual(
            parent_map['model.root.events'],
            [],
        )
        self.assertEqual(
            parent_map['model.snowplow.events'],
            [],
        )

        self.assertEqual(
            child_map['model.root.sibling'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.nested'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.dep'],
            ['model.root.nested']
        )
        self.assertEqual(
            child_map['model.root.multi'],
            []
        )
        self.assertEqual(
            set(child_map['model.root.events']),
            set(['model.root.dep', 'model.root.sibling'])
        )
        self.assertEqual(
            child_map['model.snowplow.events'],
            []
        )

    def test__to_flat_graph(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[])
        flat_graph = manifest.to_flat_graph()
        flat_nodes = flat_graph['nodes']
        self.assertEqual(set(flat_graph), set(['nodes', 'macros']))
        self.assertEqual(flat_graph['macros'], {})
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        compiled_count = 0
        for node in flat_nodes.values():
            if node.get('compiled'):
                self.assertEqual(frozenset(node), REQUIRED_COMPILED_NODE_KEYS)
                compiled_count += 1
            else:
                self.assertEqual(frozenset(node), REQUIRED_PARSED_NODE_KEYS)
        self.assertEqual(compiled_count, 2)
