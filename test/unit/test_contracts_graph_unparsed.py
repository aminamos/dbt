from datetime import timedelta
from unittest import TestCase

from hologram import ValidationError

from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedRunHook, UnparsedMacro, Time, TimePeriod,
    FreshnessStatus, FreshnessThreshold, Quoting, UnparsedSourceDefinition,
    UnparsedSourceTableDefinition, UnparsedDocumentationFile, NamedTested,
    UnparsedNodeUpdate
)
from dbt.node_types import NodeType


class ContractTestCase(TestCase):
    ContractType = None

    def assert_to_dict(self, obj, dct):
        self.assertEqual(obj.to_dict(), dct)

    def assert_from_dict(self, obj, dct):
        self.assertEqual(self.ContractType.from_dict(dct),  obj)

    def assert_symmetric(self, obj, dct):
        self.assert_to_dict(obj, dct)
        self.assert_from_dict(obj, dct)

    def assert_fails_validation(self, dct, cls=None):
        if cls is None:
            cls = self.ContractType

        with self.assertRaises(ValidationError):
            cls.from_dict(dct)


class TestUnparsedNode(ContractTestCase):
    ContractType = UnparsedNode

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Model,
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("thing") }}',
        }
        node = UnparsedNode(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("thing") }}',
            name='foo',
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)

    def test_empty(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Model,
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '  \n',
        }
        node = UnparsedNode(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='  \n',
            name='foo',
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertTrue(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)

    def test_bad_type(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Source,  # not valid!
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("thing") }}',
        }
        self.assert_fails_validation(node_dict)


class TestUnparsedRunHook(ContractTestCase):
    ContractType = UnparsedRunHook

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': 'test/dbt_project.yml',
            'resource_type': NodeType.Operation,
            'path': '/root/dbt_project.yml',
            'original_file_path': '/root/dbt_project.yml',
            'package_name': 'test',
            'raw_sql': 'GRANT select on dbt_postgres',
            'index': 4
        }
        node = UnparsedRunHook(
            package_name='test',
            root_path='test/dbt_project.yml',
            path='/root/dbt_project.yml',
            original_file_path='/root/dbt_project.yml',
            raw_sql='GRANT select on dbt_postgres',
            name='foo',
            resource_type=NodeType.Operation,
            index=4,
        )
        self.assert_symmetric(node, node_dict)
        self.assert_fails_validation(node_dict, cls=UnparsedNode)

    def test_bad_type(self):
        node_dict = {
            'name': 'foo',
            'root_path': 'test/dbt_project.yml',
            'resource_type': NodeType.Model,  # invalid
            'path': '/root/dbt_project.yml',
            'original_file_path': '/root/dbt_project.yml',
            'package_name': 'test',
            'raw_sql': 'GRANT select on dbt_postgres',
            'index': 4
        }
        self.assert_fails_validation(node_dict)


class TestFreshnessThreshold(ContractTestCase):
    ContractType = FreshnessThreshold

    def test_empty(self):
        empty = FreshnessThreshold(None, None)
        self.assert_symmetric(empty, {})
        self.assertEqual(empty.status(float('Inf')), FreshnessStatus.Pass)
        self.assertEqual(empty.status(0), FreshnessStatus.Pass)

    def test_both(self):
        threshold = FreshnessThreshold(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        dct = {
            'error_after': {'count': 2, 'period': 'day'},
            'warn_after': {'count': 18, 'period': 'hour'}
        }
        self.assert_symmetric(threshold, dct)

        error_seconds = timedelta(days=3).total_seconds()
        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(error_seconds), FreshnessStatus.Error)
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)

    def test_merged(self):
        t1 = FreshnessThreshold(
            warn_after=Time(count=36, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        t2 = FreshnessThreshold(
            warn_after=Time(count=18, period=TimePeriod.hour),
        )
        threshold = FreshnessThreshold(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        self.assertEqual(threshold, t1.merged(t2))

        error_seconds = timedelta(days=3).total_seconds()
        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(error_seconds), FreshnessStatus.Error)
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)


class TestQuoting(ContractTestCase):
    ContractType = Quoting

    def test_empty(self):
        empty = Quoting()
        self.assert_symmetric(empty, {})

    def test_partial(self):
        a = Quoting(None, True, False)
        b = Quoting(True, False, None)
        self.assert_symmetric(a, {'schema': True, 'identifier': False})
        self.assert_symmetric(b, {'database': True, 'schema': False})

        c = a.merged(b)
        self.assertEqual(c, Quoting(True, False, False))
        self.assert_symmetric(
            c, {'database': True, 'schema': False, 'identifier': False}
        )


class TestUnparsedSourceDefinition(ContractTestCase):
    ContractType = UnparsedSourceDefinition

    def test_defaults(self):
        minimum = UnparsedSourceDefinition(name='foo')
        self.assert_from_dict(minimum, {'name': 'foo'})
        self.assert_to_dict(minimum, {'name': 'foo', 'description': '', 'quoting': {}, 'freshness': {}, 'tables': [], 'loader': ''})

    def test_contents(self):
        empty = UnparsedSourceDefinition(
            name='foo',
            description='a description',
            quoting=Quoting(database=False),
            loader='some_loader',
            freshness=FreshnessThreshold(),
            tables=[],
        )
        dct = {
            'name': 'foo',
            'description': 'a description',
            'quoting': {'database': False},
            'loader': 'some_loader',
            'freshness': {},
            'tables': [],
        }
        self.assert_symmetric(empty, dct)

    def test_table_defaults(self):
        table_1 = UnparsedSourceTableDefinition(name='table1')
        table_2 = UnparsedSourceTableDefinition(
            name='table2',
            description='table 2',
            quoting=Quoting(database=True),
        )
        source = UnparsedSourceDefinition(
            name='foo',
            tables=[table_1, table_2]
        )
        from_dict = {
            'name': 'foo',
            'tables': [
                {'name': 'table1'},
                {
                    'name': 'table2',
                    'description': 'table 2',
                    'quoting': {'database': True},
                },
            ],
        }
        to_dict = {
            'name': 'foo',
            'description': '',
            'loader': '',
            'quoting': {},
            'freshness': {},
            'tables': [
                {
                    'name': 'table1',
                    'description': '',
                    'tests': [],
                    'columns': [],
                    'quoting': {},
                    'freshness': {},
                },
                {
                    'name': 'table2',
                    'description': 'table 2',
                    'tests': [],
                    'columns': [],
                    'quoting': {'database': True},
                    'freshness': {},
                },
            ],
        }
        self.assert_from_dict(source, from_dict)
        self.assert_symmetric(source, to_dict)


class TestUnparsedDocumentationFile(ContractTestCase):
    ContractType = UnparsedDocumentationFile

    def test_ok(self):
        doc = UnparsedDocumentationFile(
            package_name='test',
            root_path='/root',
            path='/root/docs',
            original_file_path='/root/docs/doc.md',
            file_contents='blah blah blah',
        )
        doc_dict = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/docs',
            'original_file_path': '/root/docs/doc.md',
            'file_contents': 'blah blah blah',
        }
        self.assert_symmetric(doc, doc_dict)
        self.assertEqual(doc.resource_type, NodeType.Documentation)
        self.assert_fails_validation(doc_dict, UnparsedNode)

    def test_extra_field(self):
        self.assert_fails_validation({})
        doc_dict = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/docs',
            'original_file_path': '/root/docs/doc.md',
            'file_contents': 'blah blah blah',
            'resource_type': 'docs',
        }
        self.assert_fails_validation(doc_dict)


class TestUnparsedNodeUpdate(ContractTestCase):
    ContractType = UnparsedNodeUpdate

    def test_defaults(self):
        minimum = UnparsedNodeUpdate(name='foo')
        from_dict = {'name': 'foo'}
        to_dict = {'name': 'foo', 'columns': [], 'description': '', 'tests': []}
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        update = UnparsedNodeUpdate(
            name='foo',
            description='a description',
            tests=['table_test'],
            columns=[
                NamedTested(name='x', description='x description'),
                NamedTested(
                    name='y',
                    description='y description',
                    tests=[
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ]
                ),
            ],
        )
        dct = {
            'name': 'foo',
            'description': 'a description',
            'tests': ['table_test'],
            'columns': [
                {'name': 'x', 'description': 'x description', 'tests': []},
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                },
            ],
        }
        self.assert_symmetric(update, dct)

    def test_bad_test_type(self):
        dct = {
            'name': 'foo',
            'description': 'a description',
            'tests': ['table_test'],
            'columns': [
                {'name': 'x', 'description': 'x description', 'tests': []},
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        100,
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                },
            ],
        }
        self.assert_fails_validation(dct)

        dct = {
            'name': 'foo',
            'description': 'a description',
            'tests': ['table_test'],
            'columns': [
                # column missing a name
                {'description': 'x description', 'tests': []},
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                },
            ],
        }
        self.assert_fails_validation(dct)

        # missing a name
        dct = {
            'description': 'a description',
            'tests': ['table_test'],
            'columns': [
                {'name': 'x', 'description': 'x description', 'tests': []},
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                },
            ],
        }
        self.assert_fails_validation(dct)

