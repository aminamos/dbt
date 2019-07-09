from dbt.clients.system import write_json

from hologram import FieldEncoder

import dataclasses
import json


class Replaceable:
    def replace(self, **kwargs):
        return dataclasses.replace(self, **kwargs)


class Mergeable(Replaceable):
    def merged(self, *args):
        """Perform a shallow merge, where the last non-None write wins. This is
        intended to merge dataclasses that are a collection of optional values.
        """
        replacements = {}
        cls = type(self)
        for field in dataclasses.fields(cls):
            for arg in args:
                value = getattr(arg, field.name)
                if value is not None:
                    replacements[field.name] = value

        return self.replace(**replacements)


class Writable:
    def write(self, path: str, omit_none: bool = True):
        write_json(path, self.to_dict(omit_none=omit_none))


class AnyJson(FieldEncoder):
    def __init__(self, encoder=None, decoder=None):
        self.encoder = encoder
        self.decoder = decoder

    def to_wire(self, value):
        as_str = json.dumps(value, cls=self.encoder)
        return json.loads(as_str)

    def to_python(self, value):
        as_str = json.dumps(value)
        return json.loads(as_str, cls=self.decoder)

    @property
    def json_schema(self):
        return {
            'type': ['object'],
            'additionalProperties': True,
        }
