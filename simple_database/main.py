import os
import json
from datetime import date

from simple_database.exceptions import ValidationError

BASE_DB_FILE_PATH = '/tmp/simple_database/'


class BaseRowModel(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        # initialize the table file as an empty JSON list
        if not os.path.exists(self.table_filepath):
            with open(self.table_filepath, 'w') as f:
                empty_table = {'columns': columns, 'rows': []}
                f.write(json.dumps(empty_table))

        self.columns = columns or self._read_columns()
        self.RowModel = type(self.name.title(), (BaseRowModel,), {})

    def _read_columns(self):
        with open(self.table_filepath, 'r') as f:
            return json.load(f)['columns']

    def insert(self, *args):
        valid, error = self._validate_row(*args)
        if not valid:
            raise ValidationError(error)
        row = self._serialize_row(*args)
        self._write_row_to_file(row)

    def query(self, **kwargs):
        with open(self.table_filepath, 'r') as f:
            for row in json.load(f)['rows']:
                if not all([row.get(key) == value for key, value in kwargs.items()]):
                    continue
                yield self.RowModel(row)

    def all(self):
        with open(self.table_filepath, 'r') as f:
            for row in json.load(f)['rows']:
                yield self.RowModel(row)

    def count(self):
        with open(self.table_filepath, 'r') as f:
            return len(json.load(f)['rows'])

    def describe(self):
        return self.columns

    def _write_row_to_file(self, row):
        with open(self.table_filepath, 'r+') as f:
            table_data = json.load(f)
            table_data['rows'].append(row)
            f.seek(0)
            f.write(json.dumps(table_data, indent=4))

    def _validate_row(self, *args):
        if len(args) != len(self.columns):
            return False, 'Invalid amount of fields.'
        for field, column_config in zip(args, self.columns):
            if not isinstance(field, eval(column_config['type'])):
                error_msg = ('Invalid type of field "{}": Given '
                             '"{}", expected "{}"'
                             ''.format(column_config['name'],
                                       type(field).__name__,
                                       eval(column_config['type']).__name__))
                return False, error_msg
        return True, None

    @staticmethod
    def _serialize_date(value):
        return value.isoformat()

    def _serialize_row(self, *args):
        row = {}
        for column_name, field in zip([c['name'] for c in self.columns], args):
            serializer_name = '_serialize_{}'.format(type(field).__name__)
            if hasattr(self, serializer_name):
                field = getattr(self, serializer_name)(field)
            row[column_name] = field
        return row


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        if os.path.exists(db_filepath):
            raise ValidationError('Database with name "{}" already exists.'
                                  ''.format(name))
        os.makedirs(db_filepath)

    def _read_tables(self):
        tables = [f.replace('.json', '') for f in os.listdir(self.db_filepath)]
        for table in tables:
            setattr(self, table, Table(db=self, name=table))
        return tables

    def create_table(self, table_name, columns):
        if table_name in self.tables:
            raise ValidationError('Table with name "{}" in DB "{}" '
                                  'already exist.'.format(table_name, self.name))
        table = Table(db=self, name=table_name, columns=columns)
        self.tables.append(table_name)
        setattr(self, table_name, table)

    def show_tables(self):
        return self.tables



def create_database(db_name):
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    return DataBase(name=db_name)
