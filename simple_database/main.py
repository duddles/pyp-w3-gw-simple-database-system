import os
import json
from datetime import date
from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH
import copy

# Commit changes immediately on making them?
AUTOCOMMIT = True
_dt_format = '{0.year}-{0.month:{1}}-{0.day:{1}}'

def create_database(db_name):
    # make sure a db with that name doesn't exist
    print 'Creating a database'
    if os.path.isfile(db_name):
        raise ValidationError('Database with name "{}" already exists.'.format(db_name))
    
    # return db object
    return Database(db_name)
    
def connect_database(db_name):
    # check if files exists, read it in
    if not os.path.isfile(db_name):
        raise ValidationError('Database does not exist.'.format(db_name))
    
    with open(db_name,'r') as f:
        # [{}, {}, {}]
        # where each dict is a table and has these key, value pairs:
        # name: table name
        # columns: list of dicts that is paramater for create table
        # rows: list of rows where each row is parameters for Table.insert
        data = json.loads(f)
        
        db = Database(db_name)
        for table in data:
            db.create_table(table['name'], table['columns'])
            temp_table = getattr(db, table['name'])
            for row in table['rows']:
                
                # Convert datetime.date objects to :
                for i, value in enumerate(row):
                    if table['cloumns'][i]['type'] == 'date':
                        row[i] = _deserialize_dt(value)
                
                # Then insert the row
                temp_table.insert(row)
        
        return db
        
def _serialize_dt(dt_obj): # date -> string
    return _dt_format.format(dt_obj, '02')

def _deserialize_dt(value): # string -> datetime
    year = int(value[:4])
    month = int(value[5:7])
    day = int(value[8:])
    return date(year, month, day)
    
class Database(object):
    def __init__(self, db_name):
        self._tables = []
        self._db_name = db_name
        
    def __str__(self):
        return '["{}"]'.format('", "'.join(table.name for table in self._tables))
    
    def create_table(self, table_name, columns):
        '''
        Reads in name of table and a list of columns where each column is a 
        dict with {'name': x, 'type: y}
        '''

        # check if there is already a table with that name
        if hasattr(self, table_name):
            raise ValidationError('Duplicate table name')
        
        # Make sure "table_name" is a valid name
        #_validate_name(table_name)
        
        new_table = Table(self, table_name, columns)
        setattr(self, table_name, new_table)
        self._tables.append(new_table)
        
        # Commit changes
        if AUTOCOMMIT:
            self.commit()
    
    def show_tables(self):
       return str(self)
       
    def commit(self):
        # write to a file in json format 
        
        json_data = []
        for table in self._tables:
            rows = copy.deepcopy(table.rows)
            
            temp_dict = {}
            temp_dict['name'] = table.name
            temp_dict['columns'] = table.columns
            temp_dict['rows'] = []
            
            # Serialize datetime.date objects:
            for column in temp_dict['columns']:
                if column['type'] == 'date':
                    for row in rows:
                        row[column['name']] = _serialize_dt(row[column['name']])
            
            # Append the rows to the temporary dict
            for row in rows:
                temp_dict['rows'].append(row.values())
            
            json_data.append(temp_dict)
            
        with open(self._db_name,'a') as f:
            json.dump(json_data, f)

class Table(object):
    def __init__(self, parent, name, columns):
        '''
        Columns is a list of dictionaries [{'name': x, 'type': y}, ...]
        '''
        self.parent = parent # used by insert to call commit on the db instance
        self.name = name
        self.columns = columns
        self.rows = [] # each row will be stored as a dict in this list
        self.col_names = [col['name'] for col in self.columns] # id, name, birth, etc
        
    def __str__(self):
        # first print the col_names as a header line
        s = '\t'.join(col['name'] for col in self.columns)
        s += '\n'    

        # get the values for each row
        for row in self.rows:
            row_s = '\t'.join(str(row[col_name]) for col_name in self.col_names)
            s += row_s + '\n'
        return s

        
    def count(self):
        '''
        Return the count of rows in the table.
        '''
        return len(self.rows)
    
    def insert(self, *args):
        ''' 
        Insert a row of data in the table.  Validate number of fields and data
        type of each field before performing the insert.
        '''
        # First validate number of fields for the new entry
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of field')
        
        # use this to convert types that are in string format into types
        type_dict = {
            'int':int,
            'str':str,
            'date':date,
            'bool':bool
        }
        
        row_dict = {} # The new entry will be stored as a dict
        # Read in the args - they should be in the same order as self.columns
        # Compare each arg to what type it should be
        for index, arg in enumerate(args):
            col_name = self.columns[index]['name'] # for example: name, id, date, etc
            col_type = self.columns[index]['type'] # int, bool, str, etc
            if not isinstance(arg, type_dict[col_type]):
                error_msg = 'Invalid type of field "{}": Given "{}", expected "{}"'.format(arg, type(arg), col_type)
                raise ValidationError(error_msg)
            else:
                # add the key, value to our row dictionary
                row_dict[col_name] = arg
        self.rows.append(row_dict)
        
        # Commit the changes
        if AUTOCOMMIT:
            self.parent.commit()
    
    def describe(self):
        '''
        Return the column configuration of the table.
        '''
        return self.columns
        # s = '['
        # for row in self.columns:
        #     s += '\t' + str(row)
        # s += ']'
        # return s
    
    def query(self, **kwargs):
        '''
        Return an iterator, with each element that the
        iterator returns containing values that match a row in the table
        for which the query arguments match the query values given.
        '''
        # Validate the keyword arguments passed to the query
        for key in kwargs.keys():
            if key not in self.col_names:
                msg = 'column "{}" not defined in table "{}".'
                raise ValueError(msg.format(key, self.name))
        
        # If no arguments were passed to the query, yield every row
        if not kwargs:
            for row in self.rows:
                yield Row(row)
        
        # Iterate through the rows, and on each row check to see if the row
        #  values match the values passed for the respective keyword argument.
        #  If they all match, yield that row.
        else:
            for row in self.rows:
                for key, value in kwargs.iteritems():
                    if row[key] == value:
                        yield Row(row)
    
    def all(self):
        '''
        Return the whole list of rows in the table without filtering.
        '''
        return self.query()
    

class Row(object):
    '''
    Will be used in the Table.query() and Table.all()
    '''
    def __init__(self, row):
        self.__dict__.update(row)

# Example row:: {'name': 'Jorge Borges', 'id': 1, 'birth_date': datetime(1945, 12, 1), ...}
if (__name__ == '__main__'):
    
    db = create_database('library')
    
    db.create_table('authors', columns=[
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'str'},
        {'name': 'birth_date', 'type': 'date'},
        {'name': 'nationality', 'type': 'str'},
        {'name': 'alive', 'type': 'bool'},
    ])
    db.authors.insert(1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False)
    db2 = create_database('library')
    # print db.authors
    # print("db.authors = ")
    # print db.authors
    #print(db.authors.rows[0]['name'])
    #print(str(db))
    # q = db.authors.query(name='Jorge Luis Borges')
    # #print db.authors
    # for i, item in enumerate(q):
    #     print i
    #     print item.birth_date