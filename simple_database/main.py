# -*- coding: utf-8 -*-
import os
import json
from datetime import date
import yaml  # PyYaml module, avoids unicode conversion on json load

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH
import copy
import string

# Commit changes immediately on making them?
AUTOCOMMIT = True

# Primary database access functions
def create_database(db_name):
    '''
    Creates and returns a database as long as it does not already exist
    '''
    # make sure a db with that name doesn't exist
    if os.path.isfile(os.path.join(BASE_DB_FILE_PATH, db_name)):
        msg = 'Database with name "{}" already exists.'
        raise ValidationError(msg.format(db_name))
    
    # return db object
    return Database(db_name)
    
def connect_database(db_name):
    '''
    Connects to an existing database from a file and returns a database object
    '''
    # check if files exists, read it in
    if not os.path.isfile(os.path.join(BASE_DB_FILE_PATH, db_name)):
        raise ValidationError('Database does not exist.')
    
    with open(os.path.join(BASE_DB_FILE_PATH, db_name),'r') as f:
        # Database object is stored as a list of Table dicts
        # [{}, {}, {}]
        # where each Table dict has these key, value pairs:
        # name: table name
        # columns: list of dicts (paramaters needed for create_table)
        # rows: list of rows where each row is parameters for Table.insert
        data = yaml.safe_load(f) # ensures strings wont be read in as unicode
        
    db = Database(db_name)
    for table in data:
        db.create_table(table['name'], table['columns'])
        db_table = getattr(db, table['name'])
        
        # Deserialize datetime.date objects, converting from string -> date
        db_table.rows = table['rows']            
        db_table.rows = db_table.row_handler(_deserialize_dt)
        
    return db

# Datetime.date <-> string conversion functions for json serialization

def _serialize_dt(dt_obj):
    '''
    Converts a datetime.date object to a string to be stored in the json file
    '''
    return '{0.year}-{0.month:{1}}-{0.day:{1}}'.format(dt_obj, '02')

def _deserialize_dt(value):
    '''
    Converts a string extracted from a json database storage file to a 
    datetime.date object to be used in a Database object
    '''
    year = int(value[:4])
    month = int(value[5:7])
    day = int(value[8:])
    return date(year, month, day)

class Database(object):
    def __init__(self, db_name):
        self._tables = [] # list of Table objects
        self._db_name = db_name
        
    def __str__(self):
        return "<Database object: '{}'".format(self._db_name)
    
    def create_table(self, table_name, columns):
        '''
        Reads in name of table and a list of columns where each column is a 
        dict with {'name': x, 'type: y}
        '''
        # check if there is already a table with that name
        if hasattr(self, table_name):
            raise ValidationError('Duplicate table name')
        
        # Make sure "table_name" is a valid name and doesn't start with underscore
        if table_name[0].lower() not in string.ascii_lowercase:
            raise ValidationError('Table name must begin with a character')
        
        new_table = Table(self, table_name, columns)
        setattr(self, table_name, new_table) # for example, db.authors
        self._tables.append(new_table)
        
        # Commit changes to file
        if AUTOCOMMIT:
            self.commit()
    
    def show_tables(self):
        '''
        Prints a list of table names in the database
        '''
        return [table.name for table in self._tables]
       
    def commit(self):
        '''
        Writes the contents of the Database to a file in json format 
        '''
        json_data = []
        for table in self._tables:
            # We are converting date objects into strings when writing to file
            # so use deepcopy to avoid changing the actual db.Table
            rows = copy.deepcopy(table.rows)
            
            # temp_dict will hold all the data for the Table we are writing
            temp_dict = {}
            temp_dict['name'] = table.name
            temp_dict['columns'] = table.columns
            
            # Serialize datetime.date objects, convert from date -> string
            temp_dict['rows'] = table.row_handler(_serialize_dt)
            
            json_data.append(temp_dict)
        
        # Create the directory if it does not already exist
        if not os.path.exists(BASE_DB_FILE_PATH):
            os.mkdir(BASE_DB_FILE_PATH)
            
        # dump the json data to the file
        with open(os.path.join(BASE_DB_FILE_PATH, self._db_name),'w') as f:
            json.dump(json_data, f)

class Table(object):
    def __init__(self, parent, name, columns):
        '''
        parent is the database that contains the table
        columns is a list of dictionaries [{'name': x, 'type': y}, ...]
        '''
        self.parent = parent # used by insert to call commit on the db instance
        self.name = name
        self.columns = columns
        self.rows = [] # each row will be stored as a list in this list
        self.col_names = [col['name'] for col in self.columns]
        
    def __str__(self):
        # first print the col_names as a header line
        s = '\t'.join(i for i in self.col_names)
        s += '\n'    

        # get the values for each row
        for row in self.rows:
            row_s = '\t'.join(str(i) for i in row)
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
            'date': date,
            'int': int,
            'str': str,
            'bool': bool,
        }
        # type_dict.update(__builtins__) 
        # builtins populates the rest of the dict with 'str':str, 'int':int, etc
        # but this won't work when running main.py directly
        
        row_list = [] # The new entry will be stored as a list
        # Read in the args - they should be in the same order as self.columns
        # Compare each arg to what type it should be
        for index, arg in enumerate(args):
            col_name = self.columns[index]['name'] # for example: name, id, date, etc
            col_type = self.columns[index]['type'] # int, bool, str, etc
            if not isinstance(arg, type_dict[col_type]):
                msg = 'Invalid type of field "{}": Given "{}", expected "{}"'
                msg = msg.format(col_name, type(arg).__name__, col_type)
                raise ValidationError(msg)
            else:
                # append the argument to our row list
                row_list.append(arg)
        
        # Since all type checks passed, insert the row in the table
        self.rows.append(row_list)
        
        # Commit the changes
        if AUTOCOMMIT:
            self.parent.commit()
    
    def describe(self):
        '''
        Return the column configuration of the table
        '''
        return self.columns
    
    def query(self, **kwargs):
        '''
        Generator that yields rows to iterate over
        that match all the keywords and their values passed to the method.
        '''
        # Validate the keyword arguments passed to the query
        for key in kwargs.keys():
            if key not in self.col_names:
                msg = 'column "{}" not defined in table "{}".'
                raise ValueError(msg.format(key, self.name))
        
        # Iterate through the rows, and on each row check to see if the row
        #  values match the values passed for the respective keyword argument.
        #  If they all match, yield that row.
        for row in self.rows:
            matches = True
            for key, value in kwargs.items():
                if row[self.col_names.index(key)] != value:
                    matches = False
                # we could add a check for missing data in the row
            if matches:
                yield Row(self.col_names, row)
    
    def all(self):
        '''
        Return the whole list of rows in the table without filtering.
        '''
        return self.query()
        
    def row_handler(self, conversion_function):
        '''
        Checks for any columns that are of type date
        Then applies the conversion_function to each row value at this column
        The conversion will either go date -> string, or string -> date
        It will then return the rows
        '''
        new_table = copy.deepcopy(self)
        for pos, column in enumerate(new_table.columns):
            if column['type'] == 'date':
                for row in new_table.rows:
                    row[pos] = conversion_function(row[pos])

        return new_table.rows
    

class Row(object):
    '''
    Will be used in the Table.query() and Table.all()
    '''
    def __init__(self, col_names, row):
        for i, column in enumerate(col_names):
            setattr(self, column, row[i])
