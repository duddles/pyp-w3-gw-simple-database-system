# [pyp-w2] Simple Database System

You'll need to build a simple database system using files.
The fact that your database is using files underneath should be COMPLETELY hidden to your user.
From the outside (public interface), the user must play with your database library as any other
database client, without knowing how it works internally.

To start using a database, you can either create a new database:

```python
>>> db = create_database('library')
```

or connect to an existing one:

```python
>>> db = connect_database('library')  # library DB already exists
```

Once you have your database connection, you must be able to create tables in the database.
While creating a table, you must provide the columns configuration with proper names and data types for each of them.

```python
>>> db.create_table('authors', columns=[
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'str'},
    {'name': 'birth_date', 'type': 'date'},
    {'name': 'nationality', 'type': 'str'},
    {'name': 'alive', 'type': 'bool'},
])
```

After the table creation, a new dynamic database attribute with the name of the table will be available: `db.authors`. This attribute will allow us perform any operations at a table level.
For example, to count the amount of rows in the table, we can use the `count` function:

```python
>>> db.authors.count()
0
```

To display the whole list of tables in a database, use the `show_tables` function:

```python
>>> db.show_tables()
["authors"]
```

We are now ready to start feeding the table with data. To do that, use the `insert` table function (make sure to respect the column order and data types):

```python
>>> db.authors.insert(1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False)
```

While inserting data, errors might occur:

```python
>>> db.authors.insert(1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False, 'something-else')
ValidationError: Invalid amount of fields.

>>> db.authors.insert(1, 'Jorge Luis Borges', "1899-8-24", 'ARG', False)  # must be a date object
ValidationError: 'Invalid type of field "birth_date": Given "str", expected "date"'
```

If you need to see the columns configuration of certain table, you can execute the `describe` function:

```python
>>> db.authors.describe()
[
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'str'},
    {'name': 'birth_date', 'type': 'date'},
    {'name': 'nationality', 'type': 'str'},
    {'name': 'alive', 'type': 'bool'},
]
```

After inserting data in your tables, you probably want to query it. For that, use the `query` table function. The result must be a generator, which should allow the use to loop through all rows that matched given query:

```python
>>> gen = self.db.authors.query(nationality='ARG')
>>> for author in gen:
        print(author.name)
"Jorge Luis Borges"
"Julio Cortázar"
```

Note that each element yielded by the generator must be a custom object, with dynamic attributes containing each of the row columns (ie: "id", "name", "birth_date", etc)

To fetch the whole list of rows in the table without any filtering, use the `all` table function:

```python
>>> gen = self.db.authors.all()
```

The result returned by `all` is also a generator, following the same specifications as the `query` function.

Please feel free to implement any extra functionalities around this database client (ie: sorting, indexing, default values, unique values, required values, foreign keys to other tables, etc)
