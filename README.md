# [pyp-w2] Simple Database System

You'll need to build a simple database system using files.
The fact that your database is using files underneath should be **COMPLETELY** hidden to your user.
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

To display the whole list of tables in a database, use the `show_tables` function:

```python
>>> db.show_tables()
["authors"]
```

## Tables

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

After the table is created, a new dynamic database attribute with the name of the table will be available. In the example above, when we created the table `authors`, we should see the following attribute: `db.authors`. That attribute will be the main accessor for table-level operations. That means, any time we want to interact with the _authors_ table, we'll have to do it through `db.authors`.

There are a few operations we can do in a table-level fashion:

### Table count

We should be able to get the count of rows in the table, using the `count()` method of that table:

```python
>>> db.authors.count()
0
```

### Inserting data

We need to use the `insert` table method to insert data in the table. You have to make sure the column order and data types are respected:

```python
>>> db.authors.insert(1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False)
```

While inserting data, errors might occur:

```python
>>> db.authors.insert(1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False, 'something-else')
ValidationError: Invalid amount of fields.

>>> db.authors.insert(1, 'Jorge Luis Borges', "1899-8-24", 'ARG', False) # must be a date object
ValidationError: 'Invalid type of field "birth_date": Given "str", expected "date"'
```

### Getting information from a table

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

### Querying

After inserting data in your tables, you probably want to query it. To do so we use the `query` table method. The result must be an iterator:

```python
>>> arg_authors = self.db.authors.query(nationality='ARG')
>>> for author in arg_authors:
        print(author.name)
"Jorge Luis Borges"
"Julio Cortázar"
```

Note that each element returned by the iterator must be a custom object, with dynamic attributes containing values per each column in the row (ie: "id", "name", "birth_date", etc)

To fetch the whole list of rows in the table without any filtering, use the `all` table method:

```python
>>> gen = self.db.authors.all()
```

The result returned by `all` is also an iterator, following the same specifications as the `query` function.

## Extra points

There's a huge room for improvement in this specification. You can take a lot of learning from this example, after all, you're implementing a real database system. Some functionalities missing:
* Sorting
* indexing
* Default, unique, required values
* Foreign keys
