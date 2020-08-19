# transplant

Library for selectively seeding a target database from another with the same table schemas.

## Basic Usage

```python
from transplant import transplant
from transplant.handlers import where, where_in

transplant(
    [
        {'table': 'one', 'select_handler': where('id > 100 and id < 200')},
        # gets rows from two that are represented in the one.two_id fk
        ('table': 'two', 'select_handler': where_in('one', ['two_id'], 'id'))
    ],
    from_uri='postgres://<user>:<password>@<host>:<port>/<database>',
    to_uri='postgres://<user>:<password>@<host>:<port>/<database>'
)
```