from typing import Tuple

from transplant import (
    Context,
    Table,
    SQL,
    Columns,
    Rows,
    SQLSelectHandler,
    SQLInsertHandler,
    TransplantError,
)

Where = str
Fields = Columns


def where(where_: Where) -> SQLSelectHandler:
    def _select_handler(context: Context, table: Table) -> SQL:
        return f"select * from {table} where {where_}"

    return _select_handler


def where_in(from_table: Table, columns: Columns, in_column: str) -> SQLSelectHandler:
    def _select_handler(context: Context, table: Table) -> SQL:
        if from_table in context["table_data"]:
            table_items = set()
            rows = context["table_data"][from_table]["rows"]
            for row in rows:
                for column in columns:
                    if row[column] is not None:
                        table_items.add(row[column])
            return f"select * from {table} where {in_column} in {tuple(table_items)}"
        else:
            raise TransplantError(f"{from_table} isn't available")

    return _select_handler


def null_fields(fields: Fields) -> SQLInsertHandler:
    def _pre_insert_handler(
        context: Context, table: Table, columns: Columns, rows: Rows
    ) -> Tuple[Columns, Rows]:
        for row in rows:
            for field in fields:
                row[field] = None
        return columns, rows

    return _pre_insert_handler
