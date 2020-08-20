import os
import logging
import psycopg2  # type: ignore
import pickle

from typing import Optional, Tuple, List, Callable, Union, Dict
from typing_extensions import TypedDict

from psycopg2.extras import Json  # type: ignore
from psycopg2.extensions import register_adapter, cursor  # type: ignore

register_adapter(dict, Json)

loglevel = os.environ.get("LOG_LEVEL", "info").upper()
logging.basicConfig(level=getattr(logging, loglevel))

logger = logging.getLogger(__name__)

TP_DIR = "./.transplant"
TP_FROM_URI = os.environ.get("TRANSPLANT_FROM_URI")
TP_TO_URI = os.environ.get("TRANSPLANT_TO_URI")

Table = str
SQL = str
Columns = List[str]
Rows = List[dict]


class TableData(TypedDict, total=False):
    table: Table
    sql: SQL
    from_cache: bool
    columns: Columns
    rows: Rows


class Context(TypedDict):
    table_data: dict


SQLSelectHandler = Callable[[Context, Table], SQL]
SQLInsertHandler = Callable[[Context, Table, Columns, Rows], Tuple[Columns, Rows]]


class _TableSpec(TypedDict):
    table: Table


class TableSpec(_TableSpec, total=False):
    select_handler: Union[str, SQLSelectHandler]
    pre_insert_handler: SQLInsertHandler


class TransplantError(Exception):
    pass


def _get_table_data(table: str, sql: str, cur: cursor, ignore_cache=False) -> TableData:
    data: TableData = {"sql": sql, "table": table, "from_cache": False}
    cache_path = f"{TP_DIR}/{table}.pickle"
    if not ignore_cache and os.path.exists(cache_path):
        data = pickle.load(open(cache_path, "rb"))
        data["from_cache"] = True
        logger.info(f"found cache for {table}")
        return data
    cur.execute(sql)
    data["columns"] = [col.name for col in cur.description]
    data["rows"] = [dict(zip(data["columns"], row)) for row in cur.fetchall()]
    with open(cache_path, "wb") as out:
        pickle.dump(data, out)
    return data


def _get_all_table_data(
    tables: List[TableSpec],
    from_uri: Optional[
        str
    ],  # not optional, but mypy can't detect the logic that prevents this,
    ignore_cache: bool = False,
) -> Context:
    con = psycopg2.connect(from_uri)
    cur = con.cursor()
    context: Context = {"table_data": {}}
    try:
        for table_spec in tables:
            table = table_spec["table"]
            if "select_handler" in table_spec:
                if isinstance(table_spec["select_handler"], str):
                    sql = table_spec["select_handler"]
                else:
                    sql = table_spec["select_handler"](context, table_spec["table"])
            else:
                sql = f"select * from {table_spec['table']}"
            logger.info(f"pulling rows from {table} ...")
            data = _get_table_data(table_spec["table"], sql, cur, ignore_cache)
            if "id" in data["rows"]:
                data["rows"].sort(key=lambda row: row["id"])
            row_count = len(data["rows"])
            if row_count:
                logger.info(f"found {row_count} from {table}")
                context["table_data"][data["table"]] = data
            else:
                logger.warning(f"found {row_count} from{table}, skipping ...")
        return context
    finally:
        con.close()
        cur.close()


def _table_columns_rows_to_insert_sql(
    table: Table, columns: Columns, rows: List[list]
) -> SQL:
    columns_ = f"""({', '.join('"' + c + '"' for c in columns)})"""
    values_ = f"({', '.join(('%s',) * len(rows[0]))})"
    return f"insert into {table} {columns_} values {values_} on conflict do nothing;"


def _insert_table_data(table: Table, columns: Columns, rows: Rows, cur: cursor) -> None:
    rows_ = [[row[col] for col in columns] for row in rows]
    cur.executemany(_table_columns_rows_to_insert_sql(table, columns, rows_), rows_)


def _get_latest_id_from_table(table: Table, cur: cursor) -> int:
    cur.execute(f"select id from {table} order by id desc limit 1")
    row = cur.fetchone()
    return row[0] if row else None


def _insert_all_table_data(
    context: Context,
    to_uri: Optional[
        str
    ],  # not optional, but mypy can't detect the logic that prevents this
    handlers: Dict[str, SQLInsertHandler],
    insert_occupied: bool = False,
) -> None:
    con = psycopg2.connect(to_uri)
    cur = con.cursor()
    try:
        for table, data in context["table_data"].items():
            if table in handlers:
                handler = handlers[table]
                columns, rows = handler(context, table, data["columns"], data["rows"])
            else:
                columns, rows = data["columns"], data["rows"]
            if not insert_occupied and _get_latest_id_from_table(table, cur):
                logger.warning(f"table {table} has data, skipping ...")
            else:
                logger.info(f"inserting {len(rows)} into {table} ...")
                _insert_table_data(table, columns, rows, cur)
    except:
        con.close()
        cur.close()
        raise
    finally:
        con.commit()
        con.close()
        cur.close()


def transplant(
    tables: List[TableSpec],
    from_uri: Optional[str] = TP_FROM_URI,
    to_uri: Optional[str] = TP_TO_URI,
    ignore_cache: bool = False,
    insert_occupied: bool = False,
):
    if not all(uri is not None for uri in (from_uri, to_uri)):
        raise TransplantError(
            "transplant requires from_uri and to_uri to be argued or these "
            "ENV variables to be defined: "
            "TRANSPLANT_FROM_URI, TRANSPLANT_TO_URI"
        )
    elif not all(
        uri is not None and uri.startswith("postgres://") for uri in (from_uri, to_uri)
    ):
        raise TransplantError("transplant currently only supports postgres")
    else:
        if not os.path.exists(TP_DIR):
            os.mkdir(TP_DIR)
        context = _get_all_table_data(tables, from_uri, ignore_cache)
        handlers = {
            spec["table"]: spec["pre_insert_handler"]
            for spec in tables
            if "pre_insert_handler" in spec
        }
        _insert_all_table_data(context, to_uri, handlers, insert_occupied)
