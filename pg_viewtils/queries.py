import os
from os import path
from click import secho
from sqlalchemy.exc import ProgrammingError, IntegrityError

def query_to_dataframe(db, filename_or_query, **kwargs):
    """
    Run a query on a SQL database (represented by
    a SQLAlchemy database object) and turn it into a
    `Pandas` dataframe.
    """
    from pandas import read_sql

    if "SELECT" in str(filename_or_query):
        # We are working with a query string instead of
        # an SQL file.
        sql = filename_or_query
    else:
        with open(filename_or_query) as f:
            sql = f.read()

    return read_sql(sql,db,**kwargs)


def pretty_print(sql, **kwargs):
    for line in sql.split("\n"):
        for i in ["SELECT", "INSERT","UPDATE","CREATE", "DROP","DELETE"]:
            if not line.startswith(i):
                continue
            start = line.split("(")[0].strip()
            secho(start, **kwargs)
            return


def run_sql_file(session, sql_file):
    sql = open(sql_file).read()
    queries = sql.split(';')
    for q in queries:
        sql = q.strip()
        if sql == '':
            continue
        try:
            session.execute(sql)
            pretty_print(sql, dim=True)
        except (ProgrammingError,IntegrityError) as err:
            pretty_print(sql, fg='red')
            secho(str(err.orig), fg='red', dim=True)
    session.commit()
    session.close()