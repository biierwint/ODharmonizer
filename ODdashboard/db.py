from sqlalchemy import create_engine
import pandas as pd

def get_engine(user, password, host, port, db, schema=None):
    """Return SQLAlchemy engine for OMOP DB, optionally with schema search_path."""
    if schema:
        return create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db}",
            connect_args={"options": f"-csearch_path={schema}"}
        )
    else:
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def run_query(engine, sql):
    """Run SQL and return DataFrame."""
    return pd.read_sql(sql, engine.connect())

