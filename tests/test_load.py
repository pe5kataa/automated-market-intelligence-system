from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from ingestion.stocks_load import _insert_on_conflict_nothing, load_stock_prices
from sql.create_raw_stocks_table import stock_prices

# What pandas hands the function: column names, then one tuple per row.
KEYS = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
ROWS = [
    ("AAPL", date(2026, 1, 2), 1.0, 2.0, 0.5, 1.5, 1000),
    ("MSFT", date(2026, 1, 2), 3.0, 4.0, 2.5, 3.5, 2000),
]


def build_sql():
    """Run the load against a fake connection and return the SQL it tried to send."""
    conn = MagicMock()
    conn.execute.return_value.rowcount = 2

    _insert_on_conflict_nothing(MagicMock(table=stock_prices), conn, KEYS, ROWS)

    statement = conn.execute.call_args[0][0]
    return str(statement.compile(compile_kwargs={"literal_binds": True}))


def test_insert_skips_duplicates_instead_of_failing():
    sql = build_sql()

    assert "INSERT INTO raw.raw_stock_prices" in sql
    assert "ON CONFLICT DO NOTHING" in sql


def test_row_values_land_under_the_right_columns():
    sql = build_sql()

    assert '("Ticker", "Date", "Open", "High", "Low", "Close", "Volume")' in sql
    assert "('AAPL', '2026-01-02', 1.0, 2.0, 0.5, 1.5, 1000)" in sql
    assert "('MSFT', '2026-01-02', 3.0, 4.0, 2.5, 3.5, 2000)" in sql


@patch("pandas.DataFrame.to_sql")
def test_load_appends_to_the_raw_table(mock_to_sql):
    """A wrong table, schema, or if_exists here would silently write to the wrong place."""
    load_stock_prices(pd.DataFrame({"Ticker": ["AAPL"]}))

    args, kwargs = mock_to_sql.call_args
    assert args[0] == "raw_stock_prices"
    assert kwargs["schema"] == "raw"
    assert kwargs["if_exists"] == "append"  # "replace" would DROP the table
    assert kwargs["index"] is False


@patch("pandas.DataFrame.to_sql")
def test_load_wires_up_the_conflict_handler(mock_to_sql):
    """Without this, pandas does a plain INSERT and every re-run dies on duplicates."""
    load_stock_prices(pd.DataFrame({"Ticker": ["AAPL"]}))

    assert mock_to_sql.call_args.kwargs["method"] is _insert_on_conflict_nothing
