import pytest
import pandas as pd


def query_function(bq_client):
    row_iter = bq_client.query("SELECT * FROM table").result()
    return row_iter


def load_table_function(bq_client, df):
    load_job = bq_client.load_table_from_dataframe(df, "a_table")
    print(load_job.state)
    return load_job.done()


@pytest.mark.bq_query_return_data(
    [
        {
            "query": "SELECT * FROM table",
            "table": {
                "columns": [
                    "id_row",
                    "name",
                ],
                "rows": [
                    [1, "Alice"],
                    [2, "Pete"],
                    [3, "Steven"],
                ],
            },
        },
    ]
)
def test_query_function(bq_client_mock):
    row_iter = query_function(bq_client_mock)

    expected_row_dicts = [
        {"id_row": 1, "name": "Alice"},
        {"id_row": 2, "name": "Pete"},
        {"id_row": 3, "name": "Steven"},
    ]
    for row, expected_row in zip(row_iter, expected_row_dicts):
        assert dict(row) == expected_row


@pytest.mark.bq_load_table_state(done_state=True, exist_state=True, running_state=False)
def test_load_table_function(bq_client_mock):

    mock_df_row_dicts = [
        {"id_row": 1, "name": "Alice"},
        {"id_row": 2, "name": "Pete"},
        {"id_row": 3, "name": "Steven"},
    ]

    df = pd.DataFrame(mock_df_row_dicts)

    done = load_table_function(bq_client_mock, df)
    assert done
