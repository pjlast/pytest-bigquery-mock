# pytest-bigquery-mock

Pytest plugin that provides a `bq_client_mock` fixture.
This fixture mocks the `google.cloud.bigquery.Client` class and provides a way to mock an API response using `pytest.mark`, for example:

```
def function_that_calls_bigquery(bq_client):
    row_iter = bq_client.query("SELECT * FROM table").result()
    return row_iter


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
def test_function_that_calls_bigquery(bq_client_mock):
    row_iter = function_that_calls_bigquery(bq_client_mock)

    expected_row_dicts = [
        {"id_row": 1, "name": "Alice"},
        {"id_row": 2, "name": "Pete"},
        {"id_row": 3, "name": "Steven"},
    ]
    for row, expected_row in zip(row_iter, expected_row_dicts):
        assert dict(row) == expected_row

```
