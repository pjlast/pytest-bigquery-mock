# pytest-bigquery-mock

Pytest plugin that provides a `bq_client_mock` fixture.
This fixture mocks the `google.cloud.bigquery.Client` class and provides a way to mock an API response using `pytest.mark`, for example:

## Install and use

Install the plugin with
```pip install pytest-bigquery-mock```

Then, in your `conftest.py` file add `pytest-bigquery-mock` to your list of plugins

```tests/conftest.py

plugins = ["pytest-bigquery-mock"]
```

This allows you to use the `bq_client_mock` fixture in your pytest tests.


## Example Usage:

### Mocking the client query functionality:

```python
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

### Mocking the load_table functionality

Supports the following load_table operations:
* `load_table_from_dataframe`
* `load_table_from_json`
* `load_table_from_file`
* `load_table_from_uri`

```python
def function_that_calls_bigquery(bq_client, df):
    load_job = bq_client.load_table_from_dataframe(df, "table_ref")
    print(load_job.state)
    return load_job.done()


# Pass the final states you want the mock load_table job to reach as an argument
@pytest.mark.bq_load_table_state(done_state=True, exist_state=True, running_state=False)
def test_function_that_calls_bigquery(bq_client_mock):

    mock_df_row_dicts = [
        {"id_row": 1, "name": "Alice"},
        {"id_row": 2, "name": "Pete"},
        {"id_row": 3, "name": "Steven"},
    ]

    mock_df = pd.DataFrame(mock_df_row_dicts)

    done = function_that_calls_bigquery(bq_client_mock, df)
    assert done
```
