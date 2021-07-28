import pandas as pd
import pytest
from google.cloud.bigquery.table import Row


class FakeRowIterator:
    def __init__(self, rows):
        self._rows = rows
        self.total_rows = len(rows)

    def __iter__(self):
        for row in self._rows:
            yield row

    def to_dataframe(self):
        df = pd.DataFrame(columns=list(self._rows[0].keys()))
        for i, row in enumerate(self._rows):
            df.loc[i] = row.values()

        return df

    def result(self):
        return self


@pytest.fixture
def bq_client_mock(request, mocker):

    marker = request.node.get_closest_marker("bq_query_return_data")
    if marker is None:
        data = None
    else:
        data = marker.args[0]

    if data:

        def mock_client_query(query, *args, **kwargs):
            for qry_data in data:
                if query == qry_data.get("query"):
                    columns = {k: v for v, k in enumerate(qry_data["table"]["columns"])}
                    rows = [
                        Row(row_data, columns) for row_data in qry_data["table"]["rows"]
                    ]
                    row_iter = FakeRowIterator(rows)
                    return row_iter

        mock_bq_client = mocker.patch("google.cloud.bigquery.client.Client")
        mock_bq_client.query = mock_client_query
        return mock_bq_client
    else:
        return mocker.MagicMock()
