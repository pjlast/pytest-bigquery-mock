from __future__ import annotations

import pandas as pd
import pytest
from google.cloud.bigquery.table import Row


job_counter = 0


class FakeRowIterator:
    def __init__(self, rows):
        self._rows = rows
        self.total_rows = len(rows)

    def __iter__(self):
        yield from self._rows

    def to_dataframe(self):
        df = pd.DataFrame(columns=list(self._rows[0].keys()))
        for i, row in enumerate(self._rows):
            df.loc[i] = row.values()

        return df


class FakeQuery:
    def __init__(self, row_iterator):
        self.row_iterator = row_iterator
        global job_counter
        self.job_id, job_counter = job_counter, job_counter + 1

    def add_done_callback(self, func):
        func(self)

    def result(self, *args, **kwargs):
        return self.row_iterator

    def to_dataframe(self):
        return self.result().to_dataframe()
        

class FakeLoadJob:
    def __init__(
        self,
        cancel_state=None,
        done_state=True,
        exist_state=True,
        running_state=False,
        errors=None,
        **kwargs,
    ):
        """Creates an instance of a fake LoadJob
        (see https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJob)
        with mock LoadJob states set by class attributes.

        Args:
            cancel_state (bool | None, optional): Mock job cancel state. Defaults to None.
            done_state (bool, optional): Mock job done state. Defaults to True.
            exist_state (bool, optional): Mock job exist state. Defaults to True.
            running_state (bool, optional): Mock job running state. Should be False if
                done_state is True. Defaults to False.
            errors (List[Exception], optional): List of mock errors raised by job.
                Defaults to None.
        """
        if done_state and running_state:
            raise ValueError("'done_state' must be False if 'running_state' is True.")
        self.cancel_state = cancel_state
        self.done_state = done_state
        self.exist_state = exist_state
        self.running_state = running_state
        self.errors = errors
        self.__dict__.update(kwargs)
        self.state = self._get_state()

    def _get_state(self):
        if self.done_state:
            return "DONE"
        elif self.running_state:
            return "RUNNING"
        else:
            return "FAILURE"

    def add_done_callback(self, func):
        func(self)

    def cancel(self):
        return self.cancel_state

    def cancelled(self):
        """Always returns False as per the original LoadJob
        """
        return False

    def done(self):
        return self.done_state

    def exception(self):
        return self.errors

    def exists(self):
        return self.exist_state

    def result(self):
        return FakeLoadJob()

    def running(self):
        return self.running_state


def patch_query(mock_bq_client, query_data):
    def mock_client_query(query, *args, **kwargs):
        for qry_data in query_data:
            if query == qry_data.get("query"):
                columns = {k: v for v, k in enumerate(qry_data["table"]["columns"])}
                rows = [
                    Row(row_data, columns) for row_data in qry_data["table"]["rows"]
                ]
                row_iter = FakeRowIterator(rows)
                query = FakeQuery(row_iter)
                return query

    mock_bq_client.query = mock_client_query

    return mock_bq_client


def patch_load_table(mock_bq_client, **kwargs):
    """Patches all load_table_from_<src> methods in the bigquery
    client.
    """
    fake_load_job = FakeLoadJob(**kwargs)

    def mock_load_table(src, destination, *args, **kwargs):
        return fake_load_job

    mock_bq_client.load_table_from_dataframe = mock_load_table
    mock_bq_client.load_table_from_json = mock_load_table
    mock_bq_client.load_table_from_file = mock_load_table
    mock_bq_client.load_table_from_uri = mock_load_table

    return mock_bq_client


@pytest.fixture
def bq_client_mock(request, mocker):

    mock_bq_client = mocker.patch("google.cloud.bigquery.client.Client")

    query_marker = request.node.get_closest_marker("bq_query_return_data")
    query_data = None if query_marker is None else query_marker.args[0]

    load_table_marker = request.node.get_closest_marker("bq_load_table_state")
    load_table_states = None if load_table_marker is None else load_table_marker.kwargs

    if query_data:
        mock_bq_client = patch_query(mock_bq_client, query_data)
    if load_table_states:
        mock_bq_client = patch_load_table(mock_bq_client, **load_table_states)

    return mock_bq_client
