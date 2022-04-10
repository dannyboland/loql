import pytest
from pytest_mock import MockerFixture

from clidb.database import DatabaseController
from clidb.events import Query


@pytest.mark.asyncio
async def test_database_init(mocker: MockerFixture) -> None:
    db = DatabaseController()
    mocker.patch.object(db, "log", autospec=True)
    spy = mocker.spy(db, "emit")
    await db.handle_query(Query(mocker.MagicMock(), "select 1"))

    emit_call = spy.call_args[0]
    result_obj = emit_call[0]
    table = result_obj.result.renderable
    assert table.columns[0]._cells[0] == "1"
