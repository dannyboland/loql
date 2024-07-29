from clidb.app import Clidb


async def test_open_file_write(tmpdir):
    with tmpdir.as_cwd():
        app = Clidb()
        tmpdir.join("data.csv").write_text("a,b,c\n1,2,3\n4,5,6\n", encoding="utf-8")
        async with app.run_test() as pilot:
            await pilot.press("ctrl+o")  # open file
            await pilot.click('#file_path')
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("data.csv"))
            await pilot.press("enter")
            await pilot.click("#sql_input")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("select * from data"))
            await pilot.press("ctrl+s")  # save result
            assert tmpdir.join("results.csv").read_text(
                encoding="utf-8") == "a,b,c\n1,2,3\n4,5,6\n"


async def test_ctas_join(tmpdir):
    with tmpdir.as_cwd():
        app = Clidb()
        tmpdir.join("data1.csv").write_text(
            "id,value_a\n1,hello\n2,hi\n", encoding="utf-8")
        tmpdir.join("data2.csv").write_text(
            "id,value_b\n1,world\n2,there\n", encoding="utf-8")
        async with app.run_test() as pilot:
            await pilot.press("ctrl+o")  # open file
            await pilot.click('#file_path')
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("data1.csv"))
            await pilot.press("enter")
            await pilot.press("ctrl+o")  # open file
            await pilot.click('#file_path')
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("data2.csv"))
            await pilot.press("enter")
            await pilot.click("#sql_input")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list(
                """create table data3 as (
                    select * from data1 join data2 on data1.id = data2.id
                )"""
            ))
            await pilot.press("ctrl+r")  # execute query
            await pilot.click("#sql_input")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("select value_a,value_b from data3"))
            await pilot.press("ctrl+s")  # save result
            assert tmpdir.join("results.csv").read_text(
                encoding="utf-8") == "value_a,value_b\nhello,world\nhi,there\n"
