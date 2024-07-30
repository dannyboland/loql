from loql.app import LoQL


async def test_ctas_join(tmpdir):
    with tmpdir.as_cwd():
        app = LoQL()
        tmpdir.join("data1.csv").write_text(
            "id,value_a\n1,hello\n2,hi\n", encoding="utf-8"
        )
        tmpdir.join("data2.csv").write_text(
            "id,value_b\n1,world\n2,there\n", encoding="utf-8"
        )
        async with app.run_test() as pilot:
            await pilot.press("ctrl+o")  # open file
            await pilot.click("#file_path")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("data1.csv"))
            await pilot.press("enter")
            await pilot.press("ctrl+o")  # open file
            await pilot.click("#file_path")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("data2.csv"))
            await pilot.press("enter")
            await pilot.click("#sql_input")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(
                *list(
                    "create table data3 as (select * from data1 join data2 on data1.id = data2.id)"  # noqa
                )
            )
            await pilot.press("ctrl+r")  # execute query
            await pilot.click("#sql_input")
            await pilot.press("ctrl+c")  # clear input
            await pilot.press(*list("select value_a,value_b from data3"))
            await pilot.press("ctrl+s")  # save result
            file_content = tmpdir.join("results.csv").read_text(encoding="utf-8")

            lines = list(filter(lambda x: x != "", file_content.splitlines()))
            expected_lines = ["value_a,value_b", "hello,world", "hi,there"]
            assert lines == expected_lines
