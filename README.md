# LoQL

![screenshot](./img/loql_iris.png)

LoQL is a command line sql client for individual data files, allowing these to be queried (even joined) and viewed. It natively supports CSV, parquet and other formats.

## Data Formats
The following file types can be opened as views in LoQL:
- csv
- parquet(.gz)
- json(l)
- xls(x)
- clipboard
- ...

## Usage

This package can be installed with:

```bash
pipx install loql
```

and executed via:

```bash
loql
```

### Arguments

If a filename is supplied as an argument to LoQL then it will open the data file as a view.

If a directory is supplied then the open file view will start in that location.

For example:

```bash
loql data/iris.csv
```

The contents of the clipboard can be converted into a view (e.g. after copying from Google Sheets), using the `--clipboard` argument:

```bash
loql --clipboard
```

## Advanced Usage
New views can be created from an opened file. For example if `iris.csv` was opened as the view `iris`, then we could create a new view:
```sql
create view iris_variety as (
    select
        variety,
        avg("petal.length") as avg_petal_length
    from iris
    group by variety
)
```

![create view](./img/loql_iris_variety.png)

Views can be joined together, for example:
```sql
select * from iris natural join iris_variety
```

![join](./img/loql_iris_join.png)
