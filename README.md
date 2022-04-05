# clidb

![screenshot](./img/iris.png)

clidb is a command line sql client for individual data files, allowing these to be queried (even joined) and viewed. It natively supports CSV and parquet formats, with support for other file types available via the optional extras dependency.

## Data Formats
The following file types can be opened as views in clidb without extras:
- csv
- parquet(.gz)

With pandas installed, the following are also supported:
- json(l)
- xls(x)
- clipboard
- ...

## Usage

This package can be installed with:

```bash
pip install "clidb[extras]"
```

and executed via:

```bash
clidb
```

### Arguments

If a filename is supplied as an argument to clidb then it will open the data file as a view.

If a directory or S3 path is supplied then the directory view will open in that location.

For example:

```bash
clidb data/iris.csv
```

or

```bash
clidb s3://somebucket/data/
```

The contents of the clipboard can be converted into a view (e.g. after copying from Google Sheets), using the `--clipboard` argument:

```bash
clidb --clipboard
```

For some data sources, it can be helpful to render lines that separate rows. This can be enabled via the `row-lines` option:

```bash
clidb --row-lines
```

## Advanced Usage
New views can be created from an opened file. For example if `iris.csv` was opened as the view `iris`, then we could create a new view:
```sql
create view iris_variety as (select variety, avg("petal.length") from iris group by variety)
```

![create view](./img/iris_variety.png)

Views can be joined together, for example:
```sql
select * from iris natural join iris_variety
```

![join](./img/iris_join.png)
