# luigi_project

NOTE: No requirements file in this repo. I should add this.

This was just a test to learn luigi with sql. I Create two tables. One table that I download stock data (close, open, etc.) from quandl API, and one table where I store the rows which have a positive return (I define this as close-open). I then create two luigi tasks for this.

Once the database is setup (use create_db_tables.py). The task can be run.

A config.py file is needed for the database and quandl credentials:
db_host = 'host:port'
db_db_name = 'tablename'
db_user = 'username'
db_password = 'password'
quandl_api_key = 'apikey'

Example command to run luigi:
PYTHONPATH='.' luigi --module luigi_tasks SavePositiveReturn --date 2018-01-10 --code WIKI/PRICES --ticker A

host luigi central planner:
luigid

it will run on http://localhost:808


