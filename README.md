# JDBC Streaming Source for Flink

This is implementation of JDBC streaming source for Flink and PyFlink.


## JDBC Streaming Source

A JDBC streaming source reads a JDBC compliant database by using provided query. To create a source you can use following code (in PyFlink):


```python
from pyflink.common import Duration
from pyflink_deepbi.jdbc import JDBCSourceBuilder, Parsers

source_fn = (
    JDBCSourceBuilder()
        .with_query("SELECT * FROM users;")  # Query to run against the database
        .with_url("jdbc:sqlite:sample.db")  # Database connection string
        .with_parser(Parsers.json_string())  # Parser that converts database records to stream event
        .with_discovery_interval(Duration.of_hours(1))  # How frequently query the database (in milliseconds)?
        .build_source_function()  # Builds the source function
)

stream = env.add_source(source_fn, "JDBC Query", source_fn.output_type)
```

Above implementation uses source function to read the database. However, we also have the Source API based implementation.
The problem is that it only works if you run the application with: `flink run --python script.py` and the JARs are in the
Flink classpath. There is a bug when starting application with `python script.py`, that the Flink cannot see the classes from
provided JARS (as the Source API exposes custom classes outside the function). For this reason the code above is recommended as
you can run the same code on cluster and locally from script.

## Deployment

To build a fat jar change directory to `flink-java` and run: `mvn package`.

## Development

Supported Python versions are Python 3.7, Python 3.8, Python 3.9 and Python 3.10. 
Create and activate a virtual environment with the Python version of your choice.

To install Python dependencies change directory to `flink-python` and run:
```bash
pip install -r requirements.txt
pip install -e .  # to install module as editable for the unit test
```

To test across Python versions run `tox`. For running only unit test on current interpreter run: `pytest tests/`.