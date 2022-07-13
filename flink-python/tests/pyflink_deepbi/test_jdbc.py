from typing import Callable, Sequence
from itertools import combinations
import pytest

from pyflink.common import Duration
from pyflink.datastream import StreamExecutionEnvironment, SourceFunction
from pyflink.datastream.connectors import Source
from pyflink_deepbi.jdbc import JDBCSourceBuilder, Parsers
from py4j.protocol import Py4JJavaError

BuilderStep = Callable[[JDBCSourceBuilder], JDBCSourceBuilder]
BuilderSteps = Sequence[BuilderStep]

# TODO: Replace it with downloading from repository!
JARS = [
    "file:///home/bmikulski/Deep/Repos/deep-flink-source-jdbc/flink-java/target/flink-source-jdbc-1.0-SNAPSHOT.jar",
    "file:///home/bmikulski/Deep/Repos/deep-flink-source-jdbc/flink-python/sqlite-jdbc-3.36.0.3.jar",
]

@pytest.fixture()
def env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(*JARS)
    return env


def builder_steps(repeat): 
    with_query = lambda builder: builder.with_query("SELECT * FROM table;") 
    with_parser = lambda builder: builder.with_parser(Parsers.json_string())
    with_url = lambda builder: builder.with_url("jdbc:sqlite::mem:")
    with_discovery_interval = lambda builder: builder.with_discovery_interval(Duration.of_minutes(10))

    return combinations([with_query, with_parser, with_url, with_discovery_interval], r=repeat)


def compose(steps: BuilderSteps) -> BuilderStep:

    def composition(builder: JDBCSourceBuilder) -> JDBCSourceBuilder:
        for step in steps:
            builder = step(builder)
        return builder
    
    return composition


@pytest.mark.parametrize("steps", builder_steps(3))
def test_invalid_build_throws_exception(env, steps: BuilderSteps):
    builder = compose(steps)(JDBCSourceBuilder())

    with pytest.raises(Py4JJavaError):
        builder.build_source_function()


@pytest.mark.parametrize("steps", builder_steps(4))
def test_can_build_source_fn_with_minimal_parameters(env, steps: BuilderSteps):
    source = compose(steps)(JDBCSourceBuilder()).build_source_function()
    assert isinstance(source, SourceFunction)


@pytest.mark.parametrize("steps", builder_steps(4))
def test_can_build_source_fn_with_minimal_parameters(env, steps: BuilderSteps):
    source = compose(steps)(JDBCSourceBuilder()).build_source()
    assert isinstance(source, Source)