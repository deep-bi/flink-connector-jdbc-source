from itertools import combinations
from pathlib import Path
from typing import Callable, List, Sequence

import pytest
from py4j.protocol import Py4JJavaError
from pyflink.common import Duration
from pyflink.datastream import StreamExecutionEnvironment, SourceFunction
from pyflink.datastream.connectors import Source

from pyflink_deepbi.jdbc import JDBCSourceBuilder, Parsers

BuilderStep = Callable[[JDBCSourceBuilder], JDBCSourceBuilder]
BuilderSteps = Sequence[BuilderStep]


@pytest.fixture()
def jars() -> List[str]:
    # TODO: Replace it with downloading from repository!
    names = [
        "flink-connector-jdbc-source-1.15-0.1.1.jar",
        "sqlite-jdbc-3.36.0.3.jar"
    ]

    paths = ["file://" + str((Path("tests/pyflink_deepbi/jars") / name).absolute()) for name in names]
    print(paths)
    return paths


@pytest.fixture()
def env(jars) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(*jars)
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
def test_can_build_source_fn_with_lower_case_parser(env, steps: BuilderSteps):
    builder = compose(steps)(JDBCSourceBuilder())
    source = builder.with_parser(Parsers.json_string(str.lower)).build_source_function()

    assert isinstance(source, SourceFunction)


@pytest.mark.parametrize("steps", builder_steps(4))
def test_can_build_source_with_minimal_parameters(env, steps: BuilderSteps):
    source = compose(steps)(JDBCSourceBuilder()).build_source()
    assert isinstance(source, Source)
