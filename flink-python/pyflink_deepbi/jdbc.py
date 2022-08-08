from typing import Dict, Optional, Callable

from py4j.java_gateway import JavaObject
from pyflink.common import TypeInformation, Types, Duration
from pyflink.datastream import SourceFunction
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway

from pyflink_deepbi.util import dict_to_java_properties


class DisplayFunction:

    def __init__(self, display_f: Callable[[str], str]):
        self.display_f = display_f

    def apply(self, value: str) -> str:
        return self.display_f(value)

    class Java:
        implements = ["bi.deep.flink.connector.source.utils.SerializableFunction"]


class Parser:
    """ Python wrapper around JDBC Parsers """

    def __init__(self, j_parser: JavaObject, output_type: TypeInformation):
        self.j_parser = j_parser
        self.output_type = output_type


class Parsers:

    @staticmethod
    def json_string(display_f: Optional[Callable[[str], str]] = None) -> Parser:
        """
        :param display_f: Optional function that transforms JDBC column name to a display column name. If None (default)
                          returns unchanged JDBC column name.
        """
        j_parsers = get_gateway().jvm.bi.deep.flink.connector.source.database.parsers.Parsers
        if display_f is None:
            return Parser(j_parsers.JsonString(), Types.STRING())
        else:
            return Parser(j_parsers.JsonString(DisplayFunction(display_f)), Types.STRING())


class JDBCSource(Source):
    """
    JDBC Source that uses Source API. Periodically runs a given query against the JDBC database and emits all 
    records as a stream with a given DB record to stream event parser.

    The source defines property `output_type` - a data type returned by the parser.
    """

    def __init__(self, configuration: JavaObject, output_type: TypeInformation):
        JSource = get_gateway().jvm.bi.deep.flink.connector.source.JdbcSource(configuration)
        self.output_type = output_type
        super().__init__(JSource)


class JDBCSourceFunction(SourceFunction):
    """
    JDBC Source that uses Source Function. Periodically runs a given query against the JDBC database and emits all 
    records as a stream with a given DB record to stream event parser.

    The source defines property `output_type` - a data type returned by the parser.
    """

    def __init__(self, configuration: JavaObject, output_type: TypeInformation):
        JSource = get_gateway().jvm.bi.deep.flink.connector.source.JdbcSourceFunction(configuration)
        self.output_type = output_type
        super().__init__(JSource)


class JDBCSourceBuilder:
    """
    JDBC Source Builder. This is entry point for the source creation. The parameters: `query`, `url`, `parser` and 
    `discovery_interval` are required by the builder during build.

    Use `build_source_function()` to build the source.
    """

    def __init__(self):
        self.output_type = Types.PICKLED_BYTE_ARRAY()
        self._j_builder = get_gateway().jvm.bi.deep.flink.connector.source.JdbcSourceConfig.builder()

    def with_query(self, query: str) -> 'JDBCSourceBuilder':
        """
        Database query to be executed. You can use any scan query, nested queries, filters, groupby etc. 
        Can also use: `SELECT * ...` the names will be resolved automatically.
        Required.
        """
        self._j_builder = self._j_builder.withQuery(query)
        return self

    def with_url(self, connection_url: str) -> 'JDBCSourceBuilder':
        """
        Database URL (connection) string. Required.
        """
        self._j_builder = self._j_builder.withUrl(connection_url)
        return self

    def with_user(self, user: str) -> 'JDBCSourceBuilder':
        """ Name of the `user` that reads this database. Optional. """
        self._j_builder = self._j_builder.withUser(user)
        return self

    def with_password(self, password: str) -> 'JDBCSourceBuilder':
        """ The password for this database. Optional. """
        self._j_builder = self._j_builder.withPassword(password)
        return self

    def with_connection_properties(self, properties: Dict[str, str]) -> 'JDBCSourceBuilder':
        """ 
        The properties to be used by the underlying JDBC Connection. 
        See the corresponding JDBC database implementation to see what properties can be defined.
        Optional. 
        """
        j_properties = dict_to_java_properties(properties)
        self._j_builder = self._j_builder.withConnectionProperties(j_properties)
        return self

    def with_parser(self, parser: Parser) -> 'JDBCSourceBuilder':
        """ Parser that converts the database records into stream events. Required. """
        self.output_type = parser.output_type
        self._j_builder = self._j_builder.withParser(parser.j_parser)
        return self

    def with_ignore_parse_exception(self, ignore: bool) -> 'JDBCSourceBuilder':
        """ Should the parsing exception be ignored? By default they are not ignored and source throws an exception. Optional. """
        self._j_builder = self._j_builder.withIgnoreParseException(ignore)
        return self

    def with_initial_discovery_offset(self, duration: Duration) -> 'JDBCSourceBuilder':
        """ The offset between source start and first database query execution. By default 0. Optional."""
        self._j_builder = self._j_builder.withInitialDiscoveryOffset(duration._j_duration)
        return self

    def with_discovery_interval(self, duration: Duration) -> 'JDBCSourceBuilder':
        """ How often the database should be queried? Required. """
        self._j_builder = self._j_builder.withDiscoveryInterval(duration._j_duration)
        return self

    def with_poll_interval(self, duration: Duration) -> 'JDBCSourceBuilder':
        """ 
        Internal configuration. How long the worker should wait for the internal queue for the JDBC results. Optional.
        """
        self._j_builder = self._j_builder.withPollInterval(duration._j_duration)
        return self

    def build_source(self) -> JDBCSource:
        """ Builds a `JDBCSource` as a Source to be used with `env.from_source`"""
        j_config = self._j_builder.build()
        return JDBCSource(j_config, self.output_type)

    def build_source_function(self) -> JDBCSourceFunction:
        """ Builds a `JDBCSourceFunction` as a Source Function to be used with `env.add_source` """
        j_config = self._j_builder.build()
        return JDBCSourceFunction(j_config, self.output_type)
