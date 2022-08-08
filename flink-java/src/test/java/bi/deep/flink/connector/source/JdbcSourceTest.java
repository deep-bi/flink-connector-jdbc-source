package bi.deep.flink.connector.source;

import bi.deep.flink.connector.source.database.parsers.Parsers;
import bi.deep.flink.connector.source.utils.Result;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcSourceTest {

    @BeforeAll
    static void initAll() throws SQLException {
        try (Connection connection = DriverManager.getConnection(H2_URL)) {
            connection.createStatement().execute("" +
                    "CREATE TABLE employees (" +
                    dataColumns.get(0) + " VARCHAR(20) NOT NULL, " +
                    dataColumns.get(1) + " INT NOT NULL, " +
                    dataColumns.get(2) + " NUMERIC(10, 2) NOT NULL" +
                    ");");

            PreparedStatement stmt = connection.prepareStatement("INSERT INTO employees VALUES (?, ?, ?)");
            for (Object[] record : data) {
                stmt.clearParameters();
                for (int i = 0; i < record.length; i++) {
                    stmt.setObject(i + 1, record[i]);
                }
                stmt.execute();
            }
        }
    }


    @AfterAll
    static void closeAll() throws SQLException {
        try (Connection connection = DriverManager.getConnection(H2_URL)) {
            connection.createStatement().execute("DROP TABLE employees;");
        }
    }


    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(1)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testSelectWithSource() throws Exception {
        CollectSink.values.clear();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(new JdbcSource<>(queryJsonConfig("SELECT * FROM employees;")),
                WatermarkStrategy.forMonotonousTimestamps(),
                "jdbc-source", TypeInformation.of(String.class)
        ).addSink(new CollectSink());

        JobClient client = env.executeAsync();
        Thread.sleep(500);
        client.cancel().get();

        assertArrayEquals(dataToArray(dataColumns), sinkToArray());
    }

    @Test
    public void testInvalidDatabaseConfiguration() {
        CollectSink.values.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(new JdbcSource<>(queryJsonConfig("SELUCT * FROAM employees;")),
                WatermarkStrategy.forMonotonousTimestamps(),
                "jdbc-source", TypeInformation.of(String.class)
        ).addSink(new CollectSink());

        assertThrows(JobExecutionException.class, env::execute);
    }

    private static final ObjectMapper om = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .build();

    private static final List<String> dataColumns = List.of("name", "age", "salary");
    private static final List<Object[]> data = Arrays.asList(
            new Object[]{"Alice", 23, new BigDecimal("3000.00")},
            new Object[]{"Bob", 52, new BigDecimal("7499.99")},
            new Object[]{"Charlie", 37, new BigDecimal("6010.50")}
    );

    private static ObjectNode dataToJson(Object[] record, List<String> columns) {
        Map<String, Object> json = new HashMap<>();

        for (String col : columns) {
            int i = dataColumns.indexOf(col);
            json.put(dataColumns.get(i), record[i]);
        }
        return om.valueToTree(json);
    }

    private static final String H2_URL = "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1";

    private static JdbcSourceConfig<String> queryJsonConfig(String query) {
        return JdbcSourceConfig.<String>builder()
                .withUrl(H2_URL)
                .withQuery(query)
                .withParser(Parsers.JsonString())
                .withDiscoveryInterval(Duration.of(100, ChronoUnit.MILLIS))
                .build();
    }

    private static Object[] sinkToArray() {
        return CollectSink.values.stream().map(raw -> Result.of(raw).map(om::readTree).get()).toArray();
    }

    private static Object[] dataToArray(List<String> columns) {
        return data.stream().map(record -> dataToJson(record, columns)).toArray();
    }

    private static class CollectSink implements SinkFunction<String> {

        // must be static https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/testing/
        public static final Set<String> values = Collections.synchronizedSet(new HashSet<>());

        @Override
        public void invoke(String value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}