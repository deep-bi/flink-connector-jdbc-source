package bi.deep.jdbc.parsers;

import bi.deep.flink.connector.source.reader.JdbcReaderTask;
import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.database.parsers.Parsers;
import bi.deep.flink.connector.source.database.parsers.Result;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class H2ParsersTest {

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
        ObjectNode node = om.createObjectNode();
        Map<String, Object> json = new HashMap<>();

        for (String col : columns) {
            int i = dataColumns.indexOf(col);
            json.put(dataColumns.get(i), record[i]);
        }
        return om.valueToTree(json);
    }

    private static final String H2_URL = "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1";

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

    private static JdbcSourceConfig<String> queryJsonConfig(String query) {
        return JdbcSourceConfig.<String>builder()
                .withUrl(H2_URL)
                .withQuery(query)
                .withParser(Parsers.JsonString())
                .withDiscoveryInterval(Duration.ZERO)
                .build();
    }

    private static Object[] queueToArray(BlockingQueue<String> queue) {
        return queue.stream().map(raw -> Result.of(raw).map(om::readTree).get()).toArray();
    }

    private static Object[] dataToArray(List<String> columns) {
        return data.stream().map(record -> dataToJson(record, columns)).toArray();
    }

    private static Object[] dataToArray(List<String> columns, Predicate<JsonNode> predicate) {
        return data.stream().map(record -> dataToJson(record, columns)).filter(predicate).toArray();
    }

    @Test
    public void selectStar() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT * FROM employees;"));
        task.run();

        assertArrayEquals(dataToArray(dataColumns), queueToArray(queue));
    }

    @Test
    public void selectAge() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT age FROM employees;"));

        task.run();

        assertArrayEquals(dataToArray(List.of("age")), queueToArray(queue));
    }


    @Test
    public void selectSalary() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT salary FROM employees;"));

        task.run();

        assertArrayEquals(dataToArray(List.of("salary")), queueToArray(queue));
    }


    @Test
    public void selectName() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT name FROM employees;"));

        task.run();

        assertArrayEquals(dataToArray(List.of("name")), queueToArray(queue));
    }

    @Test
    public void selectStarWhereAgeAtLeast30() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT * FROM employees WHERE age >= 30;"));

        task.run();

        assertArrayEquals(dataToArray(dataColumns, (node) -> node.get("age").asInt() >= 30), queueToArray(queue));
    }
}