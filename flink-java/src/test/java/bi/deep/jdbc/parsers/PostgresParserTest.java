package bi.deep.jdbc.parsers;

import bi.deep.flink.connector.source.reader.JdbcReaderTask;
import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.database.parsers.Parsers;
import bi.deep.flink.connector.source.database.parsers.Result;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class PostgresParserTest {
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
    public void testConnection() {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest")) {
            postgres.withInitScript("init_postgresql.sql").start();

            JdbcSourceConfig<String> config = JdbcSourceConfig.<String>builder()
                    .withUrl(postgres.getJdbcUrl())
                    .withQuery("SELECT * FROM employees;")
                    .withParser(Parsers.JsonString())
                    .withUser(postgres.getUsername())
                    .withPassword(postgres.getPassword())
                    .withDiscoveryInterval(Duration.ZERO)
                    .build();

            BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, config);
            task.run();

            assertArrayEquals(dataToArray(dataColumns), queueToArray(queue));
        }
    }

}
