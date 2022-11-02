package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.reader.JdbcReaderTask;
import bi.deep.flink.connector.source.utils.Result;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresParserQuotetTextTest {

    @Test
    public void testConnection() {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest")) {
            postgres.withInitScript("init_postgresql_quotes.sql").start();

            JdbcSourceConfig<String> config = JdbcSourceConfig.<String>builder()
                    .withUrl(postgres.getJdbcUrl())
                    .withQuery("SELECT v1 FROM quotes;")
                    .withParser(Parsers.JsonString())
                    .withUser(postgres.getUsername())
                    .withPassword(postgres.getPassword())
                    .withDiscoveryInterval(Duration.ZERO)
                    .build();

            BlockingQueue<Result<String>> queue = new LinkedBlockingQueue<>();
            JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, config);
            task.run();

            assertEquals("{\"v1\":\"Some famous \\\"quote\\\"\"}", Objects.requireNonNull(queue.poll()).get());
        }
    }

}
