package bi.deep.jdbc.parsers;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.database.parsers.Parsers;
import bi.deep.flink.connector.source.reader.JdbcReaderTask;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresArrayParserTest {
    private static final String H2_URL = "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1";

    @BeforeAll
    static void initAll() throws SQLException {
        try (Connection connection = DriverManager.getConnection(H2_URL)) {
            connection.createStatement().execute("" +
                    "CREATE TABLE foo (bar INTEGER ARRAY);");

            PreparedStatement stmt = connection.prepareStatement("INSERT INTO foo VALUES (?)");
            stmt.clearParameters();
            stmt.setObject(1, new Integer[]{1, 2, 3}, Types.ARRAY);
            stmt.execute();
        }
    }

    @AfterAll
    static void closeAll() throws SQLException {
        try (Connection connection = DriverManager.getConnection(H2_URL)) {
            connection.createStatement().execute("DROP TABLE foo;");
        }
    }

    @Test
    public void selectArray() throws InterruptedException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest")) {
            postgres.withInitScript("init_array_postgresql.sql").start();

            JdbcSourceConfig<String> config = JdbcSourceConfig.<String>builder()
                    .withUrl(postgres.getJdbcUrl())
                    .withQuery("SELECT * FROM foo;")
                    .withParser(Parsers.JsonString())
                    .withUser(postgres.getUsername())
                    .withPassword(postgres.getPassword())
                    .withDiscoveryInterval(Duration.ZERO)
                    .build();

            BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, config);
            task.run();

            assertEquals("{\"bar\":[1,2,3]}", queue.take());
        }
    }
}

