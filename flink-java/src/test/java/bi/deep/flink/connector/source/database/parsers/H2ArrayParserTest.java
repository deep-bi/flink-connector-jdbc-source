package bi.deep.flink.connector.source.database.parsers;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.reader.JdbcReaderTask;
import bi.deep.flink.connector.source.utils.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class H2ArrayParserTest {
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

    private static JdbcSourceConfig<String> queryJsonConfig(String query) {
        return JdbcSourceConfig.<String>builder()
                .withUrl(H2_URL)
                .withQuery(query)
                .withParser(Parsers.JsonString())
                .withDiscoveryInterval(Duration.ZERO)
                .build();
    }

    @Test
    public void selectArray() throws InterruptedException {
        BlockingQueue<Result<String>> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, queryJsonConfig("SELECT * FROM foo;"));
        task.run();

        assertEquals("{\"bar\":[1,2,3]}", queue.take().get());
    }
}

