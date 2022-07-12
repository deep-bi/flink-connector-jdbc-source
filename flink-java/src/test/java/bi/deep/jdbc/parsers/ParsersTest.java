package bi.deep.jdbc.parsers;

import bi.deep.flink.source.JdbcReaderTask;
import bi.deep.flink.source.JdbcSourceConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ParsersTest {


    @Test
    public void queryJsonFromDatabase() throws SQLException {
        JdbcSourceConfig<String> config = JdbcSourceConfig.<String>builder()
                .withUrl("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1")
                .withQuery("SELECT name, age FROM users;")
                .withParser(Parsers.JsonString())
                .withDiscoveryInterval(Duration.ZERO)
                .build();

        try (Connection connection = config.getConnection()) {
            connection.createStatement().execute("CREATE TABLE users (name VARCHAR(20) NOT NULL, age INT NOT NULL);");
            connection.createStatement().execute("INSERT INTO users VALUES ('usr1', 20);");
            connection.createStatement().execute("INSERT INTO users VALUES ('usr2', 35);");
            connection.commit();
        }

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        JdbcReaderTask<String> task = new JdbcReaderTask<>(queue, config);

        task.run();

        assertArrayEquals(new String[]{"{\"NAME\":\"usr1\",\"AGE\":20}", "{\"NAME\":\"usr2\",\"AGE\":35}"}, queue.toArray());
    }

}