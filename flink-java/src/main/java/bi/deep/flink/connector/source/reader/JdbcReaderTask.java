package bi.deep.flink.connector.source.reader;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.utils.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

public class JdbcReaderTask<T> implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(JdbcReaderTask.class);
    private final BlockingQueue<Result<T>> results;
    private final JdbcSourceConfig<T> config;

    public JdbcReaderTask(BlockingQueue<Result<T>> results, JdbcSourceConfig<T> config) {
        this.results = results;
        this.config = config;
    }

    private void processRow(ResultSet row) throws InterruptedException {
        Result<T> maybeRecord = config.getParser().apply(row);
        results.put(maybeRecord);
    }

    private void query() throws SQLException, InterruptedException {
        try (Connection connection = config.getConnection()) {
            // https://stackoverflow.com/a/1331922
            connection.setAutoCommit(false);
            try (Statement stmt = connection.createStatement()) {
                // http://benjchristensen.com/2008/05/27/mysql-jdbc-memory-usage-on-large-resultset/
                // stmt.setFetchSize(Integer.MIN_VALUE);
                stmt.setFetchSize(25000);
                try (ResultSet set = stmt.executeQuery(config.getQuery())) {
                    while (set.next()) {
                        processRow(set);
                    }
                }

            } catch (Throwable e) {
                results.put(Result.exceptional(e));
                throw e;
            }
        }
    }

    @Override
    public void run() {
        try {
            query();
        } catch (SQLException | InterruptedException e) {
            logger.error("Exception occurred when processing reader task", e);
        }
    }
}

