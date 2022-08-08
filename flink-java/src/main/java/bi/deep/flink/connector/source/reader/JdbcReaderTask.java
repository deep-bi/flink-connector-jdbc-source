package bi.deep.flink.connector.source.reader;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.utils.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        try (Statement stmt = config.getConnection().createStatement()) {
            ResultSet set = stmt.executeQuery(config.getQuery());
            while (set.next()) {
                processRow(set);
            }
        } catch (Throwable e) {
            results.put(Result.exceptional(e));
            throw e;
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

