package bi.deep.flink.connector.source.reader;

import bi.deep.flink.connector.source.JdbcSourceConfig;
import bi.deep.flink.connector.source.split.JdbcSplit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class JdbcReader<T> implements SourceReader<T, JdbcSplit> {

    private ExecutorService service;
    private final JdbcSourceConfig<T> config;
    private final Logger logger = LoggerFactory.getLogger(JdbcReader.class);
    public final BlockingQueue<T> results = new LinkedBlockingQueue<>();
    private CompletableFuture<Void> availability;

    private CompletableFuture<Void> submittedTask;

    public JdbcReader(JdbcSourceConfig<T> config) {
        this.config = config;
    }

    private void setAvailability(boolean available) {
        if (available) availability.complete(null);
        else availability = new CompletableFuture<>();
    }

    private void validateTask() {
        if (this.submittedTask != null && this.submittedTask.isCompletedExceptionally()) {
            try {
                Throwable error = this.submittedTask.handle((result, exception) -> exception).get();
                logger.error("Task completed exceptionally", error);
                throw new RuntimeException(error);
            } catch (InterruptedException | ExecutionException exception) {
                logger.error("Error occurred when checking task exception", exception);
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void start() {
        service = Executors.newSingleThreadExecutor();
        setAvailability(false);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        validateTask();

        T result = this.results.poll(config.getPollInterval().toMillis(), TimeUnit.MILLISECONDS);
        if (result == null) {
            validateTask(); // If there were no output, then maybe task failed?
            setAvailability(false);
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            output.collect(result);
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<JdbcSplit> snapshotState(long checkpointId) {
        return new LinkedList<>();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        validateTask();
        return availability;
    }

    @Override
    public void addSplits(List<JdbcSplit> splits) {
        if (splits.size() == 0) logger.warn("Added empty splits");
        else {
            if (splits.size() > 1) logger.warn("Added more than one split. Executing only one");
            submittedTask = CompletableFuture.runAsync(new JdbcReaderTask<>(results, config), service);
            setAvailability(true);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
        service.shutdown();
        if (service.awaitTermination(5, TimeUnit.SECONDS)) {
            logger.info("Closed execution service");
        } else {
            logger.info("Timeout occurred when closing execution service");
        }
    }
}
