package bi.deep.flink;

import bi.deep.flink.source.JdbcReaderTask;
import bi.deep.flink.source.JdbcSourceConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class JdbcSourceFunction<T> implements SourceFunction<T> {
    private final JdbcSourceConfig<T> config;
    private transient Logger logger;
    private transient ScheduledExecutorService queryService;
    private transient ScheduledFuture<?> queryTask;

    private boolean running;

    public JdbcSourceFunction(JdbcSourceConfig<T> config) {
        this.config = config;
    }

    private void pollLoop(BlockingQueue<T> pipe, SourceContext<T> sourceContext) throws InterruptedException {
        while (running) {
            T result = pipe.poll(config.getPollInterval().toMillis(), TimeUnit.MILLISECONDS);
            if (result != null) sourceContext.collect(result);
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) {
        queryService = Executors.newSingleThreadScheduledExecutor();
        logger = LoggerFactory.getLogger(JdbcSourceFunction.class);
        BlockingQueue<T> pipe = new LinkedBlockingQueue<>();

        queryTask = queryService.scheduleAtFixedRate(new JdbcReaderTask<>(pipe, config),
                config.getInitialDiscoveryOffset().toMillis(),
                config.getDiscoveryInterval().toMillis(),
                TimeUnit.MILLISECONDS);

        running = true;
        try {
            pollLoop(pipe, sourceContext);
        } catch (InterruptedException e) {
            running = false;
            logger.info("Interrupted JDBC Source Function");
        }
    }

    @Override
    public void cancel() {
        queryTask.cancel(true);
        queryService.shutdown();

        try {
            queryService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Didn't manage to stop threading services in time", e);
        }
    }
}
