package bi.deep.flink.source;

public class ParallelismExceededException extends RuntimeException {

    public ParallelismExceededException(int actual) {
        super(String.format("Parallelism exceeded the JDBC connector works only with parallelism of 1, but it was %d", actual));
    }

}
