package bi.deep.flink.source;

import bi.deep.jdbc.parsers.Parser;
import com.esotericsoftware.kryo.NotNull;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.function.Function;

public class JdbcSourceConfig<T> implements Serializable {

    private String query;
    private String connectionUrl;
    private String user;
    private String password;
    private Properties connectionProperties;
    private Parser<T> parser;
    private Duration initialDiscoveryOffset;

    private Duration discoveryInterval;

    private Duration pollInterval;

    private boolean ignoreParseExceptions;


    /**
     * Get JDBC connection. Parameter resolution:
     * - If connection properties were provided then they are used first with url.
     * - If both user and password are null then just url is used.
     * - Otherwise, url, user and password are used.
     */
    public Connection getConnection() throws SQLException {
        if (connectionProperties != null) {
            return DriverManager.getConnection(connectionUrl, connectionProperties);
        } else if (user == null && password == null) {
            return DriverManager.getConnection(connectionUrl);
        } else {
            return DriverManager.getConnection(connectionUrl, user, password);
        }
    }

    public String getQuery() {
        return query;
    }

    public Parser<T> getParser() {
        return parser;
    }

    public boolean ignoreParseExceptions() {
        return ignoreParseExceptions;
    }

    public Duration getDiscoveryInterval() {
        return discoveryInterval;
    }

    public Duration getInitialDiscoveryOffset() {
        return initialDiscoveryOffset;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    private JdbcSourceConfig() {
    }


    public static <U> Builder<U> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private String query;
        private String connectionUrl;
        private String user;
        private String password;
        private Properties connectionProperties;
        private Parser<T> parser;

        private boolean ignoreParseExceptions = false;

        private Duration initialDiscoveryOffset = Duration.ZERO;

        private Duration discoveryInterval;

        private Duration pollInterval = Duration.of(50, ChronoUnit.MILLIS);

        private Builder() {
        }

        public Builder<T> withQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder<T> withUrl(String connectionUrl) {
            this.connectionUrl = connectionUrl;
            return this;
        }

        public Builder<T> withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder<T> withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> withConnectionProperties(Properties properties) {
            this.connectionProperties = properties;
            return this;
        }

        public Builder<T> withParser(Parser<T> parser) {
            this.parser = parser;
            return this;
        }

        public Builder<T> withIgnoreParseException(boolean ignore) {
            this.ignoreParseExceptions = ignore;
            return this;
        }

        public Builder<T> withInitialDiscoveryOffset(Duration offset) {
            this.initialDiscoveryOffset = offset;
            return this;
        }

        public Builder<T> withDiscoveryInterval(Duration offset) {
            this.discoveryInterval = offset;
            return this;
        }

        public Builder<T> withPollInterval(Duration interval) {
            this.pollInterval = interval;
            return this;
        }

        public JdbcSourceConfig<T> build() {
            String exceptionFormat = "Field `%s` must be set in builder";
            if (this.query == null) {
                throw new RuntimeException(String.format(exceptionFormat, "query"));
            }
            if (this.connectionUrl == null) {
                throw new RuntimeException(String.format(exceptionFormat, "url"));
            }
            if (this.parser == null) {
                throw new RuntimeException(String.format(exceptionFormat, "parser"));
            }
            if (this.discoveryInterval == null) {
                throw new RuntimeException(String.format(exceptionFormat, "discoveryInterval"));
            }

            JdbcSourceConfig<T> config = new JdbcSourceConfig<>();
            config.query = query;
            config.connectionUrl = connectionUrl;
            config.user = user;
            config.password = password;
            config.connectionProperties = connectionProperties;
            config.parser = parser;
            config.ignoreParseExceptions = ignoreParseExceptions;
            config.initialDiscoveryOffset = initialDiscoveryOffset;
            config.discoveryInterval = discoveryInterval;
            config.pollInterval = pollInterval;

            return config;
        }
    }
}