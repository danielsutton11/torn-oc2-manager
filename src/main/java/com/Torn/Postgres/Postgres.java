package com.Torn.Postgres;

import com.Torn.Helpers.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Postgres {

    // Cache data sources to avoid recreating them
    private static final ConcurrentHashMap<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * Get a connection using HikariCP connection pooling
     */
    public Connection connect(String databaseUrl, Logger logger) throws SQLException {
        HikariDataSource dataSource = getOrCreateDataSource(databaseUrl, logger);
        return dataSource.getConnection();
    }

    public Connection connectSimple(String databaseUrl, Logger logger) throws SQLException {
        // Convert postgresql:// to jdbc:postgresql://
        String jdbcUrl = databaseUrl.startsWith("postgresql://")
                ? databaseUrl.replace("postgresql://", "jdbc:postgresql://")
                : databaseUrl;

        logger.info("Creating simple database connection to: {}", maskDatabaseUrl(jdbcUrl));

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            logger.info("Simple database connection successful");
            return conn;
        } catch (SQLException e) {
            logger.error("Simple database connection failed: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Get or create a HikariCP DataSource for the given database URL
     */
    private static HikariDataSource getOrCreateDataSource(String databaseUrl, Logger logger) {
        return dataSources.computeIfAbsent(databaseUrl, url -> createDataSource(url, logger));
    }

    /**
     * Create a new HikariCP DataSource
     */
    private static HikariDataSource createDataSource(String databaseUrl, Logger logger) {
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalArgumentException("Database URL cannot be null or empty");
        }

        String jdbcUrl = convertToJdbcUrl(databaseUrl);
        logger.info("Creating database connection pool for: {}", maskDatabaseUrl(jdbcUrl));

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName("org.postgresql.Driver");

        // Much more aggressive timeouts for Railway
        config.setMaximumPoolSize(3);           // Reduced pool size
        config.setMinimumIdle(0);               // No minimum idle connections
        config.setConnectionTimeout(10000);     // 10 seconds instead of 30
        config.setValidationTimeout(5000);      // 5 seconds validation
        config.setIdleTimeout(300000);          // 5 minutes
        config.setMaxLifetime(600000);          // 10 minutes instead of 30
        config.setLeakDetectionThreshold(30000); // 30 seconds instead of 60

        // Test connection on startup
        config.setInitializationFailTimeout(15000); // 15 seconds max to initialize

        // PostgreSQL optimizations
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("socketTimeout", "10");      // 10 second socket timeout
        config.addDataSourceProperty("loginTimeout", "10");       // 10 second login timeout

        return new HikariDataSource(config);
    }

    /**
     * Convert various PostgreSQL URL formats to JDBC URL format
     */
    private static String convertToJdbcUrl(String databaseUrl) {
        // Already in JDBC format
        if (databaseUrl.startsWith("jdbc:postgresql://")) {
            return databaseUrl;
        }

        // Convert postgresql:// to jdbc:postgresql://
        if (databaseUrl.startsWith("postgresql://")) {
            return databaseUrl.replace("postgresql://", "jdbc:postgresql://");
        }

        if (databaseUrl.startsWith(Constants.POSTGRES_URL)) {
            // Parse: postgresql://user:pass@host:port/database
            String withoutProtocol = databaseUrl.substring("postgresql://".length());

            // Split on @ to separate credentials from host
            String[] parts = withoutProtocol.split("@", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid PostgreSQL URL format - missing @");
            }

            String credentials = parts[0]; // user:password
            String hostAndDb = parts[1];   // host:port/database

            String[] credParts = credentials.split(":", 2);
            if (credParts.length != 2) {
                throw new IllegalArgumentException("Invalid PostgreSQL URL - missing user:password");
            }

            String user = credParts[0];
            String password = credParts[1];

            // Return proper JDBC URL
            return String.format("jdbc:postgresql://%s?user=%s&password=%s", hostAndDb, user, password);
        }

        throw new IllegalArgumentException("Unsupported database URL format: " + maskDatabaseUrl(databaseUrl));
    }

    /**
     * Mask database URL for secure logging
     */
    private static String maskDatabaseUrl(String url) {
        if (url == null) return "null";
        // Replace password in URL with ****
        return url.replaceAll("://([^:/@]+):([^@/:]+)@", "://$1:****@")
                .replaceAll("password=([^&]+)", "password=****");
    }

    /**
     * Get direct access to DataSource for advanced use cases
     */
    public HikariDataSource getDataSource(String databaseUrl, Logger logger) {
        return getOrCreateDataSource(databaseUrl, logger);
    }

    /**
     * Clean up all data sources (call this on application shutdown)
     */
    public static void cleanup() {
        for (HikariDataSource dataSource : dataSources.values()) {
            if (!dataSource.isClosed()) {
                dataSource.close();
            }
        }
        dataSources.clear();
    }
}