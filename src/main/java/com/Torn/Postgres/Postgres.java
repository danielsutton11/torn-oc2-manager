package com.Torn.Postgres;

import com.Torn.Helpers.Constants;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public class Postgres {

    private static final Logger logger = LoggerFactory.getLogger(Postgres.class);

    // Cache data sources to avoid recreating them
    private static final ConcurrentHashMap<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * Get a connection using HikariCP connection pooling
     */
    public Connection connect(String databaseUrl, Logger logger) throws SQLException {

        String jdbcUrl = databaseUrl;
        String user = null;
        String password = null;

        if (databaseUrl.startsWith(Constants.POSTGRES_URL)) {
            String cleaned = databaseUrl.substring(Constants.POSTGRES_URL.length());
            String[] parts = cleaned.split("@");
            String[] userInfo = parts[0].split(":");
            user = userInfo[0];
            password = userInfo[1];

            jdbcUrl = Constants.POSTGRES_JDBC_URL + parts[1];
        }

        logger.info("Attempting to connect to database...");

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
            logger.info("Database connection successful");
            return conn;

        } catch (SQLException e) {
            logger.error("Database connection failed. URL format: {}",
                    jdbcUrl.replaceAll(":[^:/@]+@", ":***@"));
            throw e;
        }
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

    public static void clearExistingData(Connection connection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int deletedRows = pstmt.executeUpdate();
            logger.debug("Cleared {} existing rows from {}", deletedRows, tableName);
        }
    }
}