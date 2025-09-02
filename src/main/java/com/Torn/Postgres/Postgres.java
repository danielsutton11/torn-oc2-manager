package com.Torn.Postgres;

import com.Torn.Helpers.Constants;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Postgres {
    
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

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            logger.info("Database connection successful");
            return conn;

        } catch (SQLException e) {
            logger.error("Database connection failed. URL format: {}",
                    jdbcUrl.replaceAll(":[^:/@]+@", ":***@"));
            throw e;
        }
        
    }
}
