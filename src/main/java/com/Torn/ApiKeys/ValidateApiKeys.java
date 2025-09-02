package com.Torn.ApiKeys;

import com.Torn.Helpers.Constants;
import com.Torn.Postgres.Postgres;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ValidateApiKeys {

    private static final Logger logger = LoggerFactory.getLogger(ValidateApiKeys.class);
    private static final OkHttpClient client = new OkHttpClient();

    public static void Validate() throws SQLException, IOException {
        logger.info("Starting to validate API keys");

        String databaseUrl = System.getenv(Constants.DATABASE_URL);
        if(databaseUrl == null) {
            logger.error("No database url found!");
            return;
        }

        try (Connection connection = new Postgres().connect(databaseUrl, logger)) {
            if(connection == null) {
                logger.error("Failed to connect to database!");
                return;
            }

            List<String> apiKeys = new ValidateApiKeys().getApiKeys(connection);
            for(String apiKey : apiKeys) {
                validateApiKey(apiKey, connection);
            }
        }
    }
    public List<String> getApiKeys(Connection connection) throws SQLException {
        List<String> values = new ArrayList<>();

        if (!isValidIdentifier(Constants.TABLE_NAME_API_KEYS) || !isValidIdentifier(Constants.COLUMN_NAME_API_KEY)) {
            throw new IllegalArgumentException("Invalid table or column name");
        }

        String sql = "SELECT " + Constants.COLUMN_NAME_API_KEY + " FROM " + Constants.TABLE_NAME_API_KEYS;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                values.add(rs.getString(Constants.COLUMN_NAME_API_KEY));
            }
        }

        return values;
    }

    private static void validateApiKey(String apiKey, Connection connection) throws IOException {
        Request request = new Request.Builder()
                .url(Constants.API_URL_VALIDATE_KEY)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + apiKey)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.warn("API key validation failed for key: {} (Response code: {})", apiKey, response.code());
                updateApiKeyStatus(apiKey, false, connection);
            } else {
                logger.info("API key validation successful for key: {}", apiKey);
                updateApiKeyStatus(apiKey, true, connection);
            }
        } catch (SQLException e) {
            logger.error("Database error while updating API key status for key: {}", apiKey, e);
        }
    }

    private static void updateApiKeyStatus(String apiKey, boolean active, Connection connection) throws SQLException {
        // Validate column name for security
        if (!new ValidateApiKeys().isValidIdentifier(Constants.COLUMN_NAME_ACTIVE)) {
            throw new IllegalArgumentException("Invalid active column name");
        }

        String sql = "UPDATE " + Constants.TABLE_NAME_API_KEYS +
                " SET " + Constants.COLUMN_NAME_ACTIVE + " = ? " +
                " WHERE " + Constants.COLUMN_NAME_API_KEY + " = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setBoolean(1, active);
            pstmt.setString(2, apiKey);

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated API key status to {} for key: {}", active, apiKey);
            } else {
                logger.warn("No rows updated for API key: {}", apiKey);
            }
        }
    }

    private boolean isValidIdentifier(String identifier) {
        return identifier != null && identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }
}