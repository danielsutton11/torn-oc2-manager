package com.Torn.ApiKeys;

import com.Torn.Helpers.Constants;
import com.Torn.Execute;  // Import the Execute class instead
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
import java.util.concurrent.TimeUnit;

public class ValidateApiKeys {

    private static final Logger logger = LoggerFactory.getLogger(ValidateApiKeys.class);

    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    private static final int RATE_LIMIT_DELAY_MS = 2000;

    public static void Validate() throws SQLException, IOException {
        logger.info("Starting API key validation process");

        String databaseUrl = System.getenv(Constants.DATABASE_URL);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL environment variable not set");
        }

        logger.info("Connecting to database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            List<String> apiKeys = getApiKeys(connection);

            if (apiKeys.isEmpty()) {
                logger.warn("No API keys found to validate");
                return;
            }

            logger.info("Found {} API keys to validate", apiKeys.size());

            int validatedCount = 0;
            int successfulCount = 0;

            for (String apiKey : apiKeys) {
                try {
                    boolean isValid = validateApiKey(apiKey, connection);
                    if (isValid) {
                        successfulCount++;
                    }
                    validatedCount++;

                    // Rate limiting - wait between API calls (except for the last one)
                    if (validatedCount < apiKeys.size()) {
                        logger.debug("Waiting {}ms before next API validation", RATE_LIMIT_DELAY_MS);
                        Thread.sleep(RATE_LIMIT_DELAY_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("API key validation interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error validating API key {}: {}", maskApiKey(apiKey), e.getMessage());
                }
            }

            logger.info("API key validation completed. Validated: {}, Successful: {}",
                    validatedCount, successfulCount);

        } catch (SQLException e) {
            logger.error("Database error during API key validation", e);
            throw e;
        }
    }

    private static List<String> getApiKeys(Connection connection) throws SQLException {
        List<String> values = new ArrayList<>();

        if (!isValidIdentifier(Constants.TABLE_NAME_API_KEYS) || !isValidIdentifier(Constants.COLUMN_NAME_API_KEY)) {
            throw new IllegalArgumentException("Invalid table or column name");
        }

        String sql = "SELECT " + Constants.COLUMN_NAME_API_KEY + " FROM " + Constants.TABLE_NAME_API_KEYS;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);
                if (apiKey != null && !apiKey.trim().isEmpty()) {
                    values.add(apiKey.trim());
                }
            }
        }

        return values;
    }

    private static boolean validateApiKey(String apiKey, Connection connection) throws IOException, SQLException {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            logger.warn("Skipping empty or null API key");
            return false;
        }

        String maskedKey = maskApiKey(apiKey);
        logger.debug("Validating API key: {}", maskedKey);

        Request request = new Request.Builder()
                .url(Constants.API_URL_VALIDATE_KEY)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + apiKey)
                .build();

        try (Response response = client.newCall(request).execute()) {
            boolean isValid = response.isSuccessful();

            if (!isValid) {
                logger.warn("API key validation failed for key: {} (Response code: {})",
                        maskedKey, response.code());

                if (response.code() == 401) {
                    logger.debug("API key {} is unauthorized (invalid)", maskedKey);
                } else if (response.code() == 429) {
                    logger.warn("Rate limited by Torn API for key {}", maskedKey);
                } else {
                    logger.warn("Unexpected response code {} for key {}", response.code(), maskedKey);
                }
            } else {
                logger.info("API key validation successful for key: {}", maskedKey);
            }

            updateApiKeyStatus(apiKey, isValid, connection);
            return isValid;

        } catch (Exception e) {
            logger.error("Exception during API validation for key {}: {}", maskedKey, e.getMessage());
            throw e;
        }
    }

    private static void updateApiKeyStatus(String apiKey, boolean active, Connection connection) throws SQLException {
        if (!isValidIdentifier(Constants.COLUMN_NAME_ACTIVE)) {
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
                logger.debug("Updated API key status to {} for key: {}", active, maskApiKey(apiKey));
            } else {
                logger.warn("No rows updated for API key: {}", maskApiKey(apiKey));
            }
        }
    }

    private static boolean isValidIdentifier(String identifier) {
        return identifier != null && identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }

    private static String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() < 8) {
            return "****";
        }
        return apiKey.substring(0, 4) + "****" + apiKey.substring(apiKey.length() - 4);
    }
}