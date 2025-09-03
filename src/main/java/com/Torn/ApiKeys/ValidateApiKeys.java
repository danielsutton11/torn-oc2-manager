package com.Torn.ApiKeys;

import com.Torn.Helpers.Constants;
import com.Torn.Execute;
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

    // New class to hold API key info including faction_id
    public static class ApiKeyInfo {
        private final String apiKey;
        private final String factionId;
        private final String ownerName;

        public ApiKeyInfo(String apiKey, String factionId, String ownerName) {
            this.apiKey = apiKey;
            this.factionId = factionId;
            this.ownerName = ownerName;
        }

        public String getApiKey() { return apiKey; }
        public String getFactionId() { return factionId; }
        public String getOwnerName() { return ownerName; }
    }

    public static void Validate() throws SQLException, IOException {
        logger.info("Starting API key validation process");

        String databaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        logger.info("Connecting to database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            List<ApiKeyInfo> apiKeys = getApiKeysWithFactionInfo(connection);

            if (apiKeys.isEmpty()) {
                logger.warn("No API keys found to validate");
                return;
            }

            logger.info("Found {} API keys to validate", apiKeys.size());

            int validatedCount = 0;
            int successfulCount = 0;

            for (ApiKeyInfo apiKeyInfo : apiKeys) {
                try {
                    boolean isValid = validateApiKey(apiKeyInfo, connection);
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
                    logger.error("Error validating API key {} for faction {}: {}",
                            maskApiKey(apiKeyInfo.getApiKey()), apiKeyInfo.getFactionId(), e.getMessage());
                }
            }

            logger.info("API key validation completed. Validated: {}, Successful: {}",
                    validatedCount, successfulCount);

        } catch (SQLException e) {
            logger.error("Database error during API key validation", e);
            throw e;
        }
    }

    // Updated method to get API keys with faction information
    private static List<ApiKeyInfo> getApiKeysWithFactionInfo(Connection connection) throws SQLException {
        List<ApiKeyInfo> apiKeys = new ArrayList<>();

        if (!isValidIdentifier(Constants.TABLE_NAME_API_KEYS) ||
                !isValidIdentifier(Constants.COLUMN_NAME_API_KEY) ||
                !isValidIdentifier(Constants.COLUMN_NAME_FACTION_ID)) {
            throw new IllegalArgumentException("Invalid table or column name");
        }

        String sql = "SELECT " +
                Constants.COLUMN_NAME_API_KEY + ", " +
                Constants.COLUMN_NAME_FACTION_ID + ", " +
                "owner_name " +
                "FROM " + Constants.TABLE_NAME_API_KEYS;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String ownerName = rs.getString(Constants.COLUMN_NAME_OWNER_NAME);

                if (apiKey != null && !apiKey.trim().isEmpty() &&
                        factionId != null && !factionId.trim().isEmpty()) {
                    apiKeys.add(new ApiKeyInfo(apiKey.trim(), factionId.trim(), ownerName));
                }
            }
        }

        return apiKeys;
    }

    // Updated validateApiKey method that takes ApiKeyInfo instead of just the key
    private static boolean validateApiKey(ApiKeyInfo apiKeyInfo, Connection connection) throws IOException, SQLException {
        String apiKey = apiKeyInfo.getApiKey();
        String factionId = apiKeyInfo.getFactionId();

        if (apiKey == null || apiKey.trim().isEmpty()) {
            logger.warn("Skipping empty or null API key for faction {}", factionId);
            return false;
        }

        String maskedKey = maskApiKey(apiKey);
        logger.debug("Validating API key: {} for faction: {}", maskedKey, factionId);

        Request request = new Request.Builder()
                .url(Constants.API_URL_VALIDATE_KEY)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + apiKey)
                .build();

        try (Response response = client.newCall(request).execute()) {
            boolean isValid = response.isSuccessful();

            if (!isValid) {
                logger.warn("API key validation failed for key: {} and faction: {} (Response code: {})",
                        maskedKey, factionId, response.code());

                if (response.code() == 401) {
                    logger.debug("API key {} for faction {} is unauthorized (invalid)", maskedKey, factionId);
                } else if (response.code() == 429) {
                    logger.warn("Rate limited by Torn API for key {} and faction {}", maskedKey, factionId);
                } else {
                    logger.warn("Unexpected response code {} for key {} and faction {}",
                            response.code(), maskedKey, factionId);
                }
            } else {
                logger.info("API key validation successful for key: {} and faction: {}", maskedKey, factionId);
            }

            updateApiKeyStatus(apiKey, factionId, isValid, connection);
            return isValid;

        } catch (Exception e) {
            logger.error("Exception during API validation for key {} and faction {}: {}",
                    maskedKey, factionId, e.getMessage());
            throw e;
        }
    }

    // Updated updateApiKeyStatus method that requires faction ID
    private static void updateApiKeyStatus(String apiKey, String factionId, boolean active, Connection connection) throws SQLException {
        if (!isValidIdentifier(Constants.COLUMN_NAME_ACTIVE)) {
            throw new IllegalArgumentException("Invalid active column name");
        }

        String sql = "UPDATE " + Constants.TABLE_NAME_API_KEYS +
                " SET " + Constants.COLUMN_NAME_ACTIVE + " = ? " +
                " WHERE " + Constants.COLUMN_NAME_API_KEY + " = ? " +
                " AND " + Constants.COLUMN_NAME_FACTION_ID + " = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setBoolean(1, active);
            pstmt.setString(2, apiKey);
            pstmt.setString(3, factionId);

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.debug("Updated {} API key record(s) to active={} for key: {} and faction: {}",
                        rowsAffected, active, maskApiKey(apiKey), factionId);
            } else {
                logger.warn("No rows updated for API key: {} and faction: {}", maskApiKey(apiKey), factionId);
            }

            if (rowsAffected > 1) {
                logger.warn("Updated {} rows for API key {} and faction {} - this suggests duplicate entries",
                        rowsAffected, maskApiKey(apiKey), factionId);
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