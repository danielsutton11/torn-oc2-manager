package com.Torn.ApiKeys;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Helpers.Constants;
import com.Torn.Execute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ValidateApiKeys {

    private static final Logger logger = LoggerFactory.getLogger(ValidateApiKeys.class);

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

    public static void Validate() throws SQLException {
        logger.info("Starting API key validation process with robust error handling");

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

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping API key validation to prevent further failures");
                logger.info("Circuit breaker will reset automatically after 5 minutes, or can be manually reset");
                return;
            }

            int validatedCount = 0;
            int successfulCount = 0;
            int temporaryFailures = 0;
            int permanentFailures = 0;

            for (ApiKeyInfo apiKeyInfo : apiKeys) {
                try {
                    ValidationResult result = validateApiKeyRobust(apiKeyInfo, connection);

                    switch (result.getStatus()) {
                        case SUCCESS:
                            successfulCount++;
                            break;
                        case TEMPORARY_FAILURE:
                            temporaryFailures++;
                            break;
                        case PERMANENT_FAILURE:
                            permanentFailures++;
                            break;
                        case CIRCUIT_BREAKER_OPEN:
                            logger.error("Circuit breaker opened during validation - stopping remaining validations");
                            break;
                    }

                    validatedCount++;

                    // If circuit breaker opened, stop processing
                    if (result.getStatus() == ValidationResult.Status.CIRCUIT_BREAKER_OPEN) {
                        break;
                    }

                } catch (Exception e) {
                    logger.error("Unexpected error validating API key {} for faction {}: {}",
                            maskApiKey(apiKeyInfo.getApiKey()), apiKeyInfo.getFactionId(), e.getMessage());
                    permanentFailures++;
                    validatedCount++;
                }
            }

            // Final summary
            logger.info("API key validation completed:");
            logger.info("  Total processed: {}/{}", validatedCount, apiKeys.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Temporary failures (will retry later): {}", temporaryFailures);
            logger.info("  Permanent failures (need attention): {}", permanentFailures);

            // Log circuit breaker final status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

        } catch (SQLException e) {
            logger.error("Database error during API key validation", e);
            throw e;
        }
    }

    private static ValidationResult validateApiKeyRobust(ApiKeyInfo apiKeyInfo, Connection connection) throws SQLException {
        String apiKey = apiKeyInfo.getApiKey();
        String factionId = apiKeyInfo.getFactionId();
        String maskedKey = maskApiKey(apiKey);

        if (apiKey == null || apiKey.trim().isEmpty()) {
            logger.warn("Skipping empty or null API key for faction {}", factionId);
            return ValidationResult.permanentFailure("Empty or null API key");
        }

        logger.debug("Validating API key: {} for faction: {}", maskedKey, factionId);

        // Use the robust API handler
        ApiResponse response = TornApiHandler.executeRequest(Constants.API_URL_VALIDATE_KEY, apiKey);

        // Handle different response types
        ValidationResult result = processApiResponse(response, maskedKey, factionId);

        // Update database based on result
        updateApiKeyStatusFromResult(apiKey, factionId, result, connection);

        return result;
    }

    private static ValidationResult processApiResponse(ApiResponse response, String maskedKey, String factionId) {
        switch (response.getType()) {
            case SUCCESS:
                logger.info("✓ API key validation successful for key: {} and faction: {}", maskedKey, factionId);
                return ValidationResult.success();

            case AUTHENTICATION_ERROR:
                logger.error("✗ API key {} for faction {} is invalid or expired", maskedKey, factionId);
                return ValidationResult.permanentFailure("Invalid or expired API key");

            case AUTHORIZATION_ERROR:
                logger.error("✗ API key {} for faction {} lacks required permissions", maskedKey, factionId);
                return ValidationResult.permanentFailure("Insufficient permissions");

            case RATE_LIMITED:
                logger.warn("⚠ Rate limited for key {} and faction {} - will retry later", maskedKey, factionId);
                return ValidationResult.temporaryFailure("Rate limited");

            case SERVER_ERROR:
                logger.warn("⚠ Torn API server error for key {} and faction {} - will retry later", maskedKey, factionId);
                return ValidationResult.temporaryFailure("Server error");

            case NETWORK_ERROR:
                logger.warn("⚠ Network error for key {} and faction {} - will retry later", maskedKey, factionId);
                return ValidationResult.temporaryFailure("Network error");

            case CIRCUIT_BREAKER_OPEN:
                logger.error("⚠ Circuit breaker is open - stopping validation for key {} and faction {}", maskedKey, factionId);
                return ValidationResult.circuitBreakerOpen();

            case MAX_RETRIES_EXCEEDED:
                logger.error("✗ Max retries exceeded for key {} and faction {}", maskedKey, factionId);
                return ValidationResult.temporaryFailure("Max retries exceeded");

            case INTERRUPTED:
                logger.warn("⚠ Validation interrupted for key {} and faction {}", maskedKey, factionId);
                return ValidationResult.temporaryFailure("Operation interrupted");

            default:
                logger.error("✗ Unknown error for key {} and faction {}: {}",
                        maskedKey, factionId, response.getErrorMessage());
                return ValidationResult.temporaryFailure("Unknown error: " + response.getErrorMessage());
        }
    }

    private static void updateApiKeyStatusFromResult(String apiKey, String factionId, ValidationResult result, Connection connection) throws SQLException {
        // Only update status for definitive results
        if (result.getStatus() == ValidationResult.Status.SUCCESS) {
            updateApiKeyStatus(apiKey, factionId, true, connection);
        } else if (result.getStatus() == ValidationResult.Status.PERMANENT_FAILURE) {
            updateApiKeyStatus(apiKey, factionId, false, connection);
        }
        // For temporary failures and circuit breaker, don't change the status
        // This allows the key to be retried on the next run
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

    private static void updateApiKeyStatus(String apiKey, String factionId, boolean active, Connection connection) throws SQLException {
        if (!isValidIdentifier(Constants.COLUMN_NAME_ACTIVE)) {
            throw new IllegalArgumentException("Invalid active column name");
        }

        String sql = "UPDATE " + Constants.TABLE_NAME_API_KEYS +
                " SET " + Constants.COLUMN_NAME_ACTIVE + " = ? " +
                " WHERE " + Constants.COLUMN_NAME_API_KEY + " = ? " +
                " AND " + Constants.COLUMN_NAME_FACTION_ID + " = ?::bigint"; // Cast to bigint

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setBoolean(1, active);
            pstmt.setString(2, apiKey);
            pstmt.setString(3, factionId); // Still pass as string, but SQL will cast it

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

    // Helper class for validation results
    private static class ValidationResult {
        public enum Status {
            SUCCESS,
            TEMPORARY_FAILURE,
            PERMANENT_FAILURE,
            CIRCUIT_BREAKER_OPEN
        }

        private final Status status;
        private final String message;

        private ValidationResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static ValidationResult success() {
            return new ValidationResult(Status.SUCCESS, "Valid API key");
        }

        public static ValidationResult temporaryFailure(String message) {
            return new ValidationResult(Status.TEMPORARY_FAILURE, message);
        }

        public static ValidationResult permanentFailure(String message) {
            return new ValidationResult(Status.PERMANENT_FAILURE, message);
        }

        public static ValidationResult circuitBreakerOpen() {
            return new ValidationResult(Status.CIRCUIT_BREAKER_OPEN, "Circuit breaker is open");
        }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
    }
}