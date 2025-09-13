package com.Torn.FactionCrimes.Completed;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class GetTornStatsCPR {

    private static final Logger logger = LoggerFactory.getLogger(GetTornStatsCPR.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TORN_STATS_API_RATE_LIMIT_MS = 2000;

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;
        private final String apiKey;

        public FactionInfo(String factionId, String dbSuffix, String apiKey) {
            this.factionId = factionId;
            this.dbSuffix = dbSuffix;
            this.apiKey = apiKey;
        }

        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
        public String getApiKey() { return apiKey; }
    }

    public static class TornStatsCPRData {
        private final String userId;
        private final String crimeName;
        private final String slotName;
        private final int cprValue;

        public TornStatsCPRData(String userId, String crimeName, String slotName, int cprValue) {
            this.userId = userId;
            this.crimeName = crimeName;
            this.slotName = slotName;
            this.cprValue = cprValue;
        }

        public String getUserId() { return userId; }
        public String getCrimeName() { return crimeName; }
        public String getSlotName() { return slotName; }
        public int getCprValue() { return cprValue; }

        public String getCrimeSlotKey() { return crimeName + "|" + slotName; }
    }

    public static class CrimeSlotInfo {
        private final String crimeName;
        private final String crimeAbbreviation;
        private final String slotName;
        private final String columnName;

        public CrimeSlotInfo(String crimeName, String slotName) {
            this.crimeName = crimeName;
            this.crimeAbbreviation = createCrimeAbbreviation(crimeName);
            this.slotName = slotName;
            this.columnName = this.crimeAbbreviation + " - " + slotName;
        }

        public String getCrimeName() { return crimeName; }
        public String getCrimeAbbreviation() { return crimeAbbreviation; }
        public String getSlotName() { return slotName; }
        public String getColumnName() { return columnName; }

        private static String createCrimeAbbreviation(String crimeName) {
            if (crimeName == null || crimeName.trim().isEmpty()) {
                return "UNK";
            }

            return Arrays.stream(crimeName.split("\\s+"))
                    .filter(word -> !word.isEmpty())
                    .map(word -> word.substring(0, 1).toUpperCase())
                    .collect(java.util.stream.Collectors.joining(""));
        }
    }

    /**
     * Main entry point for updating CPR tables with TornStats data for all factions
     */
    public static void updateAllFactionsCPRFromTornStats() throws SQLException, IOException {
        logger.info("Starting TornStats CPR data fetch for all factions");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to databases...");
        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("Database connections established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping TornStats CPR fetch to prevent further failures");
                return;
            }

            // Get all factions with TornStats API keys
            List<FactionInfo> factions = getFactionInfoWithTornStatsKeys(configConnection);
            if (factions.isEmpty()) {
                logger.warn("No factions with TornStats API keys found");
                return;
            }

            // Get crime slot information for column mapping
            List<CrimeSlotInfo> crimeSlots = getCrimeSlotInfo(configConnection);
            if (crimeSlots.isEmpty()) {
                logger.warn("No crime slot information found - cannot map TornStats data");
                return;
            }

            logger.info("Found {} factions with TornStats keys and {} crime slots to process",
                    factions.size(), crimeSlots.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalUpdatesApplied = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing TornStats CPR for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Fetch and process CPR data for this faction
                    TornStatsResult result = fetchAndUpdateTornStatsCPR(ocDataConnection, factionInfo, crimeSlots);

                    if (result.isSuccess()) {
                        logger.info("✓ Successfully processed TornStats CPR for faction {} - {} updates applied",
                                factionInfo.getFactionId(), result.getUpdatesApplied());
                        successfulCount++;
                        totalUpdatesApplied += result.getUpdatesApplied();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("✗ Failed to process TornStats CPR for faction {}: {}",
                                factionInfo.getFactionId(), result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                    // Rate limiting between factions (except for the last one)
                    if (processedCount < factions.size()) {
                        logger.debug("Waiting {}ms before processing next faction", TORN_STATS_API_RATE_LIMIT_MS);
                        Thread.sleep(TORN_STATS_API_RATE_LIMIT_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("TornStats CPR processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("✗ Unexpected error processing TornStats CPR for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("TornStats CPR update completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total CPR updates applied: {}", totalUpdatesApplied);

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

        } catch (SQLException e) {
            logger.error("Database error during TornStats CPR update", e);
            throw e;
        }
    }

    /**
     * Get faction information with API keys from the standard api_keys table pattern
     */
    private static List<FactionInfo> getFactionInfoWithTornStatsKeys(Connection configConnection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        // Use the same pattern as all other classes - join factions with api_keys table
        String sql = "SELECT DISTINCT ON (f." + Constants.COLUMN_NAME_FACTION_ID + ") " +
                "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                "f." + Constants.COLUMN_NAME_DB_SUFFIX + ", " +
                "ak." + Constants.COLUMN_NAME_API_KEY + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                "AND f.oc2_enabled = true " +
                "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);

                if (factionId != null && dbSuffix != null && apiKey != null &&
                        isValidDbSuffix(dbSuffix)) {
                    factions.add(new FactionInfo(factionId, dbSuffix, apiKey));
                } else {
                    logger.warn("Skipping faction with invalid data: factionId={}, dbSuffix={}, apiKey={}",
                            factionId, dbSuffix, (apiKey == null ? "null" : "***"));
                }
            }
        }

        logger.info("Found {} factions with API keys for TornStats CPR update", factions.size());
        return factions;
    }

    /**
     * Get crime and slot information from the config database for column mapping
     */
    private static List<CrimeSlotInfo> getCrimeSlotInfo(Connection configConnection) throws SQLException {
        List<CrimeSlotInfo> crimeSlots = new ArrayList<>();

        String sql = "SELECT c.crime_name, s.slot_1, s.slot_2, s.slot_3, s.slot_4, s.slot_5, s.slot_6 " +
                "FROM " + Constants.TABLE_NAME_OC2_CRIMES + " c " +
                "JOIN " + Constants.TABLE_NAME_OC2_CRIMES_SLOTS + " s ON c.crime_name = s.crime_name " +
                "ORDER BY c.crime_name";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String crimeName = rs.getString("crime_name");

                for (int i = 1; i <= 6; i++) {
                    String slotName = rs.getString("slot_" + i);
                    if (slotName != null && !slotName.trim().isEmpty()) {
                        crimeSlots.add(new CrimeSlotInfo(crimeName, slotName.trim()));
                    }
                }
            }
        }

        logger.info("Found {} crime-slot combinations for TornStats mapping", crimeSlots.size());
        return crimeSlots;
    }

    /**
     * Fetch TornStats CPR data and update the CPR table for a single faction
     */
    private static TornStatsResult fetchAndUpdateTornStatsCPR(Connection ocDataConnection, FactionInfo factionInfo,
                                                              List<CrimeSlotInfo> crimeSlots) {
        try {
            String cprTableName = Constants.TABLE_NAME_CPR + factionInfo.getDbSuffix();

            // Check if CPR table exists
            if (!doesCPRTableExist(ocDataConnection, cprTableName)) {
                logger.warn("CPR table {} does not exist for faction {} - skipping TornStats update",
                        cprTableName, factionInfo.getFactionId());
                return TornStatsResult.success(0); // Not an error, just no table to update
            }

            // Fetch TornStats data
            List<TornStatsCPRData> tornStatsData = fetchTornStatsCPRData(factionInfo);
            if (tornStatsData.isEmpty()) {
                logger.debug("No TornStats CPR data found for faction {}", factionInfo.getFactionId());
                return TornStatsResult.success(0);
            }

            logger.info("Fetched {} CPR records from TornStats for faction {}",
                    tornStatsData.size(), factionInfo.getFactionId());

            // Update CPR table with TornStats data
            int updatesApplied = updateCPRTableWithTornStatsData(ocDataConnection, cprTableName,
                    tornStatsData, crimeSlots, factionInfo);

            logger.info("Applied {} CPR updates from TornStats for faction {}",
                    updatesApplied, factionInfo.getFactionId());
            return TornStatsResult.success(updatesApplied);

        } catch (Exception e) {
            logger.error("Exception processing TornStats CPR for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return TornStatsResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Check if CPR table exists for the faction
     */
    private static boolean doesCPRTableExist(Connection ocDataConnection, String tableName) throws SQLException {
        String sql = "SELECT EXISTS (" +
                "SELECT 1 FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name = ?" +
                ")";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            pstmt.setString(1, tableName.toLowerCase());
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * Fetch CPR data from TornStats API using the existing Torn API key
     */
    private static List<TornStatsCPRData> fetchTornStatsCPRData(FactionInfo factionInfo) throws IOException {
        String apiUrl = "https://www.tornstats.com/api/v2/" + factionInfo.getApiKey() + "/faction/cpr";

        logger.debug("Fetching TornStats CPR data for faction: {}", factionInfo.getFactionId());

        // Use robust API handler (it should work with external APIs too)
        ApiResponse response = TornApiHandler.executeRequest(apiUrl, null); // No API key needed in header for TornStats

        if (response.isSuccess()) {
            logger.info("Successfully fetched TornStats CPR data for faction {}", factionInfo.getFactionId());
            return parseTornStatsCPRResponse(response.getBody(), factionInfo);

        } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
            logger.error("Circuit breaker is open - skipping TornStats fetch for faction {}",
                    factionInfo.getFactionId());
            throw new IOException("Circuit breaker open - API calls suspended");

        } else if (response.isAuthenticationIssue()) {
            logger.error("TornStats API key authentication issue for faction {}: {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("TornStats API key authentication failed: " + response.getErrorMessage());

        } else {
            logger.error("Failed to fetch TornStats CPR data for faction {}: {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("TornStats API error: " + response.getErrorMessage());
        }
    }

    /**
     * Parse TornStats CPR response
     */
    private static List<TornStatsCPRData> parseTornStatsCPRResponse(String responseBody, FactionInfo factionInfo) throws IOException {
        List<TornStatsCPRData> cprData = new ArrayList<>();

        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error
            if (jsonResponse.has("status") && !jsonResponse.get("status").asBoolean()) {
                String errorMessage = jsonResponse.has("message") ? jsonResponse.get("message").asText() : "Unknown error";
                logger.error("TornStats API Error for faction {}: {}", factionInfo.getFactionId(), errorMessage);

                if ("ERROR: User not found.".equals(errorMessage)) {
                    logger.warn("TornStats API key for faction {} appears to be invalid", factionInfo.getFactionId());
                }

                throw new IOException("TornStats API Error: " + errorMessage);
            }

            // Parse members data
            JsonNode membersNode = jsonResponse.get("members");
            if (membersNode == null || !membersNode.isObject()) {
                logger.warn("No members data found in TornStats response for faction {}", factionInfo.getFactionId());
                return cprData;
            }

            // Iterate through each user
            membersNode.fieldNames().forEachRemaining(userId -> {
                JsonNode userCrimes = membersNode.get(userId);
                if (userCrimes != null && userCrimes.isObject()) {

                    // Iterate through each crime for this user
                    userCrimes.fieldNames().forEachRemaining(crimeName -> {
                        JsonNode crimeSlots = userCrimes.get(crimeName);
                        if (crimeSlots != null && crimeSlots.isObject()) {

                            // Iterate through each slot for this crime
                            crimeSlots.fieldNames().forEachRemaining(slotName -> {
                                JsonNode cprValueNode = crimeSlots.get(slotName);
                                if (cprValueNode != null && cprValueNode.isInt()) {
                                    int cprValue = cprValueNode.asInt();
                                    cprData.add(new TornStatsCPRData(userId, crimeName, slotName, cprValue));

                                    logger.debug("Parsed TornStats CPR: User {} - {} {} = {}",
                                            userId, crimeName, slotName, cprValue);
                                }
                            });
                        }
                    });
                }
            });

        } catch (Exception e) {
            logger.error("Error parsing TornStats CPR response for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage());
            throw new IOException("Failed to parse TornStats CPR response", e);
        }

        logger.info("Parsed {} CPR records from TornStats for faction {}",
                cprData.size(), factionInfo.getFactionId());
        return cprData;
    }

    /**
     * Update CPR table with TornStats data, only updating if the new value is higher
     */
    private static int updateCPRTableWithTornStatsData(Connection ocDataConnection, String cprTableName,
                                                       List<TornStatsCPRData> tornStatsData,
                                                       List<CrimeSlotInfo> crimeSlots,
                                                       FactionInfo factionInfo) throws SQLException {

        if (tornStatsData.isEmpty()) {
            return 0;
        }

        // Create mapping from crime-slot combinations to column names
        Map<String, String> crimeSlotToColumn = new HashMap<>();
        for (CrimeSlotInfo slot : crimeSlots) {
            String key = slot.getCrimeName() + "|" + slot.getSlotName();
            String columnName = sanitizeColumnName(slot.getColumnName());
            crimeSlotToColumn.put(key, columnName);

            logger.debug("Crime-slot mapping: '{}' -> column '{}'", key, columnName);
        }

        // Group TornStats data by user for batch processing
        Map<String, List<TornStatsCPRData>> userDataMap = new HashMap<>();
        for (TornStatsCPRData data : tornStatsData) {
            userDataMap.computeIfAbsent(data.getUserId(), k -> new ArrayList<>()).add(data);
        }

        int totalUpdatesApplied = 0;
        int usersNotFound = 0;
        int unmappedSlots = 0;

        ocDataConnection.setAutoCommit(false);
        try {
            for (Map.Entry<String, List<TornStatsCPRData>> entry : userDataMap.entrySet()) {
                String userId = entry.getKey();
                List<TornStatsCPRData> userCPRData = entry.getValue();

                // Check if user exists in CPR table
                if (!doesUserExistInCPRTable(ocDataConnection, cprTableName, userId)) {
                    usersNotFound++;
                    logger.debug("User {} not found in CPR table {} - skipping (probably left faction)",
                            userId, cprTableName);
                    continue;
                }

                // Process each CPR entry for this user
                for (TornStatsCPRData cprData : userCPRData) {
                    String crimeSlotKey = cprData.getCrimeSlotKey();
                    String columnName = crimeSlotToColumn.get(crimeSlotKey);

                    if (columnName == null) {
                        unmappedSlots++;
                        logger.debug("No column mapping found for crime-slot: '{}' (user: {})",
                                crimeSlotKey, userId);
                        continue;
                    }

                    // Update CPR value only if new value is higher
                    boolean updated = updateCPRValueIfHigher(ocDataConnection, cprTableName, userId,
                            columnName, cprData.getCprValue());
                    if (updated) {
                        totalUpdatesApplied++;
                        logger.debug("Updated CPR for user {} - {} {} = {} (TornStats)",
                                userId, cprData.getCrimeName(), cprData.getSlotName(), cprData.getCprValue());
                    }
                }
            }

            ocDataConnection.commit();

            logger.info("TornStats CPR update summary for faction {}:", factionInfo.getFactionId());
            logger.info("  Total updates applied: {}", totalUpdatesApplied);
            logger.info("  Users not found in CPR table: {}", usersNotFound);
            logger.info("  Unmapped crime-slots: {}", unmappedSlots);

        } catch (SQLException e) {
            ocDataConnection.rollback();
            logger.error("Failed to update CPR table {} with TornStats data, rolling back", cprTableName, e);
            throw e;
        } finally {
            ocDataConnection.setAutoCommit(true);
        }

        return totalUpdatesApplied;
    }

    /**
     * Check if user exists in CPR table
     */
    private static boolean doesUserExistInCPRTable(Connection ocDataConnection, String tableName, String userId) throws SQLException {
        String sql = "SELECT 1 FROM " + tableName + " WHERE user_id = ? LIMIT 1";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Update CPR value only if the new value is higher than the existing value
     */
    private static boolean updateCPRValueIfHigher(Connection ocDataConnection, String tableName, String userId,
                                                  String columnName, int newValue) throws SQLException {
        // First check the current value
        String selectSql = "SELECT \"" + columnName + "\" FROM " + tableName + " WHERE user_id = ?";

        try (PreparedStatement selectPstmt = ocDataConnection.prepareStatement(selectSql)) {
            selectPstmt.setString(1, userId);
            try (ResultSet rs = selectPstmt.executeQuery()) {
                if (rs.next()) {
                    Integer currentValue = rs.getObject(1, Integer.class);

                    // If current value is null or new value is higher, update
                    if (currentValue == null || newValue > currentValue) {
                        String updateSql = "UPDATE " + tableName + " SET \"" + columnName + "\" = ? WHERE user_id = ?";

                        try (PreparedStatement updatePstmt = ocDataConnection.prepareStatement(updateSql)) {
                            updatePstmt.setInt(1, newValue);
                            updatePstmt.setString(2, userId);

                            int rowsAffected = updatePstmt.executeUpdate();
                            if (rowsAffected > 0) {
                                logger.debug("Updated {} for user {} from {} to {}",
                                        columnName, userId, currentValue, newValue);
                                return true;
                            }
                        }
                    } else {
                        logger.debug("Skipping update for {} user {} - existing value {} >= new value {}",
                                columnName, userId, currentValue, newValue);
                    }
                }
            }
        }

        return false;
    }

    /**
     * Sanitize column name for PostgreSQL compatibility (same as UpdateMemberCPR)
     */
    private static String sanitizeColumnName(String columnName) {
        if (columnName == null) {
            return "unknown";
        }

        String sanitized = columnName
                .replaceAll("[^a-zA-Z0-9\\s\\-_#]", "")
                .replaceAll("\\s+", "_")
                .toLowerCase();

        if (sanitized.matches("^\\d.*")) {
            sanitized = "col_" + sanitized;
        }

        if (sanitized.length() > 60) {
            sanitized = sanitized.substring(0, 60);
        }

        return sanitized;
    }

    /**
     * Validate db suffix for SQL injection prevention
     */
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }

    /**
     * Result wrapper for TornStats processing
     */
    private static class TornStatsResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int updatesApplied;
        private final String errorMessage;

        private TornStatsResult(boolean success, boolean circuitBreakerOpen, int updatesApplied, String errorMessage) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.updatesApplied = updatesApplied;
            this.errorMessage = errorMessage;
        }

        public static TornStatsResult success(int updatesApplied) {
            return new TornStatsResult(true, false, updatesApplied, null);
        }

        public static TornStatsResult failure(String errorMessage) {
            return new TornStatsResult(false, false, 0, errorMessage);
        }

        public static TornStatsResult circuitBreakerOpen() {
            return new TornStatsResult(false, true, 0, "Circuit breaker is open");
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getUpdatesApplied() { return updatesApplied; }
        public String getErrorMessage() { return errorMessage; }
    }
}