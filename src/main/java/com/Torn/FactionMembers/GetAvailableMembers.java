package com.Torn.FactionMembers;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class GetAvailableMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetAvailableMembers.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TORN_API_RATE_LIMIT_MS = 2000;

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;
        private final String apiKey;
        private final String ownerName;

        public FactionInfo(String factionId, String dbSuffix, String apiKey, String ownerName) {
            this.factionId = factionId;
            this.dbSuffix = dbSuffix;
            this.apiKey = apiKey;
            this.ownerName = ownerName;
        }

        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
        public String getApiKey() { return apiKey; }
        public String getOwnerName() { return ownerName; }
    }

    public static class AvailableMember {
        private final String userId;
        private final String username;
        private final boolean isInOC;
        private final String lastAction;

        public AvailableMember(String userId, String username, boolean isInOC, String status, Integer level, String lastAction) {
            this.userId = userId;
            this.username = username;
            this.isInOC = isInOC;
            this.lastAction = lastAction;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public boolean isInOC() { return isInOC; }
        public String getLastAction() { return lastAction; }

        @Override
        public String toString() {
            return "AvailableMember{" +
                    "userId='" + userId + '\'' +
                    ", username='" + username + '\'' +
                    ", isInOC=" + isInOC +
                    '}';
        }
    }

    /**
     * Main entry point for fetching available members for all factions
     */
    public static void fetchAndProcessAllAvailableMembers() throws SQLException, IOException {
        logger.info("Starting available members fetch for all factions with robust API handling");

        String databaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to members database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping members fetch to prevent further failures");
                return;
            }

            // Get faction information from config database
            List<FactionInfo> factions = getFactionInfo();
            if (factions.isEmpty()) {
                logger.warn("No active factions found to process members for");
                return;
            }

            logger.info("Found {} active factions to process members for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalMembersProcessed = 0;
            int totalAvailableMembers = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing available members for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Fetch members for this faction
                    MembersProcessingResult result = fetchAndStoreMembersForFaction(connection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("✓ Successfully processed {} members ({} available for OC) for faction {} ({})",
                                result.getMembersProcessed(), result.getAvailableMembersCount(),
                                factionInfo.getFactionId(), factionInfo.getOwnerName());
                        successfulCount++;
                        totalMembersProcessed += result.getMembersProcessed();
                        totalAvailableMembers += result.getAvailableMembersCount();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("✗ Failed to process members for faction {} ({}): {}",
                                factionInfo.getFactionId(), factionInfo.getOwnerName(),
                                result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                    // Rate limiting between factions (except for the last one)
                    if (processedCount < factions.size()) {
                        logger.debug("Waiting {}ms before processing next faction", TORN_API_RATE_LIMIT_MS);
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("Members processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("✗ Unexpected error processing members for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("Available members processing completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total members processed: {}", totalMembersProcessed);
            logger.info("  Total available for OC: {}", totalAvailableMembers);

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

            // Warn if many factions failed
            if (failedCount > successfulCount && processedCount > 2) {
                logger.error("⚠ More than half of factions failed - Torn API may be experiencing issues");
            }

        } catch (SQLException e) {
            logger.error("Database error during members processing", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database (OC2 enabled factions only)
     */
    private static List<FactionInfo> getFactionInfo() throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger)) {
            String sql = "SELECT DISTINCT ON (f." + Constants.COLUMN_NAME_FACTION_ID + ") " +
                    "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                    "f." + Constants.COLUMN_NAME_DB_SUFFIX + ", " +
                    "ak." + Constants.COLUMN_NAME_API_KEY + ", " +
                    "ak." + Constants.COLUMN_NAME_OWNER_NAME + " " +
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
                    String ownerName = rs.getString(Constants.COLUMN_NAME_OWNER_NAME);

                    // Validate data
                    if (factionId == null || dbSuffix == null || apiKey == null) {
                        logger.warn("Skipping faction with null data: factionId={}, dbSuffix={}, apiKey={}",
                                factionId, dbSuffix, (apiKey == null ? "null" : "***"));
                        continue;
                    }

                    // Validate dbSuffix for SQL injection prevention
                    if (!isValidDbSuffix(dbSuffix)) {
                        logger.error("Invalid db_suffix for faction {}: {}", factionId, dbSuffix);
                        continue;
                    }

                    factions.add(new FactionInfo(factionId, dbSuffix, apiKey, ownerName));
                }
            }
        }

        logger.info("Found {} OC2-enabled factions for members processing", factions.size());
        return factions;
    }

    /**
     * Fetch and store available members for a single faction
     */
    private static MembersProcessingResult fetchAndStoreMembersForFaction(Connection connection, FactionInfo factionInfo) {
        try {
            // Construct API URL for this faction's members
            String apiUrl = Constants.API_URL_FACTION + factionInfo.getFactionId() +
                    Constants.API_URL_FACTION_MEMBERS + factionInfo.getApiKey();

            logger.debug("Fetching members for faction: {}", factionInfo.getFactionId());

            // Use robust API handler
            ApiResponse response = TornApiHandler.executeRequest(apiUrl, factionInfo.getApiKey());

            // Handle different response types
            if (response.isSuccess()) {
                logger.info("✓ Successfully fetched members data for faction {}", factionInfo.getFactionId());
                return processMembersResponse(connection, factionInfo, response.getBody());

            } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
                logger.error("Circuit breaker is open - skipping faction {} to prevent further API failures",
                        factionInfo.getFactionId());
                return MembersProcessingResult.circuitBreakerOpen();

            } else if (response.isAuthenticationIssue()) {
                logger.error("API key authentication issue for faction {}: {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return MembersProcessingResult.failure("API key authentication failed: " + response.getErrorMessage());

            } else if (response.isTemporaryError()) {
                logger.warn("Temporary API error for faction {} (will retry on next run): {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return MembersProcessingResult.failure("Temporary API error: " + response.getErrorMessage());

            } else {
                logger.error("Permanent API error for faction {}: {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return MembersProcessingResult.failure("API error: " + response.getErrorMessage());
            }

        } catch (Exception e) {
            logger.error("Exception processing members for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return MembersProcessingResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Process the members response and store in database
     */
    private static MembersProcessingResult processMembersResponse(Connection connection, FactionInfo factionInfo, String responseBody) {
        try {
            // Parse JSON response
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error in response
            if (jsonResponse.has("error")) {
                String errorMsg = jsonResponse.get("error").asText();
                logger.error("Torn API Error for faction {}: {}", factionInfo.getFactionId(), errorMsg);
                return MembersProcessingResult.failure("Torn API Error: " + errorMsg);
            }

            // Parse members - Torn API returns members as an ARRAY
            JsonNode membersNode = jsonResponse.get(Constants.MEMBERS);
            if (membersNode == null || !membersNode.isArray()) {
                logger.warn("No members array found in API response for faction {}", factionInfo.getFactionId());
                return MembersProcessingResult.success(0, 0);
            }

            // Create faction-specific table
            String tableName = "a_members_" + factionInfo.getDbSuffix();
            createMembersTableIfNotExists(connection, tableName);

            // Parse members from response
            List<AvailableMember> members = parseMembersFromResponse(membersNode, factionInfo);

            if (members.isEmpty()) {
                logger.info("No members found for faction {}", factionInfo.getFactionId());
                return MembersProcessingResult.success(0, 0);
            }

            // Count available members (not in OC)
            int availableCount = (int) members.stream().filter(m -> !m.isInOC()).count();

            // Store members in database
            connection.setAutoCommit(false);
            try {
                // Clear existing data for this faction
                clearExistingMembers(connection, tableName);

                // Insert all members
                insertMembers(connection, tableName, members, factionInfo);

                connection.commit();
                logger.info("Successfully stored {} members ({} available for OC) for faction {} in table {}",
                        members.size(), availableCount, factionInfo.getFactionId(), tableName);
                return MembersProcessingResult.success(members.size(), availableCount);

            } catch (SQLException e) {
                connection.rollback();
                logger.error("Failed to store members for faction {}, rolling back", factionInfo.getFactionId(), e);
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }

        } catch (Exception e) {
            logger.error("Error processing members response for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return MembersProcessingResult.failure("Response processing error: " + e.getMessage());
        }
    }

    /**
     * Parse members from JSON response
     */
    private static List<AvailableMember> parseMembersFromResponse(JsonNode membersNode, FactionInfo factionInfo) {
        List<AvailableMember> members = new ArrayList<>();

        for (JsonNode memberNode : membersNode) {
            try {
                JsonNode idNode = memberNode.get(Constants.ID);
                JsonNode nameNode = memberNode.get(Constants.NAME);
                JsonNode isInOcNode = memberNode.get("is_in_oc");
                JsonNode statusNode = memberNode.get("status");
                JsonNode levelNode = memberNode.get("level");
                JsonNode lastActionNode = memberNode.get("last_action");

                if (idNode != null && nameNode != null && isInOcNode != null) {
                    String userId = idNode.asText();
                    String username = nameNode.asText();
                    boolean isInOC = isInOcNode.asBoolean();
                    String status = statusNode != null ? statusNode.asText() : null;
                    Integer level = levelNode != null ? levelNode.asInt() : null;
                    String lastAction = lastActionNode != null ? lastActionNode.asText() : null;

                    members.add(new AvailableMember(userId, username, isInOC, status, level, lastAction));
                } else {
                    logger.warn("Missing required fields (id, name, or is_in_oc) in member data for faction {}",
                            factionInfo.getFactionId());
                }
            } catch (Exception e) {
                logger.warn("Error parsing member data for faction {}: {}", factionInfo.getFactionId(), e.getMessage());
            }
        }

        logger.info("Parsed {} members for faction {}", members.size(), factionInfo.getFactionId());
        return members;
    }

    /**
     * Create faction-specific members table if it doesn't exist
     */
    private static void createMembersTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "user_id VARCHAR(20) PRIMARY KEY," +
                "username VARCHAR(100) NOT NULL," +
                "faction_id BIGINT NOT NULL," +
                "is_in_oc BOOLEAN NOT NULL DEFAULT FALSE," +
                "last_action VARCHAR(100)," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better query performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_is_in_oc ON " + tableName + "(is_in_oc)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_last_updated ON " + tableName + "(last_updated)");

            logger.debug("Table {} created or verified with indexes", tableName);
        }
    }

    /**
     * Clear existing members data for the table
     */
    private static void clearExistingMembers(Connection connection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int deletedRows = pstmt.executeUpdate();
            logger.debug("Cleared {} existing members from {}", deletedRows, tableName);
        }
    }

    /**
     * Insert members into the database
     */
    private static void insertMembers(Connection connection, String tableName, List<AvailableMember> members, FactionInfo factionInfo) throws SQLException {
        if (members.isEmpty()) {
            logger.info("No members to insert into {}", tableName);
            return;
        }

        String insertSql = "INSERT INTO " + tableName + " (" +
                "user_id, username, faction_id, is_in_oc, last_action, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            // Convert factionId to Long
            long factionIdLong = Long.parseLong(factionInfo.getFactionId());

            for (AvailableMember member : members) {
                pstmt.setString(1, member.getUserId());
                pstmt.setString(2, member.getUsername());
                pstmt.setLong(3, factionIdLong);
                pstmt.setBoolean(4, member.isInOC());
                pstmt.setString(5, member.getLastAction());
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.debug("Inserted {} members into {}", results.length, tableName);
        }
    }

    // Utility methods
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                dbSuffix.length() >= 1 &&
                dbSuffix.length() <= 50;
    }

    /**
     * Result wrapper for members processing
     */
    private static class MembersProcessingResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int membersProcessed;
        private final int availableMembersCount;
        private final String errorMessage;

        private MembersProcessingResult(boolean success, boolean circuitBreakerOpen, int membersProcessed, int availableMembersCount, String errorMessage) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.membersProcessed = membersProcessed;
            this.availableMembersCount = availableMembersCount;
            this.errorMessage = errorMessage;
        }

        public static MembersProcessingResult success(int membersProcessed, int availableMembersCount) {
            return new MembersProcessingResult(true, false, membersProcessed, availableMembersCount, null);
        }

        public static MembersProcessingResult failure(String errorMessage) {
            return new MembersProcessingResult(false, false, 0, 0, errorMessage);
        }

        public static MembersProcessingResult circuitBreakerOpen() {
            return new MembersProcessingResult(false, true, 0, 0, "Circuit breaker is open");
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getMembersProcessed() { return membersProcessed; }
        public int getAvailableMembersCount() { return availableMembersCount; }
        public String getErrorMessage() { return errorMessage; }
    }
}