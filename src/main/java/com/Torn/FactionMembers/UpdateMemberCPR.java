package com.Torn.FactionMembers;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateMemberCPR {

    private static final Logger logger = LoggerFactory.getLogger(UpdateMemberCPR.class);

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;

        public FactionInfo(String factionId, String dbSuffix) {
            this.factionId = factionId;
            this.dbSuffix = dbSuffix;
        }

        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
    }

    public static class FactionMember {
        private final String userId;
        private final String username;
        private final String factionId;

        public FactionMember(String userId, String username, String factionId) {
            this.userId = userId;
            this.username = username;
            this.factionId = factionId;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getFactionId() { return factionId; }
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
                    .collect(Collectors.joining(""));
        }
    }

    /**
     * Main entry point for updating CPR tables for all factions
     */
    public static void updateAllFactionsCPR() throws SQLException {
        logger.info("Starting CPR table updates for all factions");

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

            // Get all factions
            List<FactionInfo> factions = getFactionInfo(configConnection);
            if (factions.isEmpty()) {
                logger.warn("No factions found to process CPR for");
                return;
            }

            // Get all crime slot information
            List<CrimeSlotInfo> crimeSlots = getCrimeSlotInfo(configConnection);
            if (crimeSlots.isEmpty()) {
                logger.warn("No crime slot information found");
                return;
            }

            logger.info("Found {} factions and {} crime slots to process", factions.size(), crimeSlots.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing CPR for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Process CPR for this faction
                    boolean success = updateFactionCPR(configConnection, ocDataConnection, factionInfo, crimeSlots);

                    if (success) {
                        logger.info("✓ Successfully updated CPR for faction {}", factionInfo.getFactionId());
                        successfulCount++;
                    } else {
                        logger.error("✗ Failed to update CPR for faction {}", factionInfo.getFactionId());
                        failedCount++;
                    }

                    processedCount++;

                } catch (Exception e) {
                    logger.error("✗ Unexpected error updating CPR for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("CPR update completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);

        } catch (SQLException e) {
            logger.error("Database error during CPR update", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database
     */
    private static List<FactionInfo> getFactionInfo(Connection configConnection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT " + Constants.COLUMN_NAME_FACTION_ID + ", " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE oc2_enabled = true";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);

                if (factionId != null && dbSuffix != null && isValidDbSuffix(dbSuffix)) {
                    factions.add(new FactionInfo(factionId, dbSuffix));
                } else {
                    logger.warn("Skipping faction with invalid data: factionId={}, dbSuffix={}", factionId, dbSuffix);
                }
            }
        }

        logger.info("Found {} factions for CPR processing", factions.size());
        return factions;
    }

    /**
     * Get crime and slot information from the config database
     */
    private static List<CrimeSlotInfo> getCrimeSlotInfo(Connection configConnection) throws SQLException {
        List<CrimeSlotInfo> crimeSlots = new ArrayList<>();

        String sql = "SELECT c.crime_name, s.slot_1, s.slot_2, s.slot_3, s.slot_4, s.slot_5, s.slot_6 " +
                "FROM all_oc2_crimes c " +
                "JOIN all_oc2_crimes_slots s ON c.crime_name = s.crime_name " +
                "ORDER BY c.crime_name";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String crimeName = rs.getString("crime_name");

                // Process each slot (1-6)
                for (int i = 1; i <= 6; i++) {
                    String slotName = rs.getString("slot_" + i);
                    if (slotName != null && !slotName.trim().isEmpty()) {
                        crimeSlots.add(new CrimeSlotInfo(crimeName, slotName.trim()));
                    }
                }
            }
        }

        logger.info("Found {} crime-slot combinations", crimeSlots.size());
        return crimeSlots;
    }

    /**
     * Update CPR table for a single faction
     */
    private static boolean updateFactionCPR(Connection configConnection, Connection ocDataConnection,
                                            FactionInfo factionInfo, List<CrimeSlotInfo> crimeSlots) throws SQLException {
        try {
            // Get faction members
            List<FactionMember> members = getFactionMembers(configConnection, factionInfo);
            if (members.isEmpty()) {
                logger.warn("No members found for faction {}", factionInfo.getFactionId());
                return true; // Not an error, just no data
            }

            // Create CPR table
            String cprTableName = "cpr_" + factionInfo.getDbSuffix();
            createCPRTable(ocDataConnection, cprTableName, crimeSlots);

            // Populate CPR data
            populateCPRData(ocDataConnection, factionInfo, cprTableName, members, crimeSlots);

            logger.info("Successfully updated CPR table {} with {} members and {} crime slots",
                    cprTableName, members.size(), crimeSlots.size());
            return true;

        } catch (Exception e) {
            logger.error("Error updating CPR for faction {}: {}", factionInfo.getFactionId(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * Get faction members from the config database
     */
    private static List<FactionMember> getFactionMembers(Connection configConnection, FactionInfo factionInfo) throws SQLException {
        List<FactionMember> members = new ArrayList<>();
        String membersTableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionInfo.getDbSuffix();

        String sql = "SELECT " + Constants.COLUMN_NAME_USER_ID + ", " + Constants.COLUMN_NAME_USER_NAME + ", faction_id " +
                "FROM " + membersTableName + " " +
                "ORDER BY " + Constants.COLUMN_NAME_USER_NAME;

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String userId = rs.getString(Constants.COLUMN_NAME_USER_ID);
                String username = rs.getString(Constants.COLUMN_NAME_USER_NAME);
                String factionId = rs.getString("faction_id");

                if (userId != null && username != null) {
                    members.add(new FactionMember(userId, username, factionId));
                }
            }
        } catch (SQLException e) {
            // Table might not exist for this faction
            logger.warn("Could not load members for faction {} (table {} might not exist): {}",
                    factionInfo.getFactionId(), membersTableName, e.getMessage());
        }

        logger.debug("Found {} members for faction {}", members.size(), factionInfo.getFactionId());
        return members;
    }

    /**
     * Create CPR table with dynamic columns based on crime slots
     */
    private static void createCPRTable(Connection ocDataConnection, String tableName, List<CrimeSlotInfo> crimeSlots) throws SQLException {
        // Drop table if it exists to recreate with new structure
        String dropSql = "DROP TABLE IF EXISTS " + tableName;

        // Build CREATE TABLE statement with dynamic columns
        StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE ").append(tableName).append(" (");
        createSql.append("user_id VARCHAR(20) NOT NULL,");
        createSql.append("username VARCHAR(100) NOT NULL,");
        createSql.append("faction_id BIGINT NOT NULL,");

        // Add column for each crime-slot combination
        for (CrimeSlotInfo crimeSlot : crimeSlots) {
            String columnName = sanitizeColumnName(crimeSlot.getColumnName());
            createSql.append("\"").append(columnName).append("\" INTEGER DEFAULT 0,");
        }

        createSql.append("last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,");
        createSql.append("PRIMARY KEY (user_id)");
        createSql.append(")");

        try (Statement stmt = ocDataConnection.createStatement()) {
            // Drop existing table
            stmt.execute(dropSql);
            logger.debug("Dropped existing table {}", tableName);

            // Create new table
            stmt.execute(createSql.toString());
            logger.debug("Created CPR table {} with {} crime-slot columns", tableName, crimeSlots.size());

            // Create indexes
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_username ON " + tableName + "(username)");
        }
    }

    /**
     * Populate CPR data for all members and crime slots
     */
    private static void populateCPRData(Connection ocDataConnection, FactionInfo factionInfo, String cprTableName,
                                        List<FactionMember> members, List<CrimeSlotInfo> crimeSlots) throws SQLException {

        String completedCrimesTableName = Constants.TABLE_NAME_COMPLETED_CRIMES + factionInfo.getDbSuffix();

        // Insert members first
        insertMembersIntoCPRTable(ocDataConnection, cprTableName, members);

        // Update CPR values for each crime-slot combination
        for (CrimeSlotInfo crimeSlot : crimeSlots) {
            updateCPRForCrimeSlot(ocDataConnection, cprTableName, completedCrimesTableName, crimeSlot);
        }

        logger.debug("Populated CPR data for {} members and {} crime slots in table {}",
                members.size(), crimeSlots.size(), cprTableName);
    }

    /**
     * Insert all members into CPR table with default values
     */
    private static void insertMembersIntoCPRTable(Connection ocDataConnection, String cprTableName,
                                                  List<FactionMember> members) throws SQLException {
        if (members.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO " + cprTableName + " (user_id, username, faction_id) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            for (FactionMember member : members) {
                pstmt.setString(1, member.getUserId());
                pstmt.setString(2, member.getUsername());
                pstmt.setLong(3, Long.parseLong(member.getFactionId()));
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.debug("Inserted {} members into CPR table {}", results.length, cprTableName);
        }
    }

    /**
     * Update CPR values for a specific crime-slot combination
     */
    private static void updateCPRForCrimeSlot(Connection ocDataConnection, String cprTableName,
                                              String completedCrimesTableName, CrimeSlotInfo crimeSlot) throws SQLException {
        String columnName = sanitizeColumnName(crimeSlot.getColumnName());

        // Query to get max CPR for each user for this crime-slot combination
        String selectSql = "SELECT user_id, MAX(checkpoint_pass_rate) as max_cpr " +
                "FROM " + completedCrimesTableName + " " +
                "WHERE crime_name = ? AND role = ? AND checkpoint_pass_rate IS NOT NULL " +
                "GROUP BY user_id";

        // Update statement
        String updateSql = "UPDATE " + cprTableName + " SET \"" + columnName + "\" = ? WHERE user_id = ?";

        try (PreparedStatement selectPstmt = ocDataConnection.prepareStatement(selectSql);
             PreparedStatement updatePstmt = ocDataConnection.prepareStatement(updateSql)) {

            // Get CPR data for this crime-slot combination
            selectPstmt.setString(1, crimeSlot.getCrimeName());
            selectPstmt.setString(2, crimeSlot.getSlotName());

            Map<String, Integer> userCPRMap = new HashMap<>();
            try (ResultSet rs = selectPstmt.executeQuery()) {
                while (rs.next()) {
                    String userId = rs.getString("user_id");
                    int maxCPR = rs.getInt("max_cpr");
                    userCPRMap.put(userId, maxCPR);
                }
            }

            // Update CPR table with found values (others remain 0)
            for (Map.Entry<String, Integer> entry : userCPRMap.entrySet()) {
                updatePstmt.setInt(1, entry.getValue());
                updatePstmt.setString(2, entry.getKey());
                updatePstmt.addBatch();
            }

            if (!userCPRMap.isEmpty()) {
                updatePstmt.executeBatch();
                logger.debug("Updated CPR for {} users in crime-slot: {}", userCPRMap.size(), crimeSlot.getColumnName());
            }

        } catch (SQLException e) {
            // Table might not exist for this faction
            logger.debug("Could not update CPR for crime-slot {} (table {} might not exist): {}",
                    crimeSlot.getColumnName(), completedCrimesTableName, e.getMessage());
        }
    }

    /**
     * Sanitize column name for PostgreSQL compatibility
     */
    private static String sanitizeColumnName(String columnName) {
        if (columnName == null) {
            return "unknown";
        }

        // Replace problematic characters and limit length
        String sanitized = columnName
                .replaceAll("[^a-zA-Z0-9\\s\\-_#]", "") // Keep only alphanumeric, spaces, hyphens, underscores, hash
                .replaceAll("\\s+", "_") // Replace spaces with underscores
                .toLowerCase();

        // Ensure it doesn't start with a number
        if (sanitized.matches("^\\d.*")) {
            sanitized = "col_" + sanitized;
        }

        // Limit length (PostgreSQL limit is 63 characters)
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
}