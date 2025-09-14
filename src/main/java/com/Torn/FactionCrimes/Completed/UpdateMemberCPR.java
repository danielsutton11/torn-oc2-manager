package com.Torn.FactionCrimes.Completed;

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

    /**
     * └── UpdateMemberCPR.java
     *     ├── Calculates Crime Pass Rate (CPR) for each member-role combination
     *     ├── Groups roles by base type (e.g., "Muscle #1", "Muscle #2" → "Muscle")
     *     ├── Shares CPR across role variants within same crime
     *     └── Creates faction-specific CPR tables with dynamic columns
     */

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
        private final String baseRole;
        private final String columnName;

        public CrimeSlotInfo(String crimeName, String slotName) {
            this.crimeName = crimeName;
            this.crimeAbbreviation = createCrimeAbbreviation(crimeName);
            this.slotName = slotName;
            this.baseRole = extractBaseRole(slotName);
            this.columnName = this.crimeAbbreviation + " - " + slotName;
        }

        public String getCrimeName() { return crimeName; }
        public String getCrimeAbbreviation() { return crimeAbbreviation; }
        public String getSlotName() { return slotName; }
        public String getBaseRole() { return baseRole; }
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

        private static String extractBaseRole(String roleName) {
            if (roleName == null) {
                return null;
            }
            return roleName.replaceAll("\\s*#\\d+$", "").trim();
        }
    }

    public static class CrimeRoleGroup {
        private final String crimeName;
        private final String baseRole;
        private final List<CrimeSlotInfo> relatedSlots;

        public CrimeRoleGroup(String crimeName, String baseRole, List<CrimeSlotInfo> relatedSlots) {
            this.crimeName = crimeName;
            this.baseRole = baseRole;
            this.relatedSlots = relatedSlots;
        }

        public String getCrimeName() { return crimeName; }
        public String getBaseRole() { return baseRole; }
        public List<CrimeSlotInfo> getRelatedSlots() { return relatedSlots; }

        public String getGroupKey() { return crimeName + "|" + baseRole; }
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

            // Group crime slots by base role for shared CPR logic
            List<CrimeRoleGroup> crimeRoleGroups = groupCrimeSlotsByRole(crimeSlots);

            logger.info("Found {} factions, {} crime slots, and {} role groups to process",
                    factions.size(), crimeSlots.size(), crimeRoleGroups.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing CPR for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Process CPR for this faction (pass all factions for cross-faction CPR lookup)
                    boolean success = updateFactionCPR(configConnection, ocDataConnection, factionInfo,
                            crimeSlots, crimeRoleGroups, factions);

                    if (success) {
                        logger.info("Successfully updated CPR for faction {}", factionInfo.getFactionId());
                        successfulCount++;
                    } else {
                        logger.error("Failed to update CPR for faction {}", factionInfo.getFactionId());
                        failedCount++;
                    }

                    processedCount++;

                } catch (Exception e) {
                    logger.error("Unexpected error updating CPR for faction {}: {}",
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
     * Group crime slots by their base role (e.g., all "Muscle" variants together)
     */
    private static List<CrimeRoleGroup> groupCrimeSlotsByRole(List<CrimeSlotInfo> crimeSlots) {
        Map<String, List<CrimeSlotInfo>> groupMap = crimeSlots.stream()
                .collect(Collectors.groupingBy(slot -> slot.getCrimeName() + "|" + slot.getBaseRole()));

        List<CrimeRoleGroup> groups = new ArrayList<>();
        for (Map.Entry<String, List<CrimeSlotInfo>> entry : groupMap.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|", 2);
            String crimeName = keyParts[0];
            String baseRole = keyParts[1];
            List<CrimeSlotInfo> relatedSlots = entry.getValue();

            groups.add(new CrimeRoleGroup(crimeName, baseRole, relatedSlots));
        }

        logger.info("Grouped {} crime slots into {} role groups", crimeSlots.size(), groups.size());

        groups.stream()
                .filter(group -> group.getRelatedSlots().size() > 1)
                .forEach(group -> logger.debug("Role group '{}' in crime '{}' has {} related slots: {}",
                        group.getBaseRole(), group.getCrimeName(), group.getRelatedSlots().size(),
                        group.getRelatedSlots().stream().map(CrimeSlotInfo::getSlotName).collect(Collectors.toList())));

        return groups;
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

        logger.info("Found {} crime-slot combinations", crimeSlots.size());
        return crimeSlots;
    }

    /**
     * Update CPR table for a single faction
     */
    private static boolean updateFactionCPR(Connection configConnection, Connection ocDataConnection,
                                            FactionInfo factionInfo, List<CrimeSlotInfo> crimeSlots,
                                            List<CrimeRoleGroup> crimeRoleGroups, List<FactionInfo> allFactions) throws SQLException {
        try {
            // Get faction members
            List<FactionMember> members = getFactionMembers(configConnection, factionInfo);
            if (members.isEmpty()) {
                logger.warn("No members found for faction {}", factionInfo.getFactionId());
                return true;
            }

            // Create CPR table
            String cprTableName = "cpr_" + factionInfo.getDbSuffix();
            createCPRTable(ocDataConnection, cprTableName, crimeSlots);

            // Populate CPR data with role sharing logic (now searches across all factions)
            populateCPRDataWithRoleSharing(ocDataConnection, cprTableName, members, crimeRoleGroups, allFactions);

            logger.info("Successfully updated CPR table {} with {} members and {} role groups (searched across {} factions)",
                    cprTableName, members.size(), crimeRoleGroups.size(), allFactions.size());
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
        String dropSql = "DROP TABLE IF EXISTS " + tableName;

        StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE ").append(tableName).append(" (");
        createSql.append("user_id VARCHAR(20) NOT NULL,");
        createSql.append("username VARCHAR(100) NOT NULL,");
        createSql.append("faction_id BIGINT NOT NULL,");

        for (CrimeSlotInfo crimeSlot : crimeSlots) {
            String columnName = sanitizeColumnName(crimeSlot.getColumnName());
            createSql.append("\"").append(columnName).append("\" INTEGER DEFAULT NULL,");  // Changed from DEFAULT 0 to DEFAULT NULL
        }

        createSql.append("last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,");
        createSql.append("PRIMARY KEY (user_id)");
        createSql.append(")");

        try (Statement stmt = ocDataConnection.createStatement()) {
            stmt.execute(dropSql);
            logger.debug("Dropped existing table {}", tableName);

            stmt.execute(createSql.toString());
            logger.debug("Created CPR table {} with {} crime-slot columns (defaulting to NULL)", tableName, crimeSlots.size());

            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_username ON " + tableName + "(username)");
        }
    }

    /**
     * Populate CPR data with role sharing logic
     */
    private static void populateCPRDataWithRoleSharing(Connection ocDataConnection, String cprTableName,
                                                       List<FactionMember> members, List<CrimeRoleGroup> crimeRoleGroups,
                                                       List<FactionInfo> allFactions) throws SQLException {
        // Insert members first
        insertMembersIntoCPRTable(ocDataConnection, cprTableName, members);

        // Update CPR values for each role group (with sharing logic across all factions)
        for (CrimeRoleGroup roleGroup : crimeRoleGroups) {
            updateCPRForRoleGroup(ocDataConnection, cprTableName, roleGroup, allFactions);
        }

        logger.debug("Populated CPR data for {} members and {} role groups in table {} (searched across {} factions)",
                members.size(), crimeRoleGroups.size(), cprTableName, allFactions.size());
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
     * Update CPR values for a role group (implements role sharing logic across all factions)
     */
    private static void updateCPRForRoleGroup(Connection ocDataConnection, String cprTableName,
                                              CrimeRoleGroup roleGroup, List<FactionInfo> allFactions) throws SQLException {

        // Get CPR data for ANY role in this group from ALL FACTIONS' completed crimes
        Map<String, Integer> userCPRMap = getCPRDataForRoleGroupAcrossAllFactions(ocDataConnection, roleGroup, allFactions);

        if (userCPRMap.isEmpty()) {
            logger.debug("No CPR data found for role group: {} in crime: {} (searched across {} factions)",
                    roleGroup.getBaseRole(), roleGroup.getCrimeName(), allFactions.size());
            return;
        }

        // Apply the same CPR values to ALL related slots in this role group
        for (CrimeSlotInfo slot : roleGroup.getRelatedSlots()) {
            updateCPRForSpecificSlot(ocDataConnection, cprTableName, slot, userCPRMap);
        }

        logger.debug("Updated CPR for {} users across {} slots in role group: {} (crime: {}) - searched {} factions",
                userCPRMap.size(), roleGroup.getRelatedSlots().size(),
                roleGroup.getBaseRole(), roleGroup.getCrimeName(), allFactions.size());
    }

    /**
     * Get CPR data for any role variant in a role group across ALL factions
     */
    private static Map<String, Integer> getCPRDataForRoleGroupAcrossAllFactions(Connection ocDataConnection,
                                                                                CrimeRoleGroup roleGroup,
                                                                                List<FactionInfo> allFactions) throws SQLException {
        Map<String, Integer> userCPRMap = new HashMap<>();

        List<String> roleNames = roleGroup.getRelatedSlots().stream()
                .map(CrimeSlotInfo::getSlotName)
                .collect(Collectors.toList());

        if (roleNames.isEmpty()) {
            return userCPRMap;
        }

        // Search across ALL factions' completed crimes tables
        for (FactionInfo faction : allFactions) {
            String completedCrimesTableName = Constants.TABLE_NAME_COMPLETED_CRIMES + faction.getDbSuffix();

            try {
                Map<String, Integer> factionCPRData = getCPRDataFromSpecificTable(ocDataConnection,
                        completedCrimesTableName,
                        roleGroup.getCrimeName(),
                        roleNames);

                // Merge results, keeping the MAXIMUM CPR value if a user appears in multiple factions
                for (Map.Entry<String, Integer> entry : factionCPRData.entrySet()) {
                    String userId = entry.getKey();
                    Integer newCPR = entry.getValue();

                    Integer existingCPR = userCPRMap.get(userId);
                    if (existingCPR == null || newCPR > existingCPR) {
                        userCPRMap.put(userId, newCPR);
                    }
                }

                if (!factionCPRData.isEmpty()) {
                    logger.debug("Found {} CPR records for role group '{}' (crime: '{}') in faction {} table: {}",
                            factionCPRData.size(), roleGroup.getBaseRole(), roleGroup.getCrimeName(),
                            faction.getFactionId(), completedCrimesTableName);
                }

            } catch (SQLException e) {
                logger.debug("Could not query table {} for role group '{}' in crime '{}': {}",
                        completedCrimesTableName, roleGroup.getBaseRole(), roleGroup.getCrimeName(), e.getMessage());
            }
        }

        logger.debug("Cross-faction search complete: Found CPR data for {} users in role group '{}' (crime: '{}') across {} factions",
                userCPRMap.size(), roleGroup.getBaseRole(), roleGroup.getCrimeName(), allFactions.size());

        return userCPRMap;
    }

    /**
     * Get CPR data for any role variant in a role group (single faction)
     */
    private static Map<String, Integer> getCPRDataForRoleGroup(Connection ocDataConnection,
                                                               String completedCrimesTableName,
                                                               CrimeRoleGroup roleGroup) throws SQLException {
        List<String> roleNames = roleGroup.getRelatedSlots().stream()
                .map(CrimeSlotInfo::getSlotName)
                .collect(Collectors.toList());

        return getCPRDataFromSpecificTable(ocDataConnection, completedCrimesTableName,
                roleGroup.getCrimeName(), roleNames);
    }

    /**
     * Get CPR data from a specific completed crimes table
     */
    private static Map<String, Integer> getCPRDataFromSpecificTable(Connection ocDataConnection,
                                                                    String completedCrimesTableName,
                                                                    String crimeName,
                                                                    List<String> roleNames) throws SQLException {
        Map<String, Integer> userCPRMap = new HashMap<>();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT user_id, MAX(checkpoint_pass_rate) as max_cpr ")
                .append("FROM ").append(completedCrimesTableName).append(" ")
                .append("WHERE crime_name = ? AND checkpoint_pass_rate IS NOT NULL ");

        if (!roleNames.isEmpty()) {
            sqlBuilder.append("AND (");
            for (int i = 0; i < roleNames.size(); i++) {
                if (i > 0) sqlBuilder.append(" OR ");
                sqlBuilder.append("role = ?");
            }
            sqlBuilder.append(") ");
        }

        sqlBuilder.append("GROUP BY user_id");

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sqlBuilder.toString())) {
            pstmt.setString(1, crimeName);

            for (int i = 0; i < roleNames.size(); i++) {
                pstmt.setString(i + 2, roleNames.get(i));
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String userId = rs.getString("user_id");
                    int maxCPR = rs.getInt("max_cpr");
                    userCPRMap.put(userId, maxCPR);
                }
            }
        }

        return userCPRMap;
    }

    /**
     * Update CPR values for a specific slot using the provided user CPR map
     */
    private static void updateCPRForSpecificSlot(Connection ocDataConnection, String cprTableName,
                                                 CrimeSlotInfo slot, Map<String, Integer> userCPRMap) throws SQLException {
        if (userCPRMap.isEmpty()) {
            return;
        }

        String columnName = sanitizeColumnName(slot.getColumnName());
        String updateSql = "UPDATE " + cprTableName + " SET \"" + columnName + "\" = ? WHERE user_id = ?";

        try (PreparedStatement updatePstmt = ocDataConnection.prepareStatement(updateSql)) {
            for (Map.Entry<String, Integer> entry : userCPRMap.entrySet()) {
                updatePstmt.setInt(1, entry.getValue());
                updatePstmt.setString(2, entry.getKey());
                updatePstmt.addBatch();
            }

            updatePstmt.executeBatch();
            logger.debug("Updated CPR column '{}' for {} users", columnName, userCPRMap.size());
        }
    }

    /**
     * Sanitize column name for PostgreSQL compatibility
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
}