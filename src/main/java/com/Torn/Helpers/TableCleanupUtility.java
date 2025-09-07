package com.Torn.Helpers;

import com.Torn.Execute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for cleaning up all faction-related and OC2 tables
 */
public class TableCleanupUtility {

    private static final Logger logger = LoggerFactory.getLogger(TableCleanupUtility.class);

    /**
     * Delete all faction-related tables and OC2 tables from both databases
     */
    public static void deleteAllTables() throws SQLException {
        logger.info("Starting comprehensive table cleanup process");

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

            // Get all faction suffixes first
            List<String> factionSuffixes = getAllFactionSuffixes(configConnection);
            if (factionSuffixes.isEmpty()) {
                logger.warn("No faction suffixes found - only deleting static OC2 tables");
            } else {
                logger.info("Found {} faction suffixes for table cleanup", factionSuffixes.size());
            }

            // Delete tables in OC_DATA database
            deleteOCDataTables(ocDataConnection, factionSuffixes);

            // Delete tables in CONFIG database
            deleteConfigTables(configConnection, factionSuffixes);

            logger.info("Comprehensive table cleanup completed successfully");

        } catch (SQLException e) {
            logger.error("Database error during table cleanup", e);
            throw e;
        }
    }

    /**
     * Delete all faction-related tables in the OC_DATA database
     */
    private static void deleteOCDataTables(Connection ocDataConnection, List<String> factionSuffixes) throws SQLException {
        logger.info("Deleting tables from OC_DATA database...");

        int deletedCount = 0;

        // Define table prefixes to delete from OC_DATA
        String[] tablePrefixes = {
                Constants.TABLE_NAME_AVAILABLE_CRIMES,     // Available crimes
                Constants.TABLE_NAME_AVAILABLE_MEMBERS,    // Available members
                Constants.TABLE_NAME_COMPLETED_CRIMES,     // Completed crimes
                Constants.TABLE_NAME_REWARDS_CRIMES,     // Rewards crimes
                Constants.TABLE_NAME_CPR,          // CPR tables
                Constants.TABLE_NAME_OVERVIEW              // Overview tables
        };

        try (Statement stmt = ocDataConnection.createStatement()) {
            for (String factionSuffix : factionSuffixes) {
                if (!isValidDbSuffix(factionSuffix)) {
                    logger.warn("Skipping invalid faction suffix: {}", factionSuffix);
                    continue;
                }

                for (String prefix : tablePrefixes) {
                    String tableName = prefix + factionSuffix;
                    try {
                        String dropSql = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
                        stmt.execute(dropSql);
                        logger.info("Deleted table: {}", tableName);
                        deletedCount++;
                    } catch (SQLException e) {
                        logger.error("Failed to delete table {}: {}", tableName, e.getMessage());
                    }
                }
            }
        }

        logger.info("Deleted {} tables from OC_DATA database", deletedCount);
    }

    /**
     * Delete all faction-related tables and OC2 tables in the CONFIG database
     */
    private static void deleteConfigTables(Connection configConnection, List<String> factionSuffixes) throws SQLException {
        logger.info("Deleting tables from CONFIG database...");

        int deletedCount = 0;

        try (Statement stmt = configConnection.createStatement()) {
            // Delete static OC2 tables first
            String[] staticTables = {
                    Constants.TABLE_NAME_OC2_CRIMES,
                    Constants.TABLE_NAME_OC2_CRIMES_SLOTS
            };

            for (String tableName : staticTables) {
                try {
                    String dropSql = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
                    stmt.execute(dropSql);
                    logger.info("Deleted static table: {}", tableName);
                    deletedCount++;
                } catch (SQLException e) {
                    logger.error("Failed to delete static table {}: {}", tableName, e.getMessage());
                }
            }

            // Delete faction-specific members tables
            for (String factionSuffix : factionSuffixes) {
                if (!isValidDbSuffix(factionSuffix)) {
                    logger.warn("Skipping invalid faction suffix: {}", factionSuffix);
                    continue;
                }

                String membersTableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionSuffix;
                try {
                    String dropSql = "DROP TABLE IF EXISTS " + membersTableName + " CASCADE";
                    stmt.execute(dropSql);
                    logger.info("Deleted faction members table: {}", membersTableName);
                    deletedCount++;
                } catch (SQLException e) {
                    logger.error("Failed to delete faction members table {}: {}", membersTableName, e.getMessage());
                }
            }
        }

        logger.info("Deleted {} tables from CONFIG database", deletedCount);
    }

    /**
     * Get all faction suffixes from the config database
     */
    private static List<String> getAllFactionSuffixes(Connection configConnection) {
        List<String> suffixes = new ArrayList<>();

        String sql = "SELECT DISTINCT " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE " + Constants.COLUMN_NAME_DB_SUFFIX + " IS NOT NULL " +
                "AND " + Constants.COLUMN_NAME_DB_SUFFIX + " != ''";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                if (isValidDbSuffix(dbSuffix)) {
                    suffixes.add(dbSuffix);
                } else {
                    logger.warn("Invalid db_suffix found: {}", dbSuffix);
                }
            }

        } catch (SQLException e) {
            logger.error("Error fetching faction suffixes from config database", e);
        }

        logger.debug("Found {} valid faction suffixes", suffixes.size());
        return suffixes;
    }

    /**
     * Delete tables for a specific faction only
     */
    public static void deleteTablesForFaction(String factionSuffix) throws SQLException {
        if (!isValidDbSuffix(factionSuffix)) {
            throw new IllegalArgumentException("Invalid faction suffix: " + factionSuffix);
        }

        logger.info("Starting table cleanup for faction suffix: {}", factionSuffix);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            int deletedCount = 0;

            // Delete from OC_DATA database
            String[] ocDataPrefixes = {Constants.TABLE_NAME_AVAILABLE_CRIMES, Constants.TABLE_NAME_AVAILABLE_MEMBERS,
                    Constants.TABLE_NAME_COMPLETED_CRIMES, Constants.TABLE_NAME_REWARDS_CRIMES, Constants.TABLE_NAME_CPR, Constants.TABLE_NAME_OVERVIEW};

            try (Statement stmt = ocDataConnection.createStatement()) {
                for (String prefix : ocDataPrefixes) {
                    String tableName = prefix + factionSuffix;
                    try {
                        String dropSql = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
                        stmt.execute(dropSql);
                        logger.info("Deleted table: {}", tableName);
                        deletedCount++;
                    } catch (SQLException e) {
                        logger.error("Failed to delete table {}: {}", tableName, e.getMessage());
                    }
                }
            }

            // Delete from CONFIG database
            String membersTableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionSuffix;

            try (Statement stmt = configConnection.createStatement()) {
                String dropSql = "DROP TABLE IF EXISTS " + membersTableName + " CASCADE";
                stmt.execute(dropSql);
                logger.info("Deleted faction members table: {}", membersTableName);
                deletedCount++;
            } catch (SQLException e) {
                logger.error("Failed to delete faction members table {}: {}", membersTableName, e.getMessage());
            }

            logger.info("Deleted {} tables for faction suffix: {}", deletedCount, factionSuffix);

        } catch (SQLException e) {
            logger.error("Database error during faction table cleanup for suffix: {}", factionSuffix, e);
            throw e;
        }
    }

    /**
     * Get a summary of all tables that would be deleted (dry run)
     */
    public static TableCleanupSummary getTableCleanupSummary() throws SQLException {
        logger.info("Generating table cleanup summary (dry run)");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            List<String> factionSuffixes = getAllFactionSuffixes(configConnection);

            TableCleanupSummary summary = new TableCleanupSummary();
            summary.factionSuffixes = new ArrayList<>(factionSuffixes);

            // Calculate tables to be deleted from OC_DATA
            String[] ocDataPrefixes = {Constants.TABLE_NAME_AVAILABLE_CRIMES, Constants.TABLE_NAME_AVAILABLE_MEMBERS,
                    Constants.TABLE_NAME_COMPLETED_CRIMES, Constants.TABLE_NAME_REWARDS_CRIMES, Constants.TABLE_NAME_CPR, Constants.TABLE_NAME_OVERVIEW};
            for (String factionSuffix : factionSuffixes) {
                for (String prefix : ocDataPrefixes) {
                    summary.ocDataTables.add(prefix + factionSuffix);
                }
            }

            // Calculate tables to be deleted from CONFIG
            summary.configTables.add(Constants.TABLE_NAME_OC2_CRIMES);
            summary.configTables.add(Constants.TABLE_NAME_OC2_CRIMES_SLOTS);
            for (String factionSuffix : factionSuffixes) {
                summary.configTables.add(Constants.TABLE_NAME_FACTION_MEMBERS + factionSuffix);
            }

            summary.totalTables = summary.ocDataTables.size() + summary.configTables.size();

            logger.info("Table cleanup summary: {} total tables would be deleted", summary.totalTables);
            logger.info("  - {} tables from OC_DATA database", summary.ocDataTables.size());
            logger.info("  - {} tables from CONFIG database", summary.configTables.size());
            logger.info("  - {} faction suffixes found", summary.factionSuffixes.size());

            return summary;

        } catch (SQLException e) {
            logger.error("Database error during cleanup summary generation", e);
            throw e;
        }
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
     * Summary class for table cleanup operations
     */
    public static class TableCleanupSummary {
        public List<String> factionSuffixes = new ArrayList<>();
        public List<String> ocDataTables = new ArrayList<>();
        public List<String> configTables = new ArrayList<>();
        public int totalTables = 0;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("TableCleanupSummary{\n");
            sb.append("  totalTables=").append(totalTables).append("\n");
            sb.append("  factionSuffixes=").append(factionSuffixes).append("\n");
            sb.append("  ocDataTables=").append(ocDataTables).append("\n");
            sb.append("  configTables=").append(configTables).append("\n");
            sb.append("}");
            return sb.toString();
        }
    }
}