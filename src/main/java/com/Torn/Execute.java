package com.Torn;

import com.Torn.FactionMembers.GetFactionMembers;
import com.Torn.Helpers.Constants;
import com.Torn.Helpers.FactionInfo;
import com.Torn.Helpers.TableCleanupUtility;
import com.Torn.Postgres.Postgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;


import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.Torn.ApiKeys.ValidateApiKeys.Validate;
import static com.Torn.FactionCrimes.AllCrimes.GetAllOc2CrimesData.fetchAndProcessAllOC2Crimes;
import static com.Torn.FactionCrimes.Available.GetAvailableCrimes.fetchAndProcessAllAvailableCrimes;
import static com.Torn.FactionCrimes.Completed.GetCompletedData.fetchAndProcessAllCompletedCrimes;
import static com.Torn.FactionCrimes.Completed.GetPaidCrimesData.fetchAndProcessAllPaidCrimes;
import static com.Torn.FactionCrimes.Available.GetAvailableMembers.fetchAndProcessAllAvailableMembers;
import static com.Torn.FactionCrimes.Completed.GetTornStatsCPR.updateAllFactionsCPRFromTornStats;
import static com.Torn.FactionCrimes._Algorithms.CrimeAssignmentOptimizer.sendDiscordAssignmentNotifications;
import static com.Torn.FactionCrimes._Overview.UpdateOverviewData.updateAllFactionsOverviewData;
import static com.Torn.FactionMembers.SyncMembers.syncFactionMembers;
import static com.Torn.FactionCrimes.Completed.UpdateMemberCPR.updateAllFactionsCPR;
import static com.Torn.Helpers.TableCleanupUtility.deleteAllTables;
import static com.Torn.Helpers.TableCleanupUtility.getTableCleanupSummary;
import static com.Torn.ItemManagement.CheckUsersHaveItems.checkUsersHaveItems;
import static com.Torn.PaymentRequests.PaymentVerificationService.verifyPaymentsAndExpireRequests;

@SpringBootApplication
@EnableWebMvc
public class Execute {

    

    /**
     * ├── Execute.java
     * │   ├── Main application entry point
     * │   ├── Handles both batch jobs and web service modes
     * │   ├── Orchestrates all setup and processing jobs
     * │   └── Manages graceful shutdown and cleanup
     */

    private static final Logger logger = LoggerFactory.getLogger(Execute.class);
    public static final Postgres postgres = new Postgres();
    public static List<FactionInfo> factionInfo = null;

    public static void main(String[] args) {
        logger.info("Application starting...");

        // Load PostgreSQL driver
        try {
            Class.forName("org.postgresql.Driver");
            logger.info("PostgreSQL JDBC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC driver not found in classpath", e);
            System.exit(1);
        }

        String jobCode = getJobCode();
        if (jobCode != null) {
            // Run as batch job (your existing functionality)
            logger.info("Running as batch job: {}", jobCode);
            runBatchJob(jobCode);
        } else {
            // Run as web service
            logger.info("No job specified - starting as web service");
            SpringApplication.run(Execute.class, args);
        }
    }

    private static void runBatchJob(String jobCode) {
        try {

            String databaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
            if (databaseUrl == null || databaseUrl.isEmpty()) {
                throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
            }

            logger.info("Creating database connection...");
            try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
                logger.info("Database connection established successfully");
                factionInfo = getFactionInfo(connection);
                logger.info("Faction Info Load completed successfully");
            } catch (Exception e) {
                logger.error("Error in faction info load", e);
                throw e;
            }

            switch (jobCode) {
                case Constants.JOB_RUN_ALL_SETUP_JOBS:
                    runAllJobs();
                    break;
                case Constants.JOB_VALIDATE_API_KEYS:
                    logger.info("Running API key validation job");
                    Validate();
                    break;
                case Constants.JOB_GET_FACTION_MEMBERS:
                    logger.info("Running faction members sync job");
                    syncFactionMembers();
                    break;
                case Constants.JOB_UPDATE_COMPLETED_DATA:
                    logger.info("Running completed data update job");
                    fetchAndProcessAllCompletedCrimes();
                    break;
                case Constants.JOB_GET_ALL_OC_CRIMES:
                    logger.info("Running Get All OC Data job");
                    fetchAndProcessAllOC2Crimes();
                    break;
                case Constants.JOB_UPDATE_CRIMES_PAID_DATA:
                    logger.info("Running paid crimes data update job");
                    fetchAndProcessAllPaidCrimes();
                    break;
                case Constants.JOB_UPDATE_UPDATE_CPR_DATA:
                    logger.info("Running update CPR job");
                    updateAllFactionsCPR();
                    break;
                case Constants.JOB_UPDATE_TORNSTATS_CPR:
                    logger.info("Running TornStats CPR update job");
                    updateAllFactionsCPRFromTornStats();
                    break;
                case Constants.JOB_UPDATE_OVERVIEW_DATA:
                    logger.info("Running overview data update job");
                    updateAllFactionsOverviewData();
                    break;
                case Constants.JOB_CHECK_USER_ITEMS:
                    logger.info("Running user items check job");
                    checkUsersHaveItems();
                    break;
                case Constants.JOB_VERIFY_PAYMENTS:
                    logger.info("Running payment verification job");
                    verifyPaymentsAndExpireRequests();
                    break;
                case Constants.JOB_CHECK_AVAILABLE_CRIMES_MEMBERS:
                    logger.info("Running available crimes check job");
                    fetchAndProcessAllAvailableCrimes();
                    fetchAndProcessAllAvailableMembers();
                    //sendDiscordAssignmentNotifications();
                    break;

                default:
                    logger.error("Unknown job code: {}", jobCode);
                    cleanup();
                    System.exit(1);
            }

            logger.info("Job {} completed successfully", jobCode);
            cleanup();
            System.exit(0);

        } catch (Exception e) {
            logger.error("Job {} failed", jobCode, e);
            cleanup();
            System.exit(1);
        }
    }

    private static void runAllJobs() throws SQLException, IOException {
        logger.warn("==========================================");
        logger.warn("DANGER: About to delete ALL faction tables!");
        logger.warn("==========================================");

        // Show what would be deleted
        try {
            TableCleanupUtility.TableCleanupSummary summary = getTableCleanupSummary();
            logger.warn("Tables that will be deleted:");
            logger.warn("  OC_DATA database: {} tables", summary.ocDataTables.size());
            for (String table : summary.ocDataTables) {
                logger.warn("    - {}", table);
            }
            logger.warn("  CONFIG database: {} tables", summary.configTables.size());
            for (String table : summary.configTables) {
                logger.warn("    - {}", table);
            }
            logger.warn("  Total: {} tables will be deleted", summary.totalTables);

            // Only proceed if execute flag is set to true
            if (getExecute()) {
                logger.warn("Execute flag is TRUE - proceeding with table deletion...");
                deleteAllTables();
                logger.info("All tables deleted successfully");

                logger.warn("==========================================");
                logger.info("Running All Set Up Jobs with Discord notifications SUPPRESSED");
                logger.warn("==========================================");

                // Set environment variable to suppress Discord notifications
                System.setProperty(Constants.SUPPRESS_PROCESSING, "true");

                try {
                    Validate();
                    syncFactionMembers();
                    fetchAndProcessAllCompletedCrimes();
                    fetchAndProcessAllOC2Crimes();
                    updateAllFactionsOverviewData();
                    fetchAndProcessAllPaidCrimes();
                    updateAllFactionsCPR();
                    updateAllFactionsCPRFromTornStats();
                } finally {
                    // Remove the suppression flag after setup is complete
                    System.clearProperty(Constants.SUPPRESS_PROCESSING);
                    logger.info("Setup jobs completed - Discord notifications re-enabled");
                }

            } else {
                logger.info("Execute flag is FALSE - skipping table deletion (this was a dry run)");
                logger.info("Set the 'Execute' environment variable to 'true' to actually delete the tables");
            }

        } catch (Exception e) {
            logger.error("Error during table cleanup operation", e);
            throw e;
        }
    }

    private static String getJobCode() {
        String jobCode = System.getenv(Constants.EXECUTE_JOB);
        return (jobCode != null && !jobCode.isEmpty()) ? jobCode : null;
    }

    private static boolean getExecute() {
        String executeCode = System.getenv(Constants.EXECUTE);
        return "true".equalsIgnoreCase(executeCode);
    }

    private static List<FactionInfo> getFactionInfo(Connection connection) throws SQLException {
        Map<String,FactionInfo> factionMap = new HashMap<>();

        String sql = "SELECT " +
                "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                "f." + Constants.COLUMN_NAME_DB_SUFFIX + ", " +
                "ak." + Constants.COLUMN_NAME_API_KEY + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);

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

                // Add to map, collecting multiple API keys per faction
                factionMap.computeIfAbsent(factionId, k -> new FactionInfo(factionId, dbSuffix, new ArrayList<>()))
                        .getApiKeys().add(apiKey);
            }
        }
        List<FactionInfo> factions = new ArrayList<>(factionMap.values());

        // Log API key counts
        for (FactionInfo faction : factions) {
            logger.info("Faction {} has {} API key(s)", faction.getFactionId(), faction.getApiKeys().size());
        }

        logger.info("Found {} active factions to process", factions.size());
        return factions;
    }

    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }

    /**
     * Clean up resources before application exit
     */
    private static void cleanup() {
        try {
            logger.info("Cleaning up resources...");
            Postgres.cleanup(); // Close all database connection pools
        } catch (Exception e) {
            logger.warn("Error during cleanup: {}", e.getMessage());
        }
    }

}
