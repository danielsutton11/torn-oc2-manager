package com.Torn.FactionCrimes.Available;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.FactionCrimes.Models.CrimesModel.Slot;
import com.Torn.Helpers.Constants;
import com.Torn.FactionCrimes.Models.ItemMarketModel.Item;
import com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import java.time.Instant;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GetAvailableCrimes {

    private static final Logger logger = LoggerFactory.getLogger(GetAvailableCrimes.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
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

    /**
     * Main entry point for fetching available crimes for all factions
     */
    public static void fetchAndProcessAllAvailableCrimes() throws SQLException, IOException {
        logger.info("Starting available crimes fetch for all factions with robust API handling");

        String databaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to crimes database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping crimes fetch to prevent further failures");
                return;
            }

            // Get faction information from config database
            List<FactionInfo> factions = getFactionInfo();
            if (factions.isEmpty()) {
                logger.warn("No active factions found to process crimes for");
                return;
            }

            logger.info("Found {} active factions to process crimes for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalCrimesProcessed = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing crimes for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Fetch crimes for this faction
                    CrimesProcessingResult result = fetchAndStoreCrimesForFaction(connection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("✓ Successfully processed {} crimes for faction {} ({})",
                                result.getCrimesProcessed(), factionInfo.getFactionId(),
                                factionInfo.getOwnerName());
                        successfulCount++;
                        totalCrimesProcessed += result.getCrimesProcessed();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("✗ Failed to process crimes for faction {} ({}): {}",
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
                    logger.warn("Crimes processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("✗ Unexpected error processing crimes for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("Available crimes processing completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total crimes processed: {}", totalCrimesProcessed);

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

            // Warn if many factions failed
            if (failedCount > successfulCount && processedCount > 2) {
                logger.error("More than half of factions failed - Torn API may be experiencing issues");
            }

        } catch (SQLException e) {
            logger.error("Database error during crimes processing", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database
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

        logger.info("Found {} active factions for crimes processing", factions.size());
        return factions;
    }

    /**
     * Fetch and store crimes for a single faction
     */
    private static CrimesProcessingResult fetchAndStoreCrimesForFaction(Connection connection, FactionInfo factionInfo) {
        try {
            // Construct API URL for this faction's crimes
            String apiUrl = Constants.API_URL_AVAILABLE_FACTION_CRIMES + "&key=" + factionInfo.getApiKey();

            logger.debug("Fetching crimes for faction: {}", factionInfo.getFactionId());

            // Use robust API handler
            ApiResponse response = TornApiHandler.executeRequest(apiUrl, factionInfo.getApiKey());

            // Handle different response types
            if (response.isSuccess()) {
                logger.info("✓ Successfully fetched crimes data for faction {}", factionInfo.getFactionId());
                return processCrimesResponse(connection, factionInfo, response.getBody());

            } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
                logger.error("Circuit breaker is open - skipping faction {} to prevent further API failures",
                        factionInfo.getFactionId());
                return CrimesProcessingResult.circuitBreakerOpen();

            } else if (response.isAuthenticationIssue()) {
                logger.error("API key authentication issue for faction {}: {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return CrimesProcessingResult.failure("API key authentication failed: " + response.getErrorMessage());

            } else if (response.isTemporaryError()) {
                logger.warn("Temporary API error for faction {} (will retry on next run): {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return CrimesProcessingResult.failure("Temporary API error: " + response.getErrorMessage());

            } else {
                logger.error("Permanent API error for faction {}: {}",
                        factionInfo.getFactionId(), response.getErrorMessage());
                return CrimesProcessingResult.failure("API error: " + response.getErrorMessage());
            }

        } catch (Exception e) {
            logger.error("Exception processing crimes for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return CrimesProcessingResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Process the crimes response and store in database
     */
    private static CrimesProcessingResult processCrimesResponse(Connection connection, FactionInfo factionInfo, String responseBody) {
        try {
            // Parse JSON response
            CrimesResponse crimesResponse = objectMapper.readValue(responseBody, CrimesResponse.class);

            if (crimesResponse.getCrimes() == null) {
                logger.warn("No crimes data in response for faction {}", factionInfo.getFactionId());
                return CrimesProcessingResult.success(0);
            }

            // Create faction-specific table
            String tableName = "a_crimes_" + factionInfo.getDbSuffix();
            createCrimesTableIfNotExists(connection, tableName);

            // Filter and sort crimes
            List<Crime> validCrimes = crimesResponse.getCrimes().stream()
                    .filter(crime -> crime.getSlots() != null && !crime.getSlots().isEmpty())
                    .sorted(Comparator
                            .comparing((Crime c) -> getStatusPriority(c.getStatus()))
                            .thenComparing(Crime::getDifficulty, Comparator.nullsLast(Comparator.reverseOrder()))
                            .thenComparing(Crime::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder()))
                            .thenComparing(Crime::getId))
                    .collect(Collectors.toList());

            if (validCrimes.isEmpty()) {
                logger.info("No valid crimes with available slots for faction {}", factionInfo.getFactionId());
                return CrimesProcessingResult.success(0);
            }

            // Process crimes in transaction
            connection.setAutoCommit(false);
            try {
                // Clear existing data for this faction
                clearExistingCrimes(connection, tableName);

                // Process each crime
                int crimesProcessed = 0;
                for (Crime crime : validCrimes) {
                    processSingleCrime(connection, tableName, crime, factionInfo);
                    crimesProcessed++;
                }

                connection.commit();
                logger.info("Successfully stored {} crimes for faction {} in table {}",
                        crimesProcessed, factionInfo.getFactionId(), tableName);
                return CrimesProcessingResult.success(crimesProcessed);

            } catch (SQLException e) {
                connection.rollback();
                logger.error("Failed to store crimes for faction {}, rolling back", factionInfo.getFactionId(), e);
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }

        } catch (Exception e) {
            logger.error("Error processing crimes response for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return CrimesProcessingResult.failure("Response processing error: " + e.getMessage());
        }
    }

    /**
     * Create faction-specific crimes table if it doesn't exist
     */
    private static void createCrimesTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "crime_id BIGINT," +
                "faction_id BIGINT NOT NULL," +
                "name VARCHAR(255) NOT NULL," +
                "difficulty INTEGER," +
                "status VARCHAR(50)," +
                "created_at TIMESTAMP," +
                "ready_at TIMESTAMP," +
                "expired_at TIMESTAMP," +

                // Slot-specific fields
                "slot_position VARCHAR(100)," +
                "slot_position_id VARCHAR(10)," +
                "slot_position_number INTEGER," +
                "item_required_id BIGINT," +
                "item_required_name VARCHAR(255)," +
                "item_required_avg_cost BIGINT," +
                "item_is_reusable BOOLEAN," +

                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (crime_id, slot_position_id)" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better query performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_status ON " + tableName + "(status)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_created_at ON " + tableName + "(created_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");

            logger.debug("Table {} created or verified with indexes", tableName);
        }
    }

    /**
     * Clear existing crimes data for the table
     */
    private static void clearExistingCrimes(Connection connection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int deletedRows = pstmt.executeUpdate();
            logger.debug("Cleared {} existing crimes from {}", deletedRows, tableName);
        }
    }

    // Item cache for market data to avoid repeated API calls
    private static final ConcurrentHashMap<Long, Item> itemCache = new ConcurrentHashMap<>();

    /**
     * Process a single crime and insert its slots
     */
    private static void processSingleCrime(Connection connection, String tableName, Crime crime, FactionInfo factionInfo) throws SQLException {
        if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
            logger.debug("Skipping crime ID {} - no available slots", crime.getId());
            return;
        }

        String insertSql = "INSERT INTO " + tableName + " (" +
                "crime_id, faction_id, name, difficulty, status, created_at, ready_at, expired_at, " +
                "slot_position, slot_position_id, slot_position_number, " +
                "item_required_id, item_required_name, item_required_avg_cost, item_is_reusable, " +
                "last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
            // Convert factionId to Long
            Long factionIdLong = Long.parseLong(factionInfo.getFactionId());

            // Group slots by position and handle duplicates
            Map<String, List<Slot>> groupedSlots = crime.getSlots().stream()
                    .collect(Collectors.groupingBy(Slot::getPosition));

            List<Slot> orderedSlots = new ArrayList<>();
            for (Map.Entry<String, List<Slot>> entry : groupedSlots.entrySet()) {
                List<Slot> group = entry.getValue();
                group.sort(Comparator.comparing(Slot::getPositionId, Comparator.nullsLast(String::compareTo)));

                if (group.size() == 1) {
                    orderedSlots.add(group.get(0));
                } else {
                    // Handle multiple slots with same position
                    for (int i = 0; i < group.size(); i++) {
                        Slot slot = group.get(i);
                        String renamedPosition = slot.getPosition() + " #" + (i + 1);
                        slot.setPosition(renamedPosition);
                        orderedSlots.add(slot);
                    }
                }
            }

            // Insert only available slots (slots without users)
            for (Slot slot : orderedSlots) {
                if (slot.getUser() != null) continue; // Skip occupied slots

                // Crime fields
                stmt.setLong(1, crime.getId());
                stmt.setLong(2, factionIdLong);
                stmt.setString(3, crime.getName());
                stmt.setObject(4, crime.getDifficulty());
                stmt.setString(5, crime.getStatus());
                stmt.setTimestamp(6, timestampFromEpoch(crime.getCreatedAt()));
                stmt.setTimestamp(7, timestampFromEpoch(crime.getReadyAt()));
                stmt.setTimestamp(8, timestampFromEpoch(crime.getExpiredAt()));

                // Slot fields
                stmt.setString(9, slot.getPosition());
                stmt.setString(10, slot.getPositionId());
                stmt.setObject(11, slot.getPositionNumber());

                // Item requirement fields
                if (slot.getItemRequirement() != null) {
                    Long itemId = slot.getItemRequirement().getId();
                    stmt.setObject(12, itemId);

                    // Get item market data (cached)
                    Item itemMarket = itemCache.computeIfAbsent(itemId, GetAvailableCrimes::fetchItemMarketSafe);
                    if (itemMarket != null) {
                        stmt.setString(13, itemMarket.getName());
                        stmt.setObject(14, itemMarket.getAveragePrice());
                    } else {
                        stmt.setNull(13, Types.VARCHAR);
                        stmt.setNull(14, Types.INTEGER);
                    }

                    stmt.setObject(15, slot.getItemRequirement().getIsReusable());
                } else {
                    stmt.setNull(12, Types.BIGINT);
                    stmt.setNull(13, Types.VARCHAR);
                    stmt.setNull(14, Types.INTEGER);
                    stmt.setNull(15, Types.BOOLEAN);
                }

                try {
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    logger.warn("Failed to insert slot for crime {}: {}", crime.getId(), e.getMessage());
                }
            }
        }
    }

    //TODO: Need to fix this

    /**
     * Fetch item market data with error handling
     */
    private static Item fetchItemMarketSafe(Long itemId) {
        try {
            if (itemId == null) return null;

            String itemUrl = Constants.API_URL_ITEM_MARKET + itemId + Constants.API_URL_ITEM_MARKET_JOIN;

            // Use a test API key for item market data (items don't require faction-specific keys)
            String testApiKey = System.getenv("HEALTH_CHECK_API_KEY");
            if (testApiKey == null) {
                testApiKey = System.getenv("TORN_LIMITED_API_KEY");
            }

            if (testApiKey == null) {
                logger.warn("No API key available for item market data - skipping item {}", itemId);
                return null;
            }

            ApiResponse response = TornApiHandler.executeRequest(itemUrl, testApiKey);

            if (response.isSuccess()) {
                ItemMarketResponse marketResponse = objectMapper.readValue(response.getBody(), ItemMarketResponse.class);
                return marketResponse != null ? marketResponse.getItemMarket() : null;
            } else {
                logger.debug("Failed to fetch item market data for item {}: {}", itemId, response.getErrorMessage());
                return null;
            }

        } catch (Exception e) {
            logger.warn("Error fetching market data for item {}: {}", itemId, e.getMessage());
            return null;
        }
    }

    // Utility methods
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                dbSuffix.length() >= 1 &&
                dbSuffix.length() <= 50;
    }

    private static Timestamp timestampFromEpoch(Long epochSeconds) {
        if (epochSeconds == null) return null;
        return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
    }

    private static int getStatusPriority(String status) {
        if (Constants.PLANNING.equalsIgnoreCase(status)) return 1;
        if (Constants.RECRUITING.equalsIgnoreCase(status)) return 2;
        return 3;
    }

    /**
     * Result wrapper for crimes processing
     */
    private static class CrimesProcessingResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int crimesProcessed;
        private final String errorMessage;

        private CrimesProcessingResult(boolean success, boolean circuitBreakerOpen, int crimesProcessed, String errorMessage) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.crimesProcessed = crimesProcessed;
            this.errorMessage = errorMessage;
        }

        public static CrimesProcessingResult success(int crimesProcessed) {
            return new CrimesProcessingResult(true, false, crimesProcessed, null);
        }

        public static CrimesProcessingResult failure(String errorMessage) {
            return new CrimesProcessingResult(false, false, 0, errorMessage);
        }

        public static CrimesProcessingResult circuitBreakerOpen() {
            return new CrimesProcessingResult(false, true, 0, "Circuit breaker is open");
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getCrimesProcessed() { return crimesProcessed; }
        public String getErrorMessage() { return errorMessage; }
    }
}