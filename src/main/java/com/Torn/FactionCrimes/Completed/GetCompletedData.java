package com.Torn.FactionCrimes.Completed;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.FactionCrimes.Models.CrimesModel.Slot;
import com.Torn.FactionCrimes.Models.CrimesModel.SlotUser;
import com.Torn.FactionCrimes.Models.ItemMarketModel.Item;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.Torn.Discord.Messages.DiscordMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class GetCompletedData {

    private static final Logger logger = LoggerFactory.getLogger(GetCompletedData.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final int TORN_API_RATE_LIMIT_MS = 2000;

    // Constants for pagination
    private static final int CRIMES_PER_PAGE = 10; // Torn API default
    private static final int MAX_PAGES_INITIAL = 100; // Reasonable limit for initial sync
    private static final int MAX_PAGES_INCREMENTAL = 10; // For incremental updates

    //Default look back time for incremental updates (can be overridden by environment variables)
    private static final int DEFAULT_INCREMENTAL_MINUTES = 60; // Default 60 minutes for incremental

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
     * Main entry point for fetching completed crimes for all factions
     */
    public static void fetchAndProcessAllCompletedCrimes() throws SQLException, IOException {
        logger.info("Starting completed crimes fetch for all factions with configurable timestamp filtering");

        String databaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to completed crimes database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping completed crimes fetch to prevent further failures");
                return;
            }

            // Get faction information from config database
            List<FactionInfo> factions = getFactionInfo();
            if (factions.isEmpty()) {
                logger.warn("No active OC2-enabled factions found to process completed crimes for");
                return;
            }

            logger.info("Found {} OC2-enabled factions to process completed crimes for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalCrimesProcessed = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing completed crimes for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Process completed crimes for this faction
                    CompletedCrimesResult result = fetchAndStoreCompletedCrimesForFaction(connection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("Successfully processed {} completed crimes for faction {} ({})",
                                result.getCrimesProcessed(), factionInfo.getFactionId(),
                                factionInfo.getOwnerName());
                        successfulCount++;
                        totalCrimesProcessed += result.getCrimesProcessed();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("Failed to process completed crimes for faction {} ({}): {}",
                                factionInfo.getFactionId(), factionInfo.getOwnerName(),
                                result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                    // Rate limiting between factions
                    if (processedCount < factions.size()) {
                        logger.debug("Waiting {}ms before processing next faction", TORN_API_RATE_LIMIT_MS);
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("Completed crimes processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("✗ Unexpected error processing completed crimes for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("Completed crimes processing finished:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total completed crimes processed: {}", totalCrimesProcessed);

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

            if (failedCount > successfulCount && processedCount > 2) {
                logger.error("More than half of factions failed - Torn API may be experiencing issues");
            }

        } catch (SQLException e) {
            logger.error("Database error during completed crimes processing", e);
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

                    if (factionId == null || dbSuffix == null || apiKey == null) {
                        logger.warn("Skipping faction with null data: factionId={}, dbSuffix={}, apiKey={}",
                                factionId, dbSuffix, (apiKey == null ? "null" : "***"));
                        continue;
                    }

                    if (!isValidDbSuffix(dbSuffix)) {
                        logger.error("Invalid db_suffix for faction {}: {}", factionId, dbSuffix);
                        continue;
                    }

                    factions.add(new FactionInfo(factionId, dbSuffix, apiKey, ownerName));
                }
            }
        }

        logger.info("Found {} OC2-enabled factions for completed crimes processing", factions.size());
        return factions;
    }

    /**
     * Fetch and store completed crimes for a single faction
     */
    private static CompletedCrimesResult fetchAndStoreCompletedCrimesForFaction(Connection connection, FactionInfo factionInfo) {
        try {
            String tableName = Constants.TABLE_NAME_COMPLETED_CRIMES + factionInfo.getDbSuffix();
            createCompletedCrimesTableIfNotExists(connection, tableName);

            //Determine timestamp filtering based on environment variables and table state
            TimestampConfig timestampConfig = getTimestampConfig(connection, tableName);
            int maxPages = timestampConfig.isInitialSync() ? MAX_PAGES_INITIAL : MAX_PAGES_INCREMENTAL;

            logger.info("Processing completed crimes for faction {} ({}, {})",
                    factionInfo.getFactionId(),
                    timestampConfig.isInitialSync() ? "Initial sync - getting ALL data" : "Incremental update",
                    timestampConfig.getFromTimestamp() != null ?
                            "from timestamp: " + formatTimestamp(timestampConfig.getFromTimestamp()) : "no timestamp filter");

            int totalCrimesProcessed = 0;
            int currentPage = 0;
            boolean hasMoreData = true;

            // Load username lookup map from ALL factions (not just current faction)
            Map<Long, String> usernameMap = loadUsernameMapAcrossAllFactions();

            while (hasMoreData && currentPage < maxPages) {
                // Construct API URL with 'from' parameter for timestamp filtering
                String apiUrl = buildApiUrl(currentPage, timestampConfig.getFromTimestamp());

                logger.debug("Fetching completed crimes page {} for faction: {} ({})",
                        currentPage, factionInfo.getFactionId(),
                        timestampConfig.getFromTimestamp() != null ?
                                "from timestamp: " + timestampConfig.getFromTimestamp() : "getting ALL data");

                // Use robust API handler
                ApiResponse response = TornApiHandler.executeRequest(apiUrl, factionInfo.getApiKey());

                if (response.isSuccess()) {
                    CrimesResponse crimesResponse = objectMapper.readValue(response.getBody(), CrimesResponse.class);

                    if (crimesResponse.getCrimes() == null || crimesResponse.getCrimes().isEmpty()) {
                        logger.info("No more completed crimes found for faction {} at page {}",
                                factionInfo.getFactionId(), currentPage);
                        hasMoreData = false;
                        break;
                    }

                    // Filter for completed crimes only
                    List<Crime> completedCrimes = crimesResponse.getCrimes().stream()
                            .filter(crime -> Constants.COMPLETED.equalsIgnoreCase(crime.getStatus()) ||
                                    crime.getExecutedAt() != null)
                            .collect(Collectors.toList());

                    if (completedCrimes.isEmpty()) {
                        logger.debug("No completed crimes in page {} for faction {}", currentPage, factionInfo.getFactionId());
                        currentPage++;
                        continue;
                    }

                    // Process and store crimes
                    int crimesInPage = processCompletedCrimesPage(connection, tableName, completedCrimes,
                            factionInfo, usernameMap, timestampConfig.isInitialSync());
                    totalCrimesProcessed += crimesInPage;

                    // Check if we got less than a full page (indicates end of data)
                    if (crimesResponse.getCrimes().size() < CRIMES_PER_PAGE) {
                        hasMoreData = false;
                    }

                    currentPage++;

                    // Rate limiting between API calls
                    if (hasMoreData && currentPage < maxPages) {
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
                    logger.error("Circuit breaker is open - stopping completed crimes fetch for faction {}",
                            factionInfo.getFactionId());
                    return CompletedCrimesResult.circuitBreakerOpen();

                } else if (response.isAuthenticationIssue()) {
                    logger.error("API key authentication issue for faction {}: {}",
                            factionInfo.getFactionId(), response.getErrorMessage());
                    return CompletedCrimesResult.failure("API authentication failed: " + response.getErrorMessage());

                } else if (response.isTemporaryError()) {
                    logger.warn("Temporary API error for faction {} at page {}: {}",
                            factionInfo.getFactionId(), currentPage, response.getErrorMessage());
                    return CompletedCrimesResult.failure("Temporary API error: " + response.getErrorMessage());

                } else {
                    logger.error("API error for faction {} at page {}: {}",
                            factionInfo.getFactionId(), currentPage, response.getErrorMessage());
                    return CompletedCrimesResult.failure("API error: " + response.getErrorMessage());
                }
            }

            logger.info("Processed {} pages and {} completed crimes for faction {}",
                    currentPage, totalCrimesProcessed, factionInfo.getFactionId());
            return CompletedCrimesResult.success(totalCrimesProcessed);

        } catch (Exception e) {
            logger.error("Exception processing completed crimes for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return CompletedCrimesResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Load username mapping from ALL factions' members tables (not just current faction)
     */
    private static Map<Long, String> loadUsernameMapAcrossAllFactions() {
        Map<Long, String> usernameMap = new HashMap<>();

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null) {
            logger.warn("Cannot load usernames - DATABASE_URL_CONFIG not set");
            return usernameMap;
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger)) {
            // Get all faction suffixes
            List<String> factionSuffixes = getAllFactionSuffixes(configConnection);

            for (String factionSuffix : factionSuffixes) {
                String membersTableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionSuffix;

                try {
                    String sql = "SELECT user_id, username FROM " + membersTableName;

                    try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
                         ResultSet rs = pstmt.executeQuery()) {

                        int loadedFromThisFaction = 0;
                        while (rs.next()) {
                            try {
                                Long userId = Long.parseLong(rs.getString(Constants.COLUMN_NAME_USER_ID));
                                String username = rs.getString(Constants.COLUMN_NAME_USER_NAME);
                                if (username != null) {
                                    // Keep the most recent username if user appears in multiple factions
                                    usernameMap.put(userId, username);
                                    loadedFromThisFaction++;
                                }
                            } catch (NumberFormatException e) {
                                logger.debug("Invalid user_id format in table {}: {}",
                                        membersTableName, rs.getString(Constants.COLUMN_NAME_USER_ID));
                            }
                        }

                        logger.debug("Loaded {} usernames from faction table: {}", loadedFromThisFaction, membersTableName);
                    }

                } catch (SQLException e) {
                    logger.debug("Could not load usernames from table {} (might not exist): {}",
                            membersTableName, e.getMessage());
                }
            }

            logger.info("Loaded {} total usernames across {} factions for completed crimes processing",
                    usernameMap.size(), factionSuffixes.size());

        } catch (Exception e) {
            logger.error("Error loading cross-faction username map: {}", e.getMessage(), e);
        }

        return usernameMap;
    }

    /**
     * Get all faction suffixes from the config database
     */
    private static List<String> getAllFactionSuffixes(Connection configConnection) {
        List<String> suffixes = new ArrayList<>();

        String sql = "SELECT DISTINCT " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE oc2_enabled = true";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                if (isValidDbSuffix(dbSuffix)) {
                    suffixes.add(dbSuffix);
                } else {
                    logger.warn("Invalid or null db_suffix found: {}", dbSuffix);
                }
            }

        } catch (SQLException e) {
            logger.error("Error fetching faction suffixes from config database", e);
        }

        logger.debug("Found {} valid faction suffixes for cross-faction username lookup", suffixes.size());
        return suffixes;
    }

    /**
     * Get timestamp configuration based on environment variables and database state
     */
    private static TimestampConfig getTimestampConfig(Connection connection, String tableName) {
        int incrementalMinutes = DEFAULT_INCREMENTAL_MINUTES;

        // Check if table exists and has any data
        String countSql = "SELECT COUNT(*) as row_count FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(countSql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                int rowCount = rs.getInt("row_count");

                if (rowCount == 0) {
                    // Table exists but is empty - do initial sync
                    long fromTimestamp = Instant.now().minusSeconds(24 * 3600L).getEpochSecond(); // 24 hours
                    logger.info("Table {} is empty - performing initial sync from 24 hours ago (timestamp: {})",
                            tableName, fromTimestamp);
                    return new TimestampConfig(true, fromTimestamp);
                } else {
                    // Table has data - ALWAYS do incremental sync
                    long fromTimestamp = Instant.now().minusSeconds(incrementalMinutes * 60L).getEpochSecond();
                    logger.info("Table {} has {} records - performing incremental sync from {} minutes ago (timestamp: {})",
                            tableName, rowCount, incrementalMinutes, fromTimestamp);
                    return new TimestampConfig(false, fromTimestamp);
                }
            }
        } catch (SQLException e) {
            // Table doesn't exist yet - do initial sync
            logger.info("Table {} doesn't exist yet - performing initial sync: {}", tableName, e.getMessage());
            long fromTimestamp = Instant.now().minusSeconds(24 * 3600L).getEpochSecond(); // 24 hours
            return new TimestampConfig(true, fromTimestamp);
        }

        // Fallback - shouldn't reach here
        long fromTimestamp = Instant.now().minusSeconds(24 * 3600L).getEpochSecond();
        return new TimestampConfig(true, fromTimestamp);
    }

    /**
     * Helper to get integer from environment with default fallback
     */
    private static int getEnvironmentInt(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value != null && !value.trim().isEmpty()) {
            try {
                int parsed = Integer.parseInt(value.trim());
                logger.info("Using {} = {} from environment", envVar, parsed);
                return parsed;
            } catch (NumberFormatException e) {
                logger.warn("Invalid {} format: {}, using default: {}", envVar, value, defaultValue);
            }
        }
        return defaultValue;
    }

    /**
     * Helper class to hold timestamp configuration
     */
    private static class TimestampConfig {
        private final boolean isInitialSync;
        private final Long fromTimestamp;

        public TimestampConfig(boolean isInitialSync, Long fromTimestamp) {
            this.isInitialSync = isInitialSync;
            this.fromTimestamp = fromTimestamp;
        }

        public boolean isInitialSync() { return isInitialSync; }
        public Long getFromTimestamp() { return fromTimestamp; }
    }

    /**
     * Build API URL with 'from' parameter for timestamp filtering
     */
    private static String buildApiUrl(int currentPage, Long fromTimestamp) {
        StringBuilder url = new StringBuilder();
        url.append(Constants.API_URL_COMPLETED_FACTION_CRIMES)
                .append(currentPage)
                .append(Constants.API_URL_TORN_PARAMETER_JOIN_AND + Constants.API_URL_TORN_PARAMETER_SORT + Constants.API_URL_TORN_PARAMETER_DESC);

        if (fromTimestamp != null) {
            url.append(Constants.API_URL_TORN_PARAMETER_JOIN_AND + Constants.API_URL_TORN_PARAMETER_FROM).append(fromTimestamp);
        }

        return url.toString();
    }

    /**
     * Format timestamp for logging
     */
    private static String formatTimestamp(Long timestamp) {
        if (timestamp == null) return "null";
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT));
    }

    /**
     * Process a page of completed crimes and store in database
     */
    private static int processCompletedCrimesPage(Connection connection, String tableName, List<Crime> crimes,
                                                  FactionInfo factionInfo, Map<Long, String> usernameMap,
                                                  boolean isInitialSync) throws SQLException {

        // Use UPSERT for both initial and incremental to handle any overlaps
        String insertSql = "INSERT INTO " + tableName + " (" +
                "crime_id, faction_id, crime_name, difficulty, success, completed_at, " +
                "user_id, username, role, outcome, checkpoint_pass_rate, joined_at, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (crime_id, user_id) DO NOTHING"; // CHANGED: DO NOTHING instead of UPDATE

        String rewardsTableName = Constants.TABLE_NAME_REWARDS_CRIMES + factionInfo.getDbSuffix();
        createRewardsTableIfNotExists(connection, rewardsTableName);

        int recordsInserted = 0;
        long factionIdLong = Long.parseLong(factionInfo.getFactionId());

        // Track which crimes are newly processed (for notifications)
        Set<Long> newlyCompletedCrimes = new HashSet<>();

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (Crime crime : crimes) {
                if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
                    continue;
                }

                boolean hasResolvableUsernames = hasAnyResolvableUsernames(crime, usernameMap);
                if (!hasResolvableUsernames) {
                    continue;
                }

                boolean crimeSuccess = determineCrimeSuccess(crime);
                boolean willInsertCrimeRecords = false;
                int recordsInsertedForThisCrime = 0;

                // Process slots with role renaming logic
                Map<String, List<Slot>> groupedSlots = crime.getSlots().stream()
                        .collect(Collectors.groupingBy(Slot::getPosition));

                for (Map.Entry<String, List<Slot>> entry : groupedSlots.entrySet()) {
                    List<Slot> group = entry.getValue();
                    group.sort(Comparator.comparing(Slot::getPositionId, Comparator.nullsLast(String::compareTo)));

                    for (int i = 0; i < group.size(); i++) {
                        Slot slot = group.get(i);
                        SlotUser user = slot.getUser();

                        if (user == null || user.getId() == null) {
                            continue;
                        }

                        String username = usernameMap.get(user.getId());
                        if (username == null) {
                            continue;
                        }

                        willInsertCrimeRecords = true;

                        // Apply role renaming logic
                        String finalRole = slot.getPosition();
                        if (group.size() > 1) {
                            finalRole = slot.getPosition() + " #" + (i + 1);
                        }

                        Integer checkpointPassRate = slot.getCheckpointPassRate();

                        // Insert record
                        pstmt.setLong(1, crime.getId());
                        pstmt.setLong(2, factionIdLong);
                        pstmt.setString(3, crime.getName());
                        pstmt.setObject(4, crime.getDifficulty());
                        pstmt.setBoolean(5, crimeSuccess);
                        pstmt.setTimestamp(6, timestampFromEpoch(crime.getExecutedAt()));
                        pstmt.setLong(7, user.getId());
                        pstmt.setString(8, username);
                        pstmt.setString(9, finalRole);
                        pstmt.setString(10, user.getOutcome());
                        if (checkpointPassRate != null) {
                            pstmt.setInt(11, checkpointPassRate);
                        } else {
                            pstmt.setNull(11, java.sql.Types.INTEGER);
                        }
                        pstmt.setTimestamp(12, timestampFromEpoch(user.getJoinedAt()));

                        try {
                            int rowsAffected = pstmt.executeUpdate();
                            if (rowsAffected > 0) {
                                recordsInserted++;
                                recordsInsertedForThisCrime++;
                            }
                        } catch (SQLException e) {
                            logger.warn("Failed to insert slot for crime {}: {}", crime.getId(), e.getMessage());
                        }
                    }
                }

                // FIXED: Only send notifications if we actually inserted NEW records for this crime
                if (recordsInsertedForThisCrime > 0) {
                    newlyCompletedCrimes.add(crime.getId());

                    // Send crime completion notification for newly completed crimes
                    if (crimeSuccess) {
                        logger.info("NEW completed crime {} - sending Discord notification", crime.getId());

                        boolean notificationSent = DiscordMessages.sendCrimeComplete(
                                factionInfo.getFactionId(), crime.getName());

                        if (notificationSent) {
                            logger.info("✓ Sent crime completion notification for NEW crime {} ({})",
                                    crime.getId(), crime.getName());
                        } else {
                            logger.warn("✗ Failed to send crime completion notification for crime {}",
                                    crime.getId());
                        }
                    }

                    // Process rewards for newly completed crimes
                    if (crimeSuccess && willInsertCrimeRecords && crime.getRewards() != null) {
                        processRewards(connection, rewardsTableName, crime, factionIdLong,
                                factionInfo.getApiKey(), factionInfo.getDbSuffix(), true); // Pass flag indicating new crime
                    }
                } else {
                    logger.debug("Crime {} already exists in database - no notification sent", crime.getId());
                }
            }
        }

        return recordsInserted;
    }

    /**
     * Check if ANY users in a crime can be resolved to usernames across all factions
     */
    private static boolean hasAnyResolvableUsernames(Crime crime, Map<Long, String> usernameMap) {
        if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
            return false; // No users at all
        }

        for (Slot slot : crime.getSlots()) {
            SlotUser user = slot.getUser();
            if (user != null && user.getId() != null) {
                // Check if username exists in our cross-faction map
                if (usernameMap.containsKey(user.getId())) {
                    logger.debug("Crime {} has resolvable user: {} (found in cross-faction lookup)",
                            crime.getId(), user.getId());
                    return true; // At least one username can be resolved across all factions
                }
            }
        }

        logger.debug("Crime {} has no resolvable usernames across any faction", crime.getId());
        return false; // No usernames can be resolved across any faction
    }

    /**
     * Process and store rewards data for a successful crime
     */
    private static void processRewards(Connection connection, String rewardsTableName, Crime crime,
                                       Long factionIdLong, String apiKey, String factionSuffix, boolean isNewCrime) {
        try {
            // Parse rewards from Crime object
            if (!(crime.getRewards() instanceof Map)) {
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> rewards = (Map<String, Object>) crime.getRewards();

            // Calculate total success value from all slots
            double totalSuccessValue = calculateTotalSuccessValue(crime);

            // Extract reward components
            Number moneyReward = (Number) rewards.get(Constants.NODE_MONEY);
            long money = (moneyReward != null) ? moneyReward.longValue() : 0L;

            Number respectReward = (Number) rewards.get(Constants.NODE_RESPECT);
            int respect = (respectReward != null) ? respectReward.intValue() : 0;

            // Process items with single API call per item
            String itemsString = "";
            Long itemQuantity = null;
            long crimeValue = money;

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) rewards.get(Constants.NODE_ITEMS);
            if (items != null && !items.isEmpty()) {
                StringBuilder itemsBuilder = new StringBuilder();
                long totalItemValue = 0;
                boolean hasXanax = false;
                long totalXanaxQuantity = 0;

                for (int i = 0; i < items.size(); i++) {
                    Map<String, Object> item = items.get(i);
                    Number itemIdNum = (Number) item.get(Constants.NODE_ID);
                    Number quantityNum = (Number) item.get(Constants.NODE_QUANTITY);

                    if (itemIdNum != null && quantityNum != null) {
                        Long itemId = itemIdNum.longValue();
                        int quantity = quantityNum.intValue();

                        ItemMarketData itemData = getItemMarketData(itemId, apiKey);

                        if (itemData != null && itemData.getName() != null) {
                            // Check for Xanax while processing
                            if (itemData.getName().equalsIgnoreCase("Xanax")) {
                                hasXanax = true;
                                totalXanaxQuantity += quantity;
                            }

                            // Build items string
                            if (i > 0) itemsBuilder.append(", ");
                            itemsBuilder.append(itemData.getName());

                            // Calculate item value
                            if (itemData.getAveragePrice() != null) {
                                totalItemValue += (long) itemData.getAveragePrice() * quantity;
                            }

                            if (items.size() == 1) {
                                itemQuantity = (long) quantity;
                            }
                        }
                    }
                }

                // FIXED: Only send Xanax withdrawal notification for NEW crimes
                if (hasXanax && isNewCrime) {
                    logger.info("NEW crime {} rewarded {} Xanax - sending withdrawal notification",
                            crime.getId(), totalXanaxQuantity);

                    boolean notificationSent = DiscordMessages.sendLeaderWithdrawXanax(
                            String.valueOf(factionIdLong),
                            crime.getName(),
                            String.valueOf(totalXanaxQuantity)
                    );

                    if (notificationSent) {
                        logger.info("Sent Xanax withdrawal notification for NEW crime {} ({} Xanax)",
                                crime.getId(), totalXanaxQuantity);
                    } else {
                        logger.warn("Failed to send Xanax withdrawal notification for crime {}", crime.getId());
                    }
                } else if (hasXanax && !isNewCrime) {
                    logger.debug("Crime {} has Xanax but already exists in database - no notification sent",
                            crime.getId());
                }

                // Send item rewards notification for non-Xanax items (only for NEW crimes)
                if (isNewCrime && items != null && !items.isEmpty()) {
                    List<DiscordMessages.ItemReward> nonXanaxRewards = new ArrayList<>();

                    for (int i = 0; i < items.size(); i++) {
                        Map<String, Object> item = items.get(i);
                        Number itemIdNum = (Number) item.get(Constants.NODE_ID);
                        Number quantityNum = (Number) item.get(Constants.NODE_QUANTITY);

                        if (itemIdNum != null && quantityNum != null) {
                            Long itemId = itemIdNum.longValue();
                            int quantity = quantityNum.intValue();

                            ItemMarketData itemData = getItemMarketData(itemId, apiKey);

                            if (itemData != null && itemData.getName() != null) {
                                // Skip Xanax - we already handle that separately
                                if (!itemData.getName().equalsIgnoreCase("Xanax")) {
                                    nonXanaxRewards.add(new DiscordMessages.ItemReward(
                                            itemData.getName(),
                                            quantity,
                                            itemData.getAveragePrice()
                                    ));
                                }
                            }
                        }
                    }

                    // Send notification if we have non-Xanax items
                    if (!nonXanaxRewards.isEmpty()) {
                        logger.info("NEW crime {} rewarded {} non-Xanax items - sending item rewards notification",
                                crime.getId(), nonXanaxRewards.size());

                        boolean itemNotificationSent = DiscordMessages.sendLeaderItemRewards(
                                String.valueOf(factionIdLong),
                                crime.getName(),
                                nonXanaxRewards
                        );

                        if (itemNotificationSent) {
                            logger.info("✓ Sent item rewards notification for NEW crime {} ({} items)",
                                    crime.getId(), nonXanaxRewards.size());
                        } else {
                            logger.warn("✗ Failed to send item rewards notification for crime {}", crime.getId());
                        }
                    }
                }

                itemsString = itemsBuilder.toString();
                crimeValue += totalItemValue;
            }

            // Format completed date for rewards table consistency
            String completedDateFriendly = formatFriendlyDate(crime.getExecutedAt());

            // Use UPSERT for rewards but with DO NOTHING for existing records
            String insertRewardsSql = "INSERT INTO " + rewardsTableName + " (" +
                    "crime_id, faction_id, crime_name, total_members_required, total_success_value, " +
                    "completed_date, crime_value, items, item_quantity, respect_earnt, last_updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                    "ON CONFLICT (crime_id) DO NOTHING"; // CHANGED: DO NOTHING instead of UPDATE

            try (PreparedStatement rewardsPstmt = connection.prepareStatement(insertRewardsSql)) {
                rewardsPstmt.setLong(1, crime.getId());
                rewardsPstmt.setLong(2, factionIdLong);
                rewardsPstmt.setString(3, crime.getName());
                rewardsPstmt.setInt(4, crime.getSlots().size());
                rewardsPstmt.setDouble(5, totalSuccessValue);
                rewardsPstmt.setString(6, completedDateFriendly);
                rewardsPstmt.setLong(7, crimeValue);
                rewardsPstmt.setString(8, itemsString.isEmpty() ? null : itemsString);
                if (itemQuantity != null) {
                    rewardsPstmt.setLong(9, itemQuantity);
                } else {
                    rewardsPstmt.setNull(9, java.sql.Types.BIGINT);
                }
                rewardsPstmt.setInt(10, respect);

                int rowsAffected = rewardsPstmt.executeUpdate();
                if (rowsAffected > 0) {
                    logger.debug("Inserted NEW rewards data for crime {}", crime.getId());
                } else {
                    logger.debug("Rewards data already exists for crime {}", crime.getId());
                }
            }

        } catch (Exception e) {
            logger.error("Error processing rewards for crime {}: {}", crime.getId(), e.getMessage(), e);
        }
    }


    /**
     * Data holder for item market information
     */
    private static class ItemMarketData {
        private final String name;
        private final Integer averagePrice;

        public ItemMarketData(String name, Integer averagePrice) {
            this.name = name;
            this.averagePrice = averagePrice;
        }

        public String getName() { return name; }
        public Integer getAveragePrice() { return averagePrice; }
    }

    // Cache for item market data to avoid repeated API calls within the same run
    private static final ConcurrentHashMap<Long, ItemMarketData> itemMarketCache = new ConcurrentHashMap<>();

    /**
     * Get both item name and price from market data using a single API call
     */
    private static ItemMarketData getItemMarketData(Long itemId, String apiKey) {
        if (itemId == null) {
            return null;
        }

        // Check cache first
        ItemMarketData cachedData = itemMarketCache.get(itemId);
        if (cachedData != null) {
            return cachedData;
        }

        try {
            if (apiKey == null) {
                logger.debug("No API key provided for item market lookup");
                return null;
            }

            String itemUrl = Constants.API_URL_MARKET + "/" + itemId + Constants.API_URL_ITEM_MARKET;

            ApiResponse response = TornApiHandler.executeRequest(itemUrl, apiKey);

            if (response.isSuccess()) {
                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse marketResponse =
                        objectMapper.readValue(response.getBody(),
                                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse.class);

                if (marketResponse != null && marketResponse.getItemMarket() != null) {
                    Item item = marketResponse.getItemMarket();
                    ItemMarketData itemData = new ItemMarketData(item.getName(), item.getAveragePrice());

                    // Cache the result
                    itemMarketCache.put(itemId, itemData);

                    logger.debug("Fetched and cached item data for ID {}: {} (${})",
                            itemId, itemData.getName(), itemData.getAveragePrice());

                    return itemData;
                }
            } else {
                logger.debug("Failed to fetch item market data for ID {}: {}", itemId, response.getErrorMessage());
            }
        } catch (Exception e) {
            logger.debug("Could not fetch item market data for ID {}: {}", itemId, e.getMessage());
        }

        // Cache null result to avoid repeated failed calls
        itemMarketCache.put(itemId, new ItemMarketData(null, null));
        return null;
    }

    /**
     * Calculate total success value from all slots using checkpoint_pass_rate
     */
    private static double calculateTotalSuccessValue(Crime crime) {
        if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
            return 0.0;
        }

        double totalPassRate = 0.0;
        int validSlots = 0;

        for (Slot slot : crime.getSlots()) {
            if (slot.getCheckpointPassRate() != null) {
                // checkpoint_pass_rate is already a percentage (0-100)
                totalPassRate += slot.getCheckpointPassRate();
                validSlots++;
            }
        }

        return totalPassRate;
    }

    /**
     * Create rewards table if it doesn't exist
     */
    private static void createRewardsTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "crime_id BIGINT PRIMARY KEY," +
                "faction_id BIGINT NOT NULL," +
                "crime_name VARCHAR(255) NOT NULL," +
                "total_members_required INTEGER NOT NULL," +
                "total_success_value DOUBLE PRECISION NOT NULL," +
                "completed_date VARCHAR(50) NOT NULL," +
                "crime_value BIGINT NOT NULL," +
                "items TEXT," +
                "item_quantity BIGINT," +
                "respect_earnt INTEGER NOT NULL," +
                "member_payout_percentage DOUBLE PRECISION," +
                "member_payout_amount BIGINT," +
                "paid_date VARCHAR(50)," +
                "faction_payout BIGINT," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better query performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_completed_date ON " + tableName + "(completed_date)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_paid_date ON " + tableName + "(paid_date)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_crime_value ON " + tableName + "(crime_value DESC)");

            logger.debug("Rewards table {} created or verified with indexes", tableName);
        }
    }

    /**
     * Determine if a crime was successful based on crime status
     */
    private static boolean determineCrimeSuccess(Crime crime) {
        return "Successful".equalsIgnoreCase(crime.getStatus());
    }

    /**
     * Format timestamp to friendly date string
     */
    private static String formatFriendlyDate(Long epochSeconds) {
        if (epochSeconds == null) {
            return null;
        }

        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(epochSeconds),
                ZoneId.systemDefault()
        );

        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Create completed crimes table with updated schema (removed completed_date_friendly, added joined_at)
     */
    private static void createCompletedCrimesTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "crime_id BIGINT NOT NULL," +
                "faction_id BIGINT NOT NULL," +
                "crime_name VARCHAR(255) NOT NULL," +
                "difficulty INTEGER," +
                "success BOOLEAN NOT NULL," +
                "completed_at TIMESTAMP," +
                "user_id BIGINT NOT NULL," +
                "username VARCHAR(100) NOT NULL," +
                "role VARCHAR(100) NOT NULL," +
                "outcome VARCHAR(50)," +
                "checkpoint_pass_rate INTEGER," +
                "joined_at TIMESTAMP," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (crime_id, user_id)" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better query performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_id ON " + tableName + "(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_completed_at ON " + tableName + "(completed_at DESC)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_success ON " + tableName + "(success)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_user_id ON " + tableName + "(user_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_crime_name ON " + tableName + "(crime_name)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_joined_at ON " + tableName + "(joined_at DESC)");

            logger.debug("Table {} created or verified with indexes", tableName);
        }
    }

    // Utility methods
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }

    private static Timestamp timestampFromEpoch(Long epochSeconds) {
        if (epochSeconds == null) return null;
        return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
    }

    /**
     * Result wrapper for completed crimes processing
     */
    private static class CompletedCrimesResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int crimesProcessed;
        private final String errorMessage;

        private CompletedCrimesResult(boolean success, boolean circuitBreakerOpen, int crimesProcessed, String errorMessage) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.crimesProcessed = crimesProcessed;
            this.errorMessage = errorMessage;
        }

        public static CompletedCrimesResult success(int crimesProcessed) {
            return new CompletedCrimesResult(true, false, crimesProcessed, null);
        }

        public static CompletedCrimesResult failure(String errorMessage) {
            return new CompletedCrimesResult(false, false, 0, errorMessage);
        }

        public static CompletedCrimesResult circuitBreakerOpen() {
            return new CompletedCrimesResult(false, true, 0, "Circuit breaker is open");
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getCrimesProcessed() { return crimesProcessed; }
        public String getErrorMessage() { return errorMessage; }
    }
}