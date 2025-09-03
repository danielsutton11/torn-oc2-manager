package com.Torn.FactionCrimes.Completed;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.FactionCrimes.Models.CrimesModel.Slot;
import com.Torn.FactionCrimes.Models.CrimesModel.SlotUser;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // Constants for pagination and incremental updates
    private static final int CRIMES_PER_PAGE = 10; // Torn API default
    private static final int MAX_PAGES_INITIAL = 100; // Reasonable limit for initial sync
    private static final int MAX_PAGES_INCREMENTAL = 5; // For 15-minute incremental updates

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

    public static class CompletedCrimeUser {
        private final Long userId;
        private final String username;
        private final String role;
        private final String outcome;
        private final Double progress;

        public CompletedCrimeUser(Long userId, String username, String role, String outcome, Double progress) {
            this.userId = userId;
            this.username = username;
            this.role = role;
            this.outcome = outcome;
            this.progress = progress;
        }

        public Long getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getRole() { return role; }
        public String getOutcome() { return outcome; }
        public Double getProgress() { return progress; }
    }

    /**
     * Main entry point for fetching completed crimes for all factions
     */
    public static void fetchAndProcessAllCompletedCrimes() throws SQLException, IOException {
        logger.info("Starting completed crimes fetch for all factions with robust API handling");

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
                        logger.info("✓ Successfully processed {} completed crimes for faction {} ({})",
                                result.getCrimesProcessed(), factionInfo.getFactionId(),
                                factionInfo.getOwnerName());
                        successfulCount++;
                        totalCrimesProcessed += result.getCrimesProcessed();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("✗ Failed to process completed crimes for faction {} ({}): {}",
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
                logger.error("⚠ More than half of factions failed - Torn API may be experiencing issues");
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
            String tableName = "c_crimes_" + factionInfo.getDbSuffix();
            createCompletedCrimesTableIfNotExists(connection, tableName);

            // Check if this is initial sync or incremental update
            boolean isInitialSync = isInitialSync(connection, tableName);
            int maxPages = isInitialSync ? MAX_PAGES_INITIAL : MAX_PAGES_INCREMENTAL;

            logger.info("Processing completed crimes for faction {} ({})",
                    factionInfo.getFactionId(), isInitialSync ? "Initial sync" : "Incremental update");

            int totalCrimesProcessed = 0;
            int currentPage = 0;
            boolean hasMoreData = true;

            // Load username lookup map from members table
            Map<Long, String> usernameMap = loadUsernameMap(factionInfo);

            while (hasMoreData && currentPage < maxPages) {
                // Construct API URL with pagination
                String apiUrl = Constants.API_URL_TORN_BASE_URL + "faction/crimes?cat=completed&offset=" +
                        currentPage + "&sort=DESC&key=" + factionInfo.getApiKey();

                logger.debug("Fetching completed crimes page {} for faction: {}", currentPage, factionInfo.getFactionId());

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
                            .filter(crime -> "completed".equalsIgnoreCase(crime.getStatus()) ||
                                    crime.getExecutedAt() != null)
                            .collect(Collectors.toList());

                    if (completedCrimes.isEmpty()) {
                        logger.debug("No completed crimes in page {} for faction {}", currentPage, factionInfo.getFactionId());
                        currentPage++;
                        continue;
                    }

                    // Process and store crimes
                    int crimesInPage = processCompletedCrimesPage(connection, tableName, completedCrimes,
                            factionInfo, usernameMap);
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
     * Load username mapping from members table
     */
    private static Map<Long, String> loadUsernameMap(FactionInfo factionInfo) {
        Map<Long, String> usernameMap = new HashMap<>();
        String membersTableName = Constants.FACTION_MEMBERS_TABLE_PREFIX + factionInfo.getDbSuffix();

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null) {
            logger.warn("Cannot load usernames - DATABASE_URL_CONFIG not set");
            return usernameMap;
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger)) {
            String sql = "SELECT user_id, username FROM " + membersTableName;

            try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {

                while (rs.next()) {
                    try {
                        Long userId = Long.parseLong(rs.getString("user_id"));
                        String username = rs.getString("username");
                        if (username != null) {
                            usernameMap.put(userId, username);
                        }
                    } catch (NumberFormatException e) {
                        logger.debug("Invalid user_id format in members table: {}", rs.getString("user_id"));
                    }
                }

                logger.debug("Loaded {} usernames for faction {}", usernameMap.size(), factionInfo.getFactionId());
            }

        } catch (Exception e) {
            logger.warn("Error loading username map for faction {}: {}", factionInfo.getFactionId(), e.getMessage());
        }

        return usernameMap;
    }

    /**
     * Check if this is the initial sync (table is empty or doesn't exist)
     */
    private static boolean isInitialSync(Connection connection, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName + " LIMIT 1";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        } catch (SQLException e) {
            // Table might not exist yet
            logger.debug("Table {} might not exist yet, treating as initial sync", tableName);
            return true;
        }

        return true;
    }

    /**
     * Process a page of completed crimes and store in database
     */
    private static int processCompletedCrimesPage(Connection connection, String tableName, List<Crime> crimes,
                                                  FactionInfo factionInfo, Map<Long, String> usernameMap) throws SQLException {

        String insertSql = "INSERT INTO " + tableName + " (" +
                "crime_id, faction_id, crime_name, difficulty, success, completed_at, completed_date_friendly, " +
                "user_id, username, role, outcome, checkpoint_pass_rate, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (crime_id, user_id) DO UPDATE SET " +
                "username = EXCLUDED.username, role = EXCLUDED.role, outcome = EXCLUDED.outcome, " +
                "checkpoint_pass_rate = EXCLUDED.checkpoint_pass_rate, last_updated = CURRENT_TIMESTAMP";

        String rewardsTableName = "r_crimes_" + factionInfo.getDbSuffix();
        createRewardsTableIfNotExists(connection, rewardsTableName);

        int recordsInserted = 0;
        Long factionIdLong = Long.parseLong(factionInfo.getFactionId());

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (Crime crime : crimes) {
                if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
                    continue;
                }

                // Check if ANY usernames can be resolved for this crime
                boolean hasResolvableUsernames = hasAnyResolvableUsernames(crime, usernameMap);
                if (!hasResolvableUsernames) {
                    logger.debug("Skipping crime {} - no resolvable usernames found", crime.getId());
                    continue;
                }

                // Determine crime success and format date
                boolean crimeSuccess = determineCrimeSuccess(crime);
                String completedDateFriendly = formatFriendlyDate(crime.getExecutedAt());

                // Track if we'll be inserting any records for this crime
                boolean willInsertCrimeRecords = false;

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

                        // Only process users whose names we can resolve
                        String username = usernameMap.get(user.getId());
                        if (username == null) {
                            logger.debug("Skipping user {} in crime {} - username not resolvable", user.getId(), crime.getId());
                            continue;
                        }

                        willInsertCrimeRecords = true;

                        // Apply role renaming logic (same as GetAvailableCrimes)
                        String finalRole = slot.getPosition();
                        if (group.size() > 1) {
                            finalRole = slot.getPosition() + " #" + (i + 1);
                        }

                        // Get checkpoint pass rate from slot (already a percentage 0-100)
                        Integer checkpointPassRate = slot.getCheckpointPassRate();

                        // Insert record
                        pstmt.setLong(1, crime.getId());
                        pstmt.setLong(2, factionIdLong);
                        pstmt.setString(3, crime.getName());
                        pstmt.setObject(4, crime.getDifficulty());
                        pstmt.setBoolean(5, crimeSuccess);
                        pstmt.setTimestamp(6, timestampFromEpoch(crime.getExecutedAt()));
                        pstmt.setString(7, completedDateFriendly);
                        pstmt.setLong(8, user.getId());
                        pstmt.setString(9, username);
                        pstmt.setString(10, finalRole);
                        pstmt.setString(11, user.getOutcome());
                        if (checkpointPassRate != null) {
                            pstmt.setInt(12, checkpointPassRate);
                        } else {
                            pstmt.setNull(12, java.sql.Types.INTEGER);
                        }

                        pstmt.addBatch();
                        recordsInserted++;
                    }
                }

                // Process rewards if crime was successful and we're inserting records for it
                if (crimeSuccess && willInsertCrimeRecords && crime.getRewards() != null) {
                    processRewards(connection, rewardsTableName, crime, factionIdLong, completedDateFriendly, factionInfo.getApiKey());
                }
            }

            pstmt.executeBatch();
        }

        return recordsInserted;
    }

    /**
     * Process and store rewards data for a successful crime
     */
    private static void processRewards(Connection connection, String rewardsTableName, Crime crime,
                                       Long factionIdLong, String completedDateFriendly, String apiKey) {
        try {
            // Parse rewards from Crime object
            if (!(crime.getRewards() instanceof Map)) {
                logger.debug("Rewards data is not in expected format for crime {}", crime.getId());
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> rewards = (Map<String, Object>) crime.getRewards();

            // Calculate total success value from all slots
            double totalSuccessValue = calculateTotalSuccessValue(crime);

            // Extract reward components
            Number moneyReward = (Number) rewards.get("money");
            long money = (moneyReward != null) ? moneyReward.longValue() : 0L;

            Number respectReward = (Number) rewards.get("respect");
            int respect = (respectReward != null) ? respectReward.intValue() : 0;

            // Process items
            String itemsString = "";
            Long itemQuantity = null;
            long crimeValue = money; // Start with money value

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) rewards.get("items");
            if (items != null && !items.isEmpty()) {
                StringBuilder itemsBuilder = new StringBuilder();
                long totalItemValue = 0;

                for (int i = 0; i < items.size(); i++) {
                    Map<String, Object> item = items.get(i);
                    Number itemIdNum = (Number) item.get("id");
                    Number quantityNum = (Number) item.get("quantity");

                    if (itemIdNum != null && quantityNum != null) {
                        Long itemId = itemIdNum.longValue();
                        int quantity = quantityNum.intValue();

                        // Get item name and price from market data using the faction's API key
                        String itemName = getItemNameFromMarket(itemId, apiKey);
                        Integer itemPrice = getItemPriceFromMarket(itemId, apiKey);

                        if (itemName != null) {
                            if (i > 0) itemsBuilder.append(", ");
                            itemsBuilder.append(itemName);

                            if (itemPrice != null) {
                                totalItemValue += (long) itemPrice * quantity;
                            }

                            // If there's only one item type, store its quantity
                            if (items.size() == 1) {
                                itemQuantity = (long) quantity;
                            }
                        }
                    }
                }

                itemsString = itemsBuilder.toString();
                crimeValue += totalItemValue;
            }

            // Insert rewards record
            String insertRewardsSql = "INSERT INTO " + rewardsTableName + " (" +
                    "crime_id, faction_id, crime_name, total_members_required, total_success_value, " +
                    "completed_date, crime_value, items, item_quantity, respect_earnt, last_updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                    "ON CONFLICT (crime_id) DO UPDATE SET " +
                    "crime_value = EXCLUDED.crime_value, items = EXCLUDED.items, " +
                    "item_quantity = EXCLUDED.item_quantity, respect_earnt = EXCLUDED.respect_earnt, " +
                    "last_updated = CURRENT_TIMESTAMP";

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

                rewardsPstmt.executeUpdate();
                logger.debug("Inserted rewards data for crime {}", crime.getId());
            }

        } catch (Exception e) {
            logger.error("Error processing rewards for crime {}: {}", crime.getId(), e.getMessage(), e);
        }
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
                // checkpoint_pass_rate is already a percentage (0-100), don't multiply
                totalPassRate += slot.getCheckpointPassRate();
                validSlots++;
            }
        }

        return totalPassRate;
    }

    /**
     * Get item name from market data using the provided API key
     */
    private static String getItemNameFromMarket(Long itemId, String apiKey) {
        try {
            if (apiKey == null) {
                logger.debug("No API key provided for item market lookup");
                return null;
            }

            String itemUrl = Constants.API_URL_ITEM_MARKET + itemId + Constants.API_URL_ITEM_MARKET_JOIN;
            ApiResponse response = TornApiHandler.executeRequest(itemUrl, apiKey);

            if (response.isSuccess()) {
                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse marketResponse =
                        objectMapper.readValue(response.getBody(),
                                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse.class);

                if (marketResponse != null && marketResponse.getItemMarket() != null) {
                    return marketResponse.getItemMarket().getName();
                }
            }
        } catch (Exception e) {
            logger.debug("Could not fetch item name for ID {}: {}", itemId, e.getMessage());
        }
        return null;
    }

    /**
     * Get item price from market data using the provided API key
     */
    private static Integer getItemPriceFromMarket(Long itemId, String apiKey) {
        try {
            if (apiKey == null) {
                logger.debug("No API key provided for item market lookup");
                return null;
            }

            String itemUrl = Constants.API_URL_ITEM_MARKET + itemId + Constants.API_URL_ITEM_MARKET_JOIN;
            ApiResponse response = TornApiHandler.executeRequest(itemUrl, apiKey);

            if (response.isSuccess()) {
                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse marketResponse =
                        objectMapper.readValue(response.getBody(),
                                com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse.class);

                if (marketResponse != null && marketResponse.getItemMarket() != null) {
                    return marketResponse.getItemMarket().getAveragePrice();
                }
            }
        } catch (Exception e) {
            logger.debug("Could not fetch item price for ID {}: {}", itemId, e.getMessage());
        }
        return null;
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
     * Check if ANY users in a crime can be resolved to usernames
     */
    private static boolean hasAnyResolvableUsernames(Crime crime, Map<Long, String> usernameMap) {
        if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
            return false; // No users at all
        }

        for (Slot slot : crime.getSlots()) {
            SlotUser user = slot.getUser();
            if (user != null && user.getId() != null) {
                // Check if username exists in our map
                if (usernameMap.containsKey(user.getId())) {
                    return true; // At least one username can be resolved
                }
            }
        }

        return false; // No usernames can be resolved
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
     * Create completed crimes table with correct column name
     */
    private static void createCompletedCrimesTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "crime_id BIGINT NOT NULL," +
                "faction_id BIGINT NOT NULL," +
                "crime_name VARCHAR(255) NOT NULL," +
                "difficulty INTEGER," +
                "success BOOLEAN NOT NULL," +
                "completed_at TIMESTAMP," +
                "completed_date_friendly VARCHAR(50)," +
                "user_id BIGINT NOT NULL," +
                "username VARCHAR(100) NOT NULL," +
                "role VARCHAR(100) NOT NULL," +
                "outcome VARCHAR(50)," +
                "checkpoint_pass_rate INTEGER," + // Changed from progress_percentage to checkpoint_pass_rate
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

            logger.debug("Table {} created or verified with indexes", tableName);
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