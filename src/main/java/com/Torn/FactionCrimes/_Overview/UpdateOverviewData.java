package com.Torn.FactionCrimes._Overview;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.FactionCrimes.Models.CrimesModel.Slot;
import com.Torn.FactionCrimes.Models.CrimesModel.SlotUser;
import com.Torn.FactionCrimes.Models.ItemMarketModel.Item;
import com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UpdateOverviewData {

    private static final Logger logger = LoggerFactory.getLogger(UpdateOverviewData.class);
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

    public static class FactionMember {
        private final String userId;
        private final String username;
        private final boolean userInDiscord;

        public FactionMember(String userId, String username, boolean userInDiscord) {
            this.userId = userId;
            this.username = username;
            this.userInDiscord = userInDiscord;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public boolean isUserInDiscord() { return userInDiscord; }
    }

    public static class OverviewRecord {
        private final String userId;
        private final String username;
        private final boolean userInDiscord;
        private final boolean inOrganisedCrime;
        private final Long crimeId;
        private final String crimeName;
        private final String crimeStatus;
        private final Integer crimeDifficulty;
        private final String role;
        private final Integer checkpointPassRate;
        private final String itemRequired;
        private final Boolean itemIsReusable;
        private final Boolean userHasItem;
        private final Boolean crimeHasAllMembers;
        private final Timestamp crimeCompletionDate;
        private final Integer itemAveragePrice;

        public OverviewRecord(String userId, String username, boolean userInDiscord, boolean inOrganisedCrime,
                              Long crimeId,String crimeName, String crimeStatus, Integer crimeDifficulty, String role,
                              Integer checkpointPassRate, String itemRequired, Boolean itemIsReusable,
                              Boolean userHasItem, Boolean crimeHasAllMembers, Timestamp crimeCompletionDate,
                              Integer itemAveragePrice) {
            this.userId = userId;
            this.username = username;
            this.userInDiscord = userInDiscord;
            this.inOrganisedCrime = inOrganisedCrime;
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.crimeStatus = crimeStatus;
            this.crimeDifficulty = crimeDifficulty;
            this.role = role;
            this.checkpointPassRate = checkpointPassRate;
            this.itemRequired = itemRequired;
            this.itemIsReusable = itemIsReusable;
            this.userHasItem = userHasItem;
            this.crimeHasAllMembers = crimeHasAllMembers;
            this.crimeCompletionDate = crimeCompletionDate;
            this.itemAveragePrice = itemAveragePrice;
        }

        // Getters
        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public boolean isUserInDiscord() { return userInDiscord; }
        public boolean isInOrganisedCrime() { return inOrganisedCrime; }
        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getCrimeStatus() { return crimeStatus; }
        public Integer getCrimeDifficulty() { return crimeDifficulty; }
        public String getRole() { return role; }
        public Integer getCheckpointPassRate() { return checkpointPassRate; }
        public String getItemRequired() { return itemRequired; }
        public Boolean getItemIsReusable() { return itemIsReusable; }
        public Boolean getUserHasItem() { return userHasItem; }
        public Boolean getCrimeHasAllMembers() { return crimeHasAllMembers; }
        public Timestamp getCrimeCompletionDate() { return crimeCompletionDate; }
        public Integer getItemAveragePrice() { return itemAveragePrice; }
    }

    public static class DiscordNotificationData {
        private final String factionId;
        private final String factionSuffix;
        private final List<UserJoinedWithItemData> usersJoinedWithItems;

        public DiscordNotificationData(String factionId, String factionSuffix, List<UserJoinedWithItemData> usersJoinedWithItems) {
            this.factionId = factionId;
            this.factionSuffix = factionSuffix;
            this.usersJoinedWithItems = usersJoinedWithItems;
        }

        public String getFactionId() { return factionId; }
        public String getFactionSuffix() { return factionSuffix; }
        public List<UserJoinedWithItemData> getUsersJoinedWithItems() { return usersJoinedWithItems; }

        public static class UserJoinedWithItemData {
            private final String userId;
            private final String username;
            private final Long crimeId;
            private final String role;
            private final String itemRequired;
            private final Integer itemAveragePrice;

            public UserJoinedWithItemData(String userId, String username, Long crimeId, String role,
                                          String itemRequired, Integer itemAveragePrice) {
                this.userId = userId;
                this.username = username;
                this.crimeId = crimeId;
                this.role = role;
                this.itemRequired = itemRequired;
                this.itemAveragePrice = itemAveragePrice;
            }

            public String getUserId() { return userId; }
            public String getUsername() { return username; }
            public Long getCrimeId() { return crimeId; }
            public String getRole() { return role; }
            public String getItemRequired() { return itemRequired; }
            public Integer getItemAveragePrice() { return itemAveragePrice; }
        }
    }

    // Cache for item market data to avoid repeated API calls
    private static final ConcurrentHashMap<Long, Item> itemCache = new ConcurrentHashMap<>();

    /**
     * Main entry point for updating overview data for all factions
     */
    public static void updateAllFactionsOverviewData() throws SQLException, IOException {
        logger.info("Starting overview data update for all factions");

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
                logger.error("Circuit breaker is OPEN - skipping overview data update to prevent further failures");
                return;
            }

            // Get faction information from config database
            List<FactionInfo> factions = getFactionInfo(configConnection);
            if (factions.isEmpty()) {
                logger.warn("No active OC2-enabled factions found for overview data update");
                return;
            }

            logger.info("Found {} OC2-enabled factions to process overview data for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            List<DiscordNotificationData> allNotifications = new ArrayList<>();

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing overview data for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Process overview data for this faction
                    OverviewUpdateResult result = updateFactionOverviewData(configConnection, ocDataConnection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("Successfully updated overview data for faction {} ({}) - {} records",
                                factionInfo.getFactionId(), factionInfo.getOwnerName(), result.getRecordsProcessed());
                        successfulCount++;

                        // Collect notification data if any
                        if (result.getNotificationData() != null && !result.getNotificationData().getUsersJoinedWithItems().isEmpty()) {
                            allNotifications.add(result.getNotificationData());
                        }
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("Failed to update overview data for faction {} ({}): {}",
                                factionInfo.getFactionId(), factionInfo.getOwnerName(), result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                    // Rate limiting between factions
                    if (processedCount < factions.size()) {
                        logger.debug("Waiting {}ms before processing next faction", TORN_API_RATE_LIMIT_MS);
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("Overview data processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Unexpected error processing overview data for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Send Discord notifications for all factions that had users join with items
            if (!allNotifications.isEmpty()) {
                sendDiscordNotifications(allNotifications);
            }

            // Final summary
            logger.info("Overview data update completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Discord notifications sent for: {} factions", allNotifications.size());

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

            if (failedCount > successfulCount && processedCount > 2) {
                logger.error("More than half of factions failed - Torn API may be experiencing issues");
            }

        } catch (SQLException e) {
            logger.error("Database error during overview data update", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database (OC2 enabled factions only)
     */
    private static List<FactionInfo> getFactionInfo(Connection configConnection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

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

        logger.info("Found {} OC2-enabled factions for overview data processing", factions.size());
        return factions;
    }

    /**
     * Update overview data for a single faction
     */
    private static OverviewUpdateResult updateFactionOverviewData(Connection configConnection, Connection ocDataConnection,
                                                                  FactionInfo factionInfo) {
        try {
            String overviewTableName = Constants.TABLE_NAME_OVERVIEW + factionInfo.getDbSuffix();

            // Get existing overview data before updating (for comparison)
            List<OverviewRecord> existingRecords = getExistingOverviewData(ocDataConnection, overviewTableName);

            // Get faction members
            List<FactionMember> members = getFactionMembers(configConnection, factionInfo);
            if (members.isEmpty()) {
                logger.warn("No members found for faction {}", factionInfo.getFactionId());
                return OverviewUpdateResult.success(0, null);
            }

            // Fetch current available crimes from API
            List<Crime> availableCrimes = fetchAvailableCrimes(factionInfo);

            // Build new overview records
            List<OverviewRecord> newRecords = buildOverviewRecords(members, availableCrimes, factionInfo);

            // Create/update overview table
            createOverviewTableIfNotExists(ocDataConnection, overviewTableName);

            // Compare existing vs new data to identify users who joined crimes with items
            DiscordNotificationData notificationData = compareAndIdentifyChanges(existingRecords, newRecords, factionInfo);

            // Update the table with new data
            updateOverviewTable(ocDataConnection, overviewTableName, newRecords);

            logger.info("Successfully updated overview table {} with {} records for faction {}",
                    overviewTableName, newRecords.size(), factionInfo.getFactionId());

            return OverviewUpdateResult.success(newRecords.size(), notificationData);

        } catch (Exception e) {
            logger.error("Exception updating overview data for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return OverviewUpdateResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Get existing overview data from the table (before updating)
     */
    private static List<OverviewRecord> getExistingOverviewData(Connection ocDataConnection, String tableName) {
        List<OverviewRecord> existingRecords = new ArrayList<>();

        String sql = "SELECT user_id, username, user_in_discord, in_organised_crime, crime_id, crime_name, crime_status, " +
                "crime_difficulty, role, checkpoint_pass_rate, item_required, item_is_reusable, " +
                "user_has_item, crime_has_all_members, crime_completion_date, item_average_price " +
                "FROM " + tableName;

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                OverviewRecord record = new OverviewRecord(
                        rs.getString(Constants.COLUMN_NAME_USER_ID),
                        rs.getString(Constants.COLUMN_NAME_USER_NAME),
                        rs.getBoolean(Constants.COLUMN_NAME_USER_IN_DISCORD),
                        rs.getBoolean(Constants.COLUMN_NAME_IN_ORGANISED_CRIME),
                        rs.getObject(Constants.COLUMN_NAME_CRIME_ID, Long.class),
                        rs.getString(Constants.COLUMN_NAME_CRIME_NAME),
                        rs.getString(Constants.COLUMN_NAME_CRIME_STATUS),
                        rs.getObject(Constants.COLUMN_NAME_CRIME_DIFFICULTY, Integer.class),
                        rs.getString(Constants.COLUMN_NAME_ROLE),
                        rs.getObject(Constants.COLUMN_NAME_CHECKPOINT_PASS_RATE, Integer.class),
                        rs.getString(Constants.COLUMN_NAME_ITEM_REQUIRED),
                        rs.getObject(Constants.COLUMN_NAME_ITEM_IS_REUSABLE, Boolean.class),
                        rs.getObject(Constants.COLUMN_NAME_USER_HAS_ITEM, Boolean.class),
                        rs.getObject(Constants.COLUMN_NAME_CRIME_HAS_ALL_MEMBERS, Boolean.class),
                        rs.getTimestamp(Constants.COLUMN_NAME_CRIME_COMPLETION_DATE),
                        rs.getObject(Constants.COLUMN_NAME_ITEM_AVERAGE_PRICE, Integer.class)
                );
                existingRecords.add(record);
            }

        } catch (SQLException e) {
            logger.debug("Could not read existing overview data from table {} (might not exist): {}",
                    tableName, e.getMessage());
        }

        logger.debug("Loaded {} existing overview records from table {}", existingRecords.size(), tableName);
        return existingRecords;
    }

    /**
     * Compare existing vs new data and identify users who joined crimes with items they have
     */
    private static DiscordNotificationData compareAndIdentifyChanges(List<OverviewRecord> existingRecords,
                                                                     List<OverviewRecord> newRecords,
                                                                     FactionInfo factionInfo) {

        // Create maps for easy lookup
        Map<String, OverviewRecord> existingMap = existingRecords.stream()
                .collect(Collectors.toMap(OverviewRecord::getUserId, record -> record, (existing, replacement) -> existing));

        List<DiscordNotificationData.UserJoinedWithItemData> usersJoinedWithItems = new ArrayList<>();

        for (OverviewRecord newRecord : newRecords) {
            // Only process users who are now in an organised crime
            if (!newRecord.isInOrganisedCrime()) {
                continue;
            }

            OverviewRecord existingRecord = existingMap.get(newRecord.getUserId());

            // Check if this user was NOT in a crime previously and is now in a crime with an item they have
            boolean wasNotInCrime = (existingRecord == null || !existingRecord.isInOrganisedCrime());
            boolean isNowInCrime = newRecord.isInOrganisedCrime();
            boolean userHasItem = Boolean.TRUE.equals(newRecord.getUserHasItem());

            if (wasNotInCrime && isNowInCrime && userHasItem && newRecord.getItemRequired() != null) {
                usersJoinedWithItems.add(new DiscordNotificationData.UserJoinedWithItemData(
                        newRecord.getUserId(),
                        newRecord.getUsername(),
                        newRecord.getCrimeId(),
                        newRecord.getRole(),
                        newRecord.getItemRequired(),
                        newRecord.getItemAveragePrice()
                ));

                logger.info("Detected user {} joined crime {} with item {} they already have (value: ${})",
                        newRecord.getUsername(), newRecord.getCrimeId(), newRecord.getItemRequired(),
                        newRecord.getItemAveragePrice() != null ? newRecord.getItemAveragePrice() : "unknown");
            }
        }

        if (!usersJoinedWithItems.isEmpty()) {
            logger.info("Found {} users who joined crimes with items they already have for faction {}",
                    usersJoinedWithItems.size(), factionInfo.getFactionId());
            return new DiscordNotificationData(factionInfo.getFactionId(), factionInfo.getDbSuffix(), usersJoinedWithItems);
        }

        return null; // No notifications needed
    }

    /**
     * Get faction members from the config database
     */
    private static List<FactionMember> getFactionMembers(Connection configConnection, FactionInfo factionInfo) throws SQLException {
        List<FactionMember> members = new ArrayList<>();
        String membersTableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionInfo.getDbSuffix();

        String sql = "SELECT " + Constants.COLUMN_NAME_USER_ID + ", " + Constants.COLUMN_NAME_USER_NAME + ", user_in_discord " +
                "FROM " + membersTableName + " " +
                "ORDER BY " + Constants.COLUMN_NAME_USER_NAME;

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String userId = rs.getString(Constants.COLUMN_NAME_USER_ID);
                String username = rs.getString(Constants.COLUMN_NAME_USER_NAME);
                boolean userInDiscord = rs.getBoolean(Constants.COLUMN_NAME_USER_IN_DISCORD);

                if (userId != null && username != null) {
                    members.add(new FactionMember(userId, username, userInDiscord));
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
     * Fetch available crimes from Torn API
     */
    private static List<Crime> fetchAvailableCrimes(FactionInfo factionInfo) throws IOException {
        String apiUrl = Constants.API_URL_AVAILABLE_FACTION_CRIMES;

        logger.debug("Fetching available crimes for faction: {}", factionInfo.getFactionId());

        ApiResponse response = TornApiHandler.executeRequest(apiUrl, factionInfo.getApiKey());

        if (response.isSuccess()) {
            CrimesResponse crimesResponse = objectMapper.readValue(response.getBody(), CrimesResponse.class);
            List<Crime> crimes = crimesResponse.getCrimes();

            if (crimes == null) {
                logger.debug("No available crimes found for faction {}", factionInfo.getFactionId());
                return new ArrayList<>();
            }

            logger.debug("Fetched {} available crimes for faction {}", crimes.size(), factionInfo.getFactionId());
            return crimes;

        } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
            logger.error("Circuit breaker is open - cannot fetch available crimes for faction {}",
                    factionInfo.getFactionId());
            throw new IOException("Circuit breaker open - API calls suspended");
        } else {
            logger.error("Failed to fetch available crimes for faction {}: {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("API error: " + response.getErrorMessage());
        }
    }

    /**
     * Build overview records by joining members with crime data
     */
    private static List<OverviewRecord> buildOverviewRecords(List<FactionMember> members, List<Crime> availableCrimes,
                                                             FactionInfo factionInfo) {
        List<OverviewRecord> records = new ArrayList<>();

        // Create a map of user ID to crime slot for users who are in crimes
        Map<String, CrimeSlotData> userCrimeMap = buildUserCrimeMap(availableCrimes, factionInfo);

        for (FactionMember member : members) {
            CrimeSlotData crimeSlotData = userCrimeMap.get(member.getUserId());

            if (crimeSlotData != null) {
                // User is in a crime
                records.add(new OverviewRecord(
                        member.getUserId(),
                        member.getUsername(),
                        member.isUserInDiscord(),
                        true,
                        crimeSlotData.getCrimeId(),
                        crimeSlotData.getCrimeName(),
                        crimeSlotData.getCrimeStatus(),
                        crimeSlotData.getCrimeDifficulty(),
                        crimeSlotData.getRole(),
                        crimeSlotData.getCheckpointPassRate(),
                        crimeSlotData.getItemRequired(),
                        crimeSlotData.getItemIsReusable(),
                        crimeSlotData.getUserHasItem(),
                        crimeSlotData.getCrimeHasAllMembers(),
                        crimeSlotData.getCrimeCompletionDate(),
                        crimeSlotData.getItemAveragePrice()
                ));
            } else {
                // User is not in any crime
                records.add(new OverviewRecord(
                        member.getUserId(),
                        member.getUsername(),
                        member.isUserInDiscord(),
                        false, // not in organised crime
                        null, null, null, null, null, null, null, null, null, null, null, null  // Add extra null for crime_name
                ));
            }
        }

        logger.debug("Built {} overview records for faction {}", records.size(), factionInfo.getFactionId());
        return records;
    }

    /**
     * Build a map of user ID to crime slot data
     */
    private static Map<String, CrimeSlotData> buildUserCrimeMap(List<Crime> crimes, FactionInfo factionInfo) {
        Map<String, CrimeSlotData> userCrimeMap = new HashMap<>();

        for (Crime crime : crimes) {
            if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
                continue;
            }

            // Check if crime has all members (all slots filled)
            boolean crimeHasAllMembers = crime.getSlots().stream()
                    .allMatch(slot -> slot.getUser() != null);

            // Group slots by position and handle duplicates with #1, #2 logic
            Map<String, List<Slot>> groupedSlots = crime.getSlots().stream()
                    .collect(Collectors.groupingBy(Slot::getPosition));

            for (Map.Entry<String, List<Slot>> entry : groupedSlots.entrySet()) {
                List<Slot> group = entry.getValue();
                group.sort(Comparator.comparing(Slot::getPositionId, Comparator.nullsLast(String::compareTo)));

                for (int i = 0; i < group.size(); i++) {
                    Slot slot = group.get(i);
                    SlotUser user = slot.getUser();

                    if (user != null && user.getId() != null) {
                        // Apply role renaming logic
                        String finalRole = slot.getPosition();
                        if (group.size() > 1) {
                            finalRole = slot.getPosition() + " #" + (i + 1);
                        }

                        // Get item information
                        String itemRequired = null;
                        Boolean itemIsReusable = null;
                        Boolean userHasItem = null;
                        Integer itemAveragePrice = null;

                        if (slot.getItemRequirement() != null) {
                            Long itemId = slot.getItemRequirement().getId();
                            itemIsReusable = slot.getItemRequirement().getIsReusable();
                            userHasItem = slot.getItemRequirement().getIsAvailable();

                            // Fetch item details
                            Item itemDetails = fetchItemMarketSafe(itemId, factionInfo.getApiKey());
                            if (itemDetails != null) {
                                itemRequired = itemDetails.getName();
                                itemAveragePrice = itemDetails.getAveragePrice();
                            }
                        }

                        CrimeSlotData crimeSlotData = new CrimeSlotData(
                                crime.getId(),
                                crime.getName(),
                                crime.getStatus(),
                                crime.getDifficulty(),
                                finalRole,
                                slot.getCheckpointPassRate(),
                                itemRequired,
                                itemIsReusable,
                                userHasItem,
                                crimeHasAllMembers,
                                timestampFromEpoch(crime.getReadyAt()),
                                itemAveragePrice
                        );

                        userCrimeMap.put(user.getId().toString(), crimeSlotData);
                    }
                }
            }
        }

        return userCrimeMap;
    }

    /**
     * Helper class to hold crime slot data
     */
    private static class CrimeSlotData {
        private final Long crimeId;
        private final String crimeName;
        private final String crimeStatus;
        private final Integer crimeDifficulty;
        private final String role;
        private final Integer checkpointPassRate;
        private final String itemRequired;
        private final Boolean itemIsReusable;
        private final Boolean userHasItem;
        private final Boolean crimeHasAllMembers;
        private final Timestamp crimeCompletionDate;
        private final Integer itemAveragePrice;

        public CrimeSlotData(Long crimeId, String crimeName, String crimeStatus, Integer crimeDifficulty, String role,
                             Integer checkpointPassRate, String itemRequired, Boolean itemIsReusable,
                             Boolean userHasItem, Boolean crimeHasAllMembers, Timestamp crimeCompletionDate,
                             Integer itemAveragePrice) {
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.crimeStatus = crimeStatus;
            this.crimeDifficulty = crimeDifficulty;
            this.role = role;
            this.checkpointPassRate = checkpointPassRate;
            this.itemRequired = itemRequired;
            this.itemIsReusable = itemIsReusable;
            this.userHasItem = userHasItem;
            this.crimeHasAllMembers = crimeHasAllMembers;
            this.crimeCompletionDate = crimeCompletionDate;
            this.itemAveragePrice = itemAveragePrice;
        }

        // Getters
        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getCrimeStatus() { return crimeStatus; }
        public Integer getCrimeDifficulty() { return crimeDifficulty; }
        public String getRole() { return role; }
        public Integer getCheckpointPassRate() { return checkpointPassRate; }
        public String getItemRequired() { return itemRequired; }
        public Boolean getItemIsReusable() { return itemIsReusable; }
        public Boolean getUserHasItem() { return userHasItem; }
        public Boolean getCrimeHasAllMembers() { return crimeHasAllMembers; }
        public Timestamp getCrimeCompletionDate() { return crimeCompletionDate; }
        public Integer getItemAveragePrice() { return itemAveragePrice; }
    }

    /**
     * Fetch item market data with caching and error handling
     */
    private static Item fetchItemMarketSafe(Long itemId, String apiKey) {
        if (itemId == null) return null;

        // Check cache first
        Item cachedItem = itemCache.get(itemId);
        if (cachedItem != null) {
            return cachedItem;
        }

        try {
            String itemUrl = Constants.API_URL_MARKET + "/" + itemId + Constants.API_URL_ITEM_MARKET;

            ApiResponse response = TornApiHandler.executeRequest(itemUrl, apiKey);

            if (response.isSuccess()) {
                ItemMarketResponse marketResponse = objectMapper.readValue(response.getBody(), ItemMarketResponse.class);
                if (marketResponse != null && marketResponse.getItemMarket() != null) {
                    Item item = marketResponse.getItemMarket();
                    itemCache.put(itemId, item);
                    return item;
                }
            } else {
                logger.debug("Failed to fetch item market data for item {}: {}", itemId, response.getErrorMessage());
            }

        } catch (Exception e) {
            logger.debug("Error fetching market data for item {}: {}", itemId, e.getMessage());
        }

        return null;
    }

    /**
     * Create overview table if it doesn't exist
     */
    private static void createOverviewTableIfNotExists(Connection ocDataConnection, String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "user_id VARCHAR(20) NOT NULL," +
                "username VARCHAR(100) NOT NULL," +
                "user_in_discord BOOLEAN NOT NULL DEFAULT FALSE," +
                "in_organised_crime BOOLEAN NOT NULL DEFAULT FALSE," +
                "crime_id BIGINT," +
                "crime_name VARCHAR(255)," +
                "crime_status VARCHAR(50)," +
                "crime_difficulty INTEGER," +
                "role VARCHAR(100)," +
                "checkpoint_pass_rate INTEGER," +
                "item_required VARCHAR(255)," +
                "item_is_reusable BOOLEAN," +
                "user_has_item BOOLEAN," +
                "crime_has_all_members BOOLEAN," +
                "crime_completion_date TIMESTAMP," +
                "item_average_price INTEGER," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (user_id)" +
                ")";

        try (Statement stmt = ocDataConnection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_in_organised_crime ON " + tableName + "(in_organised_crime)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_crime_id ON " + tableName + "(crime_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_user_has_item ON " + tableName + "(user_has_item)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_last_updated ON " + tableName + "(last_updated)");

            logger.debug("Overview table {} created or verified with indexes", tableName);
        }
    }

    /**
     * Update overview table with new data
     */
    private static void updateOverviewTable(Connection ocDataConnection, String tableName,
                                            List<OverviewRecord> records) throws SQLException {
        if (records.isEmpty()) {
            logger.info("No records to update in overview table {}", tableName);
            return;
        }

        // Clear existing data and insert new data in transaction
        ocDataConnection.setAutoCommit(false);
        try {
            // Clear existing data
            clearExistingOverviewData(ocDataConnection, tableName);

            // Insert new data
            insertOverviewRecords(ocDataConnection, tableName, records);

            ocDataConnection.commit();
            logger.debug("Successfully updated overview table {} with {} records", tableName, records.size());

        } catch (SQLException e) {
            ocDataConnection.rollback();
            logger.error("Failed to update overview table {}, rolling back", tableName, e);
            throw e;
        } finally {
            ocDataConnection.setAutoCommit(true);
        }
    }

    /**
     * Clear existing overview data
     */
    private static void clearExistingOverviewData(Connection ocDataConnection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            int deletedRows = pstmt.executeUpdate();
            logger.debug("Cleared {} existing records from overview table {}", deletedRows, tableName);
        }
    }

    /**
     * Insert overview records into the table
     */
    private static void insertOverviewRecords(Connection ocDataConnection, String tableName,
                                              List<OverviewRecord> records) throws SQLException {

        if (records.isEmpty()) {
            logger.debug("No records to insert into overview table {}", tableName);
            return;
        }

        // Sort records according to specified criteria
        List<OverviewRecord> sortedRecords = records.stream()
                .sorted(Comparator
                        // First: Status priority (Planning, Recruiting, then others/null)
                        .comparing((OverviewRecord r) -> getStatusSortPriority(r.getCrimeStatus()))
                        // Second: Crime completion date ascending (nulls last)
                        .thenComparing(OverviewRecord::getCrimeCompletionDate, Comparator.nullsLast(Comparator.naturalOrder()))
                        // Third: Checkpoint pass rate descending (nulls last)
                        .thenComparing(OverviewRecord::getCheckpointPassRate, Comparator.nullsLast(Comparator.reverseOrder()))
                        // Fourth: Username for consistent ordering
                        .thenComparing(OverviewRecord::getUsername)
                )
                .collect(Collectors.toList());

        String insertSql = "INSERT INTO " + tableName + " (" +
                "user_id, username, user_in_discord, in_organised_crime, crime_id, crime_name, crime_status, " +
                "crime_difficulty, role, checkpoint_pass_rate, item_required, item_is_reusable, " +
                "user_has_item, crime_has_all_members, crime_completion_date, item_average_price, " +
                "last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(insertSql)) {
            for (OverviewRecord record : sortedRecords) {
                pstmt.setString(1, record.getUserId());
                pstmt.setString(2, record.getUsername());
                pstmt.setBoolean(3, record.isUserInDiscord());
                pstmt.setBoolean(4, record.isInOrganisedCrime());

                if (record.getCrimeId() != null) {
                    pstmt.setLong(5, record.getCrimeId());
                } else {
                    pstmt.setNull(5, Types.BIGINT);
                }
                pstmt.setString(6, record.getCrimeName());
                pstmt.setString(7, record.getCrimeStatus());

                if (record.getCrimeDifficulty() != null) {
                    pstmt.setInt(8, record.getCrimeDifficulty());
                } else {
                    pstmt.setNull(8, Types.INTEGER);
                }

                pstmt.setString(9, record.getRole());

                if (record.getCheckpointPassRate() != null) {
                    pstmt.setInt(10, record.getCheckpointPassRate());
                } else {
                    pstmt.setNull(10, Types.INTEGER);
                }

                pstmt.setString(11, record.getItemRequired());

                if (record.getItemIsReusable() != null) {
                    pstmt.setBoolean(12, record.getItemIsReusable());
                } else {
                    pstmt.setNull(12, Types.BOOLEAN);
                }

                if (record.getUserHasItem() != null) {
                    pstmt.setBoolean(13, record.getUserHasItem());
                } else {
                    pstmt.setNull(13, Types.BOOLEAN);
                }

                if (record.getCrimeHasAllMembers() != null) {
                    pstmt.setBoolean(14, record.getCrimeHasAllMembers());
                } else {
                    pstmt.setNull(14, Types.BOOLEAN);
                }

                if (record.getCrimeCompletionDate() != null) {
                    pstmt.setTimestamp(15, record.getCrimeCompletionDate());
                } else {
                    pstmt.setNull(15, Types.TIMESTAMP);
                }

                if (record.getItemAveragePrice() != null) {
                    pstmt.setInt(16, record.getItemAveragePrice());
                } else {
                    pstmt.setNull(16, Types.INTEGER);
                }

                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.debug("Inserted {} sorted overview records into table {}", results.length, tableName);
        }
    }

    /**
     * Helper method to determine sort priority for crime status
     */
    private static int getStatusSortPriority(String status) {
        if (Constants.PLANNING.equalsIgnoreCase(status)) {
            return 1;
        } else if (Constants.RECRUITING.equalsIgnoreCase(status)) {
            return 2;
        } else {
            return 3; // All other statuses (including null) come last
        }
    }
    /**
     * Send Discord notifications for users who joined crimes with items they have
     * PLACEHOLDER - Implementation to be completed later
     */
    private static void sendDiscordNotifications(List<DiscordNotificationData> allNotifications) {
        logger.info("=== DISCORD NOTIFICATION PLACEHOLDER ===");
        logger.info("Total factions with users who joined crimes with items: {}", allNotifications.size());

        for (DiscordNotificationData notification : allNotifications) {
            logger.info("Faction {} ({}): {} users joined crimes with items they already have:",
                    notification.getFactionId(), notification.getFactionSuffix(),
                    notification.getUsersJoinedWithItems().size());

            long totalItemValue = 0;
            for (DiscordNotificationData.UserJoinedWithItemData userData : notification.getUsersJoinedWithItems()) {
                logger.info("  - User: {} | Crime: {} | Role: {} | Item: {} | Value: ${}",
                        userData.getUsername(), userData.getCrimeId(), userData.getRole(),
                        userData.getItemRequired(), userData.getItemAveragePrice() != null ? userData.getItemAveragePrice() : "unknown");

                if (userData.getItemAveragePrice() != null) {
                    totalItemValue += userData.getItemAveragePrice();
                }
            }

            logger.info("  Total item value for faction {}: ${}", notification.getFactionId(), totalItemValue);
        }

        logger.info("=== TODO: Implement actual Discord webhook sending ===");

        // TODO: Implementation will involve:
        // 1. Load Discord webhook configuration for each faction from database
        // 2. Format notification message with user list and total item values
        // 3. Send Discord webhook using SendDiscordMessage class
        // 4. Handle any errors and retry logic

        // Example notification format:
        // "🎯 Crime Update Alert!
        //  The following members joined crimes with items they already have:
        //  • Username1 joined Crime #12345 as Muscle #1 with Heavy Armor ($2,500)
        //  • Username2 joined Crime #12346 as Hacker with Laptop ($1,200)
        //  Total item value ready: $3,700"
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
     * Result wrapper for overview update processing
     */
    private static class OverviewUpdateResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int recordsProcessed;
        private final String errorMessage;
        private final DiscordNotificationData notificationData;

        private OverviewUpdateResult(boolean success, boolean circuitBreakerOpen, int recordsProcessed,
                                     String errorMessage, DiscordNotificationData notificationData) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.recordsProcessed = recordsProcessed;
            this.errorMessage = errorMessage;
            this.notificationData = notificationData;
        }

        public static OverviewUpdateResult success(int recordsProcessed, DiscordNotificationData notificationData) {
            return new OverviewUpdateResult(true, false, recordsProcessed, null, notificationData);
        }

        public static OverviewUpdateResult failure(String errorMessage) {
            return new OverviewUpdateResult(false, false, 0, errorMessage, null);
        }

        public static OverviewUpdateResult circuitBreakerOpen() {
            return new OverviewUpdateResult(false, true, 0, "Circuit breaker is open", null);
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getRecordsProcessed() { return recordsProcessed; }
        public String getErrorMessage() { return errorMessage; }
        public DiscordNotificationData getNotificationData() { return notificationData; }
    }
}