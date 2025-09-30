package com.Torn.ItemManagement;

import com.Torn.Discord.Messages.DiscordMessages;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CheckUsersHaveItems {

    /**
     * ├── CheckUsersHaveItems.java
     * │   ├── Identifies members needing items they don't have
     * │   ├── Sends Discord notifications to faction armourers
     * │   ├── Handles high-value reusable item transfers between users
     * │   └── Logs faction purchase requirements
     */

    private static final Logger logger = LoggerFactory.getLogger(CheckUsersHaveItems.class);

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

    public static class UserItemRequest {
        private final String userId;
        private final String username;
        private final String itemRequired;
        private final Long crimeId;
        private final String crimeName;
        private final String role;
        private final Integer itemAveragePrice;
        private final boolean itemIsReusable;

        public UserItemRequest(String userId, String username, String itemRequired,
                               Long crimeId, String crimeName, String role, Integer itemAveragePrice, boolean itemIsReusable) {
            this.userId = userId;
            this.username = username;
            this.itemRequired = itemRequired;
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.role = role;
            this.itemAveragePrice = itemAveragePrice;
            this.itemIsReusable = itemIsReusable;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getItemRequired() { return itemRequired; }
        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getRole() { return role; }
        public Integer getItemAveragePrice() { return itemAveragePrice; }
        public boolean isItemReusable() { return itemIsReusable; }
    }

    public static class LastItemUser {
        private final String userId;
        private final String username;
        private final String itemName;

        public LastItemUser(String userId, String username, String itemName) {
            this.userId = userId;
            this.username = username;
            this.itemName = itemName;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getItemName() { return itemName; }
    }

    public static class ItemTransferRequest {
        private final String fromUserId;
        private final String fromUsername;
        private final String toUserId;
        private final String toUsername;
        private final String itemName;
        private final String crimeName;
        private final Long crimeId;
        private final long itemValue;

        public ItemTransferRequest(String fromUserId, String fromUsername, String toUserId,
                                   String toUsername, String itemName, String crimeName, long crimeId, long itemValue) {
            this.fromUserId = fromUserId;
            this.fromUsername = fromUsername;
            this.toUserId = toUserId;
            this.toUsername = toUsername;
            this.itemName = itemName;
            this.crimeName = crimeName;
            this.crimeId = crimeId;
            this.itemValue = itemValue;
        }

        public String getFromUserId() { return fromUserId; }
        public String getFromUsername() { return fromUsername; }
        public String getToUserId() { return toUserId; }
        public String getToUsername() { return toUsername; }
        public String getItemName() { return itemName; }
        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public long getItemValue() { return itemValue; }
    }

    /**
     * Main entry point for checking users who need items
     */
    public static void checkUsersHaveItems() throws SQLException {
        logger.info("Starting user item check process for all factions");

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
                logger.warn("No OC2-enabled factions found to check items for");
                return;
            }

            logger.info("Found {} OC2-enabled factions to check items for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalItemRequestsFound = 0;
            int notificationsSent = 0;
            int transferRequestsSent = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Checking item requests for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Check for users who need items
                    CheckItemsResult result = checkItemsForFaction(configConnection, ocDataConnection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("Successfully processed faction {} - found {} item requests, {} transfer requests",
                                factionInfo.getFactionId(), result.getItemRequestsFound(), result.getTransferRequestsFound());
                        successfulCount++;
                        totalItemRequestsFound += result.getItemRequestsFound();

                        if (result.isNotificationSent()) {
                            notificationsSent++;
                        }
                        if (result.isTransferNotificationSent()) {
                            transferRequestsSent++;
                        }
                    } else {
                        logger.error("Failed to check items for faction {}: {}",
                                factionInfo.getFactionId(), result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                } catch (Exception e) {
                    logger.error("Unexpected error checking items for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("User item check completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total item requests found: {}", totalItemRequestsFound);
            logger.info("  Discord notifications sent: {}", notificationsSent);
            logger.info("  Transfer requests sent: {}", transferRequestsSent);

        } catch (SQLException e) {
            logger.error("Database error during user item check", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database (OC2 enabled factions only)
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

                if (factionId != null && isValidDbSuffix(dbSuffix)) {
                    factions.add(new FactionInfo(factionId, dbSuffix));
                } else {
                    logger.warn("Skipping faction with invalid data: factionId={}, dbSuffix={}", factionId, dbSuffix);
                }
            }
        }

        logger.info("Found {} OC2-enabled factions for item checking", factions.size());
        return factions;
    }

    /**
     * Check items for a single faction
     */
    private static CheckItemsResult checkItemsForFaction(Connection configConnection, Connection ocDataConnection, FactionInfo factionInfo) {
        try {
            String overviewTableName = Constants.TABLE_NAME_OVERVIEW + factionInfo.getDbSuffix();

            // Get users who need items they don't have
            List<UserItemRequest> allItemRequests = getUsersNeedingItems(ocDataConnection, overviewTableName);

            if (allItemRequests.isEmpty()) {
                logger.debug("No item requests found for faction {}", factionInfo.getFactionId());
                return CheckItemsResult.success(0, 0, false, false);
            }

// Separate high-value reusable items from regular item requests
            List<UserItemRequest> regularItemRequests = new ArrayList<>();
            List<ItemTransferRequest> transferRequests = new ArrayList<>();

            for (UserItemRequest request : allItemRequests) {
                if (request.isItemReusable() &&
                        request.getItemAveragePrice() != null &&
                        request.getItemAveragePrice() > Constants.ITEM_TRANSFER_THRESHOLD) {

                    // High-value reusable item - check for potential transfer
                    // Pass the current user's ID to exclude them from the search
                    LastItemUser lastUser = getLastUserWithItem(ocDataConnection, factionInfo.getDbSuffix(),
                            request.getItemRequired(), request.getUserId());

                    if (lastUser != null && !isUserCurrentlyUsingItem(ocDataConnection, overviewTableName,
                            lastUser.getUserId(), request.getItemRequired())) {
                        // Create transfer request
                        transferRequests.add(new ItemTransferRequest(
                                lastUser.getUserId(),
                                lastUser.getUsername(),
                                request.getUserId(),
                                request.getUsername(),
                                request.getItemRequired(),
                                request.getCrimeName(),
                                request.getCrimeId(),
                                request.getItemAveragePrice().longValue()
                        ));

                        logger.info("Created transfer request: {} should send {} to {} for crime {}",
                                lastUser.getUsername(), request.getItemRequired(), request.getUsername(), request.getCrimeName());
                    } else {
                        // Previous user still using it OR no previous user found
                        // Don't add to regularItemRequests - just skip it entirely
                        logger.info("Skipping high-value item {} - previous user still using it or no transfer available",
                                request.getItemRequired());
                        // Do NOT add to regularItemRequests
                    }
                } else {
                    // Regular item request (non-reusable or below threshold)
                    regularItemRequests.add(request);
                }
            }

            // Log faction purchase requirements for regular items only
            // High-value reusable items are logged separately in logItemTransferRequest()
            for (UserItemRequest request : regularItemRequests) {
                try {
                    // Check if we should suppress tracking during setup
                    String suppressNotifications = System.getenv(Constants.SUPPRESS_PROCESSING);
                    if ("true".equalsIgnoreCase(suppressNotifications)) {
                        logger.debug("Item tracking suppressed during setup - skipping logging for item {}",
                                request.getItemRequired());
                        continue; // Skip logging during setup
                    }

                    FactionItemTracking.logFactionPurchaseRequired(
                            ocDataConnection,
                            factionInfo.getDbSuffix(),
                            request.getUserId(),
                            request.getUsername(),
                            request.getCrimeId(),
                            request.getCrimeName(),
                            request.getItemRequired(),
                            request.getItemAveragePrice() != null ? request.getItemAveragePrice().longValue() : null
                    );
                } catch (SQLException e) {
                    logger.warn("Failed to log faction purchase requirement for item {}: {}",
                            request.getItemRequired(), e.getMessage());
                }
            }

            logger.info("Found {} regular item requests and {} transfer requests for faction {}",
                    regularItemRequests.size(), transferRequests.size(), factionInfo.getFactionId());

            // Send notifications
            boolean regularNotificationSent = false;
            boolean transferNotificationSent = false;

            // Send regular item requests to armourer
            if (!regularItemRequests.isEmpty()) {
                List<DiscordMessages.ItemRequest> discordRequests = convertToDiscordRequests(configConnection, regularItemRequests);
                regularNotificationSent = sendDiscordNotification(factionInfo.getFactionId(), discordRequests);
            }

            // Send transfer requests
            if (!transferRequests.isEmpty()) {
                transferNotificationSent = sendTransferNotifications(factionInfo.getFactionId(), transferRequests);
            }

            return CheckItemsResult.success(regularItemRequests.size(), transferRequests.size(), regularNotificationSent, transferNotificationSent);

        } catch (Exception e) {
            logger.error("Exception checking items for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return CheckItemsResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Get users who need items they don't have (including reusable items)
     */
    private static List<UserItemRequest> getUsersNeedingItems(Connection ocDataConnection, String tableName) {
        List<UserItemRequest> itemRequests = new ArrayList<>();

        String sql = "SELECT " +
                Constants.COLUMN_NAME_USER_ID + ", " +
                Constants.COLUMN_NAME_USER_NAME + ", " +
                Constants.COLUMN_NAME_ITEM_REQUIRED + ", " +
                Constants.COLUMN_NAME_CRIME_ID + ", " +
                Constants.COLUMN_NAME_CRIME_NAME + ", " +
                Constants.COLUMN_NAME_ROLE + ", " +
                Constants.COLUMN_NAME_ITEM_AVERAGE_PRICE + ", " +
                Constants.COLUMN_NAME_ITEM_IS_REUSABLE + " " +
                "FROM " + tableName + " " +
                "WHERE " + Constants.COLUMN_NAME_ITEM_REQUIRED + " IS NOT NULL " +
                "AND " + Constants.COLUMN_NAME_USER_HAS_ITEM + " = false " +
                "AND " + Constants.COLUMN_NAME_IN_ORGANISED_CRIME + " = true " +
                "ORDER BY " + Constants.COLUMN_NAME_CRIME_ID + ", " + Constants.COLUMN_NAME_USER_NAME;

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String userId = rs.getString(Constants.COLUMN_NAME_USER_ID);
                String username = rs.getString(Constants.COLUMN_NAME_USER_NAME);
                String itemRequired = rs.getString(Constants.COLUMN_NAME_ITEM_REQUIRED);
                Long crimeId = rs.getObject(Constants.COLUMN_NAME_CRIME_ID, Long.class);
                String crimeName = rs.getString(Constants.COLUMN_NAME_CRIME_NAME);
                String role = rs.getString(Constants.COLUMN_NAME_ROLE);
                Integer itemAveragePrice = rs.getObject(Constants.COLUMN_NAME_ITEM_AVERAGE_PRICE, Integer.class);
                boolean itemIsReusable = rs.getBoolean(Constants.COLUMN_NAME_ITEM_IS_REUSABLE);

                if (userId != null && username != null && itemRequired != null) {
                    itemRequests.add(new UserItemRequest(
                            userId, username, itemRequired, crimeId, crimeName, role, itemAveragePrice, itemIsReusable
                    ));
                }
            }

        } catch (SQLException e) {
            logger.debug("Could not query table {} (might not exist): {}", tableName, e.getMessage());
            // Return empty list if table doesn't exist
            return new ArrayList<>();
        }

        return itemRequests;
    }

    /**
     * Get the last user who needed a specific item from the tracking table
     */
    private static LastItemUser getLastUserWithItem(Connection ocDataConnection, String dbSuffix,
                                                    String itemName, String excludeUserId) {
        String trackingTableName = "item_tracking_" + dbSuffix;

        String sql = "SELECT user_id, username, item_name " +
                "FROM " + trackingTableName + " " +
                "WHERE item_name = ? " +
                "AND faction_purchased = true " +
                "AND user_id != ? " +  // Exclude the current requester
                "ORDER BY log_date DESC " +
                "LIMIT 1";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            pstmt.setString(1, itemName);
            pstmt.setString(2, excludeUserId);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String userId = rs.getString("user_id");
                    String username = rs.getString("username");
                    String returnedItemName = rs.getString("item_name");

                    logger.debug("Found last user {} ({}) who had item {} (excluding {})",
                            username, userId, itemName, excludeUserId);
                    return new LastItemUser(userId, username, returnedItemName);
                }
            }

        } catch (SQLException e) {
            logger.debug("Could not query tracking table {} for item {}: {}",
                    trackingTableName, itemName, e.getMessage());
        }

        logger.debug("No previous user found for item {} in tracking table {}", itemName, trackingTableName);
        return null;
    }

    /**
     * Check if a user is currently in a crime that requires the specified item
     */
    private static boolean isUserCurrentlyUsingItem(Connection ocDataConnection, String overviewTableName, String userId, String itemName) {
        String sql = "SELECT COUNT(*) as count " +
                "FROM " + overviewTableName + " " +
                "WHERE " + Constants.COLUMN_NAME_USER_ID + " = ? " +
                "AND " + Constants.COLUMN_NAME_ITEM_REQUIRED + " = ? " +
                "AND " + Constants.COLUMN_NAME_IN_ORGANISED_CRIME + " = true";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, itemName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt("count");
                    boolean isUsing = count > 0;
                    logger.debug("User {} is {} using item {}", userId, isUsing ? "currently" : "not currently", itemName);
                    return isUsing;
                }
            }

        } catch (SQLException e) {
            logger.warn("Could not check if user {} is using item {}: {}", userId, itemName, e.getMessage());
            // Default to true (assume user is using it) to be safe
            return true;
        }

        return false;
    }

    /**
     * Send transfer notifications for high-value reusable items
     */
    private static boolean sendTransferNotifications(String factionId, List<ItemTransferRequest> transferRequests) {
        if (transferRequests.isEmpty()) {
            return true;
        }

        logger.info("=== STARTING TRANSFER NOTIFICATIONS ===");
        logger.info("Processing {} transfer requests for faction {}", transferRequests.size(), factionId);

        try {
            // Get database connection for Discord mappings and item tracking
            String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
            String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

            if (configDatabaseUrl == null || ocDataDatabaseUrl == null) {
                logger.error("Database URLs not configured for transfer notifications");
                return false;
            }

            try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
                 Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

                // Get faction suffix for this factionId
                String factionSuffix = getFactionSuffix(configConnection, factionId);
                if (factionSuffix == null) {
                    logger.error("Could not find faction suffix for faction {}", factionId);
                    return false;
                }

                logger.info("Loaded faction suffix: {}", factionSuffix);

                // Get Discord mappings for both sender and receiver
                Map<String, String> discordMentions = getDiscordMentions(configConnection, factionSuffix, transferRequests);
                logger.info("Loaded {} Discord mentions for transfer notifications", discordMentions.size());

                // Group transfer requests by item to avoid spam
                Map<String, List<ItemTransferRequest>> requestsByItem = new HashMap<>();
                for (ItemTransferRequest request : transferRequests) {
                    requestsByItem.computeIfAbsent(request.getItemName(), k -> new ArrayList<>()).add(request);
                }

                logger.info("Grouped into {} unique item types", requestsByItem.size());

                boolean allSuccessful = true;

                for (Map.Entry<String, List<ItemTransferRequest>> entry : requestsByItem.entrySet()) {
                    String itemName = entry.getKey();
                    List<ItemTransferRequest> requests = entry.getValue();

                    logger.info("Processing item: {} with {} transfer requests", itemName, requests.size());

                    // Update item tracking for each transfer request
                    for (ItemTransferRequest request : requests) {
                        try {
                            logger.info("Logging transfer: {} -> {} for item {}",
                                    request.getFromUsername(), request.getToUsername(), itemName);

                            FactionItemTracking.logItemTransferRequest(
                                    ocDataConnection,
                                    factionSuffix,
                                    request.getFromUserId(),
                                    request.getFromUsername(),
                                    request.getToUserId(),
                                    request.getToUsername(),
                                    request.getCrimeId(),
                                    request.getCrimeName(),
                                    request.getItemName(),
                                    request.getItemValue()
                            );

                            logger.info("Successfully logged transfer request for {}", itemName);
                        } catch (Exception e) {
                            logger.warn("Failed to log transfer request for item {}: {}",
                                    itemName, e.getMessage());
                        }
                    }

                    logger.info("Sending Discord transfer notification for item: {}", itemName);
                    boolean success = DiscordMessages.sendItemTransferRequests(factionId, itemName, requests, discordMentions);

                    if (success) {
                        logger.info("✓ Successfully sent Discord transfer notification for item {}", itemName);
                    } else {
                        allSuccessful = false;
                        logger.error("✗ Failed to send transfer notification for item {} in faction {}", itemName, factionId);
                    }
                }

                logger.info("=== TRANSFER NOTIFICATIONS COMPLETE: {} ===", allSuccessful ? "SUCCESS" : "FAILURE");
                return allSuccessful;
            }

        } catch (Exception e) {
            logger.error("Exception sending transfer notifications for faction {}: {}", factionId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Get faction suffix from faction ID
     */
    private static String getFactionSuffix(Connection configConnection, String factionId) {
        String sql = "SELECT " + Constants.COLUMN_NAME_DB_SUFFIX + " FROM " + Constants.TABLE_NAME_FACTIONS +
                " WHERE " + Constants.COLUMN_NAME_FACTION_ID + " = ?";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get faction suffix for faction {}: {}", factionId, e.getMessage());
        }
        return null;
    }

    /**
     * Get Discord mentions for users involved in transfer requests
     */
    private static Map<String, String> getDiscordMentions(Connection configConnection, String factionSuffix,
                                                          List<ItemTransferRequest> transferRequests) {
        Map<String, String> mentions = new HashMap<>();

        // Collect all user IDs
        Set<String> userIds = new HashSet<>();
        for (ItemTransferRequest request : transferRequests) {
            userIds.add(request.getFromUserId());
            userIds.add(request.getToUserId());
        }

        if (userIds.isEmpty()) {
            return mentions;
        }

        String membersTable = "members_" + factionSuffix;
        StringBuilder sql = new StringBuilder("SELECT user_id, user_discord_mention_id FROM ")
                .append(membersTable)
                .append(" WHERE user_id IN (");

        for (int i = 0; i < userIds.size(); i++) {
            if (i > 0) sql.append(",");
            sql.append("?");
        }
        sql.append(") AND user_discord_mention_id IS NOT NULL");

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql.toString())) {
            int index = 1;
            for (String userId : userIds) {
                pstmt.setString(index++, userId);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String userId = rs.getString("user_id");
                    String mention = rs.getString("user_discord_mention_id");
                    if (userId != null && mention != null) {
                        mentions.put(userId, mention);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("Failed to get Discord mentions from table {}: {}", membersTable, e.getMessage());
        }

        logger.debug("Loaded {} Discord mentions for transfer notifications", mentions.size());
        return mentions;
    }

    private static String getItemIdFromDatabase(Connection configConnection, String itemName) {
        String sql = "SELECT item_id FROM " + Constants.TABLE_NAME_OC2_ITEMS +
                " WHERE item_name = ? AND item_id IS NOT NULL LIMIT 1";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql)) {
            pstmt.setString(1, itemName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    long itemId = rs.getLong("item_id");
                    return String.valueOf(itemId);
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not lookup item ID for '{}': {}", itemName, e.getMessage());
        }

        return "0"; // Fallback to "0" if not found
    }

    /**
     * Convert internal item requests to Discord message format
     */
    private static List<DiscordMessages.ItemRequest> convertToDiscordRequests(Connection configConnection, List<UserItemRequest> itemRequests) {
        List<DiscordMessages.ItemRequest> discordRequests = new ArrayList<>();

        for (UserItemRequest request : itemRequests) {
            // Generate a request ID for tracking (using crime ID + user ID)
            String requestId = String.format("%d-%s",
                    request.getCrimeId() != null ? request.getCrimeId() : 0,
                    request.getUserId());

            // Look up the actual item ID from database
            String itemId = getItemIdFromDatabase(configConnection, request.getItemRequired());

            DiscordMessages.ItemRequest discordRequest = new DiscordMessages.ItemRequest(
                    request.getUserId(),
                    request.getUsername(),
                    itemId,
                    request.getItemRequired(),
                    requestId,
                    request.getItemAveragePrice() != null ? request.getItemAveragePrice().longValue() : null
            );

            discordRequests.add(discordRequest);
        }

        return discordRequests;
    }

    /**
     * Send Discord notification to faction armourer
     */
    private static boolean sendDiscordNotification(String factionId, List<DiscordMessages.ItemRequest> itemRequests) {
        try {
            if (itemRequests.isEmpty()) {
                logger.debug("No item requests to send for faction {}", factionId);
                return true; // Not really a failure
            }

            // Use the existing Discord message method
            boolean success = DiscordMessages.sendArmourerGetItems(factionId, itemRequests);

            if (success) {
                logger.info("Successfully sent Discord notification to faction {} with {} item requests",
                        factionId, itemRequests.size());
            } else {
                logger.error("Failed to send Discord notification to faction {}", factionId);
            }

            return success;

        } catch (Exception e) {
            logger.error("Exception sending Discord notification to faction {}: {}",
                    factionId, e.getMessage(), e);
            return false;
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
     * Result wrapper for item checking
     */
    private static class CheckItemsResult {
        private final boolean success;
        private final int itemRequestsFound;
        private final int transferRequestsFound;
        private final boolean notificationSent;
        private final boolean transferNotificationSent;
        private final String errorMessage;

        private CheckItemsResult(boolean success, int itemRequestsFound, int transferRequestsFound,
                                 boolean notificationSent, boolean transferNotificationSent, String errorMessage) {
            this.success = success;
            this.itemRequestsFound = itemRequestsFound;
            this.transferRequestsFound = transferRequestsFound;
            this.notificationSent = notificationSent;
            this.transferNotificationSent = transferNotificationSent;
            this.errorMessage = errorMessage;
        }

        public static CheckItemsResult success(int itemRequestsFound, int transferRequestsFound,
                                               boolean notificationSent, boolean transferNotificationSent) {
            return new CheckItemsResult(true, itemRequestsFound, transferRequestsFound,
                    notificationSent, transferNotificationSent, null);
        }

        public static CheckItemsResult failure(String errorMessage) {
            return new CheckItemsResult(false, 0, 0, false, false, errorMessage);
        }

        public boolean isSuccess() { return success; }
        public int getItemRequestsFound() { return itemRequestsFound; }
        public int getTransferRequestsFound() { return transferRequestsFound; }
        public boolean isNotificationSent() { return notificationSent; }
        public boolean isTransferNotificationSent() { return transferNotificationSent; }
        public String getErrorMessage() { return errorMessage; }
    }
}