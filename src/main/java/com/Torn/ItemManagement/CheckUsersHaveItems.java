package com.Torn.ItemManagement;

import com.Torn.Discord.Messages.DiscordMessages;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.Torn.ItemManagement.FactionItemTracking;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CheckUsersHaveItems {

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

        public UserItemRequest(String userId, String username, String itemRequired,
                               Long crimeId, String crimeName, String role, Integer itemAveragePrice) {
            this.userId = userId;
            this.username = username;
            this.itemRequired = itemRequired;
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.role = role;
            this.itemAveragePrice = itemAveragePrice;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getItemRequired() { return itemRequired; }
        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getRole() { return role; }
        public Integer getItemAveragePrice() { return itemAveragePrice; }
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

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Checking item requests for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Check for users who need items
                    CheckItemsResult result = checkItemsForFaction(ocDataConnection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("Successfully processed faction {} - found {} item requests",
                                factionInfo.getFactionId(), result.getItemRequestsFound());
                        successfulCount++;
                        totalItemRequestsFound += result.getItemRequestsFound();

                        if (result.isNotificationSent()) {
                            notificationsSent++;
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

                if (factionId != null && dbSuffix != null && isValidDbSuffix(dbSuffix)) {
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
    private static CheckItemsResult checkItemsForFaction(Connection ocDataConnection, FactionInfo factionInfo) {
        try {
            String overviewTableName = Constants.TABLE_NAME_OVERVIEW + factionInfo.getDbSuffix();

            // Get users who need non-reusable items they don't have
            List<UserItemRequest> itemRequests = getUsersNeedingItems(ocDataConnection, overviewTableName);

            if (itemRequests.isEmpty()) {
                logger.debug("No item requests found for faction {}", factionInfo.getFactionId());
                return CheckItemsResult.success(0, false);
            }

            // Log faction purchase requirements for items users don't have
            for (UserItemRequest request : itemRequests) {
                try {
                    FactionItemTracking.logFactionPurchaseRequired(
                            ocDataConnection,
                            factionInfo.getDbSuffix(),
                            request.getCrimeName(),
                            request.getItemRequired(),
                            request.getItemAveragePrice() != null ? request.getItemAveragePrice().longValue() : null
                    );
                } catch (SQLException e) {
                    logger.warn("Failed to log faction purchase requirement for item {}: {}",
                            request.getItemRequired(), e.getMessage());
                }
            }

            logger.info("Found {} users needing items for faction {}", itemRequests.size(), factionInfo.getFactionId());

            // Log item requests for debugging
            for (UserItemRequest request : itemRequests) {
                logger.debug("Item request: {} needs {} for crime {} (role: {}, value: ${})",
                        request.getUsername(), request.getItemRequired(), request.getCrimeName(),
                        request.getRole(), request.getItemAveragePrice());
            }

            // Convert to Discord message format
            List<DiscordMessages.ItemRequest> discordRequests = convertToDiscordRequests(itemRequests);

            // Send Discord notification
            boolean notificationSent = sendDiscordNotification(factionInfo.getFactionId(), discordRequests);

            if (notificationSent) {
                logger.info("Discord notification sent for faction {} with {} item requests",
                        factionInfo.getFactionId(), itemRequests.size());
            } else {
                logger.warn("✗ Failed to send Discord notification for faction {}", factionInfo.getFactionId());
            }

            return CheckItemsResult.success(itemRequests.size(), notificationSent);

        } catch (Exception e) {
            logger.error("Exception checking items for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return CheckItemsResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Get users who need non-reusable items they don't have
     */
    private static List<UserItemRequest> getUsersNeedingItems(Connection ocDataConnection, String tableName) throws SQLException {
        List<UserItemRequest> itemRequests = new ArrayList<>();

        String sql = "SELECT " +
                Constants.COLUMN_NAME_USER_ID + ", " +
                Constants.COLUMN_NAME_USER_NAME + ", " +
                Constants.COLUMN_NAME_ITEM_REQUIRED + ", " +
                Constants.COLUMN_NAME_CRIME_ID + ", " +
                Constants.COLUMN_NAME_CRIME_NAME + ", " +
                Constants.COLUMN_NAME_ROLE + ", " +
                Constants.COLUMN_NAME_ITEM_AVERAGE_PRICE + " " +
                "FROM " + tableName + " " +
                "WHERE " + Constants.COLUMN_NAME_ITEM_REQUIRED + " IS NOT NULL " +
                "AND " + Constants.COLUMN_NAME_ITEM_IS_REUSABLE + " = false " +
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

                if (userId != null && username != null && itemRequired != null) {
                    itemRequests.add(new UserItemRequest(
                            userId, username, itemRequired, crimeId, crimeName, role, itemAveragePrice
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
     * Convert internal item requests to Discord message format
     */
    private static List<DiscordMessages.ItemRequest> convertToDiscordRequests(List<UserItemRequest> itemRequests) {
        List<DiscordMessages.ItemRequest> discordRequests = new ArrayList<>();

        for (UserItemRequest request : itemRequests) {
            // Generate a request ID for tracking (using crime ID + user ID)
            String requestId = String.format("%d-%s",
                    request.getCrimeId() != null ? request.getCrimeId() : 0,
                    request.getUserId());

            // Note: The amount parameter in ItemRequest constructor isn't used in the Discord message,
            // so we'll pass 1 as a placeholder. The actual item name and details come from other parameters.
            DiscordMessages.ItemRequest discordRequest = new DiscordMessages.ItemRequest(
                    request.getUserId(),
                    request.getUsername(),
                    "0", // itemId - not needed for the Discord message
                    request.getItemRequired(),
                    1, // amount - placeholder, not used in Discord message
                    requestId
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
        private final boolean notificationSent;
        private final String errorMessage;

        private CheckItemsResult(boolean success, int itemRequestsFound, boolean notificationSent, String errorMessage) {
            this.success = success;
            this.itemRequestsFound = itemRequestsFound;
            this.notificationSent = notificationSent;
            this.errorMessage = errorMessage;
        }

        public static CheckItemsResult success(int itemRequestsFound, boolean notificationSent) {
            return new CheckItemsResult(true, itemRequestsFound, notificationSent, null);
        }

        public static CheckItemsResult failure(String errorMessage) {
            return new CheckItemsResult(false, 0, false, errorMessage);
        }

        public boolean isSuccess() { return success; }
        public int getItemRequestsFound() { return itemRequestsFound; }
        public boolean isNotificationSent() { return notificationSent; }
        public String getErrorMessage() { return errorMessage; }
    }
}