package com.Torn.PaymentRequests;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Discord.Messages.DiscordMessages;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Service to verify payments by checking faction news and managing request expiry
 */
public class PaymentVerificationService {

    /**
     * └── PaymentVerificationService.java
     *     ├── Monitors faction news for completed payments
     *     ├── Auto-fulfills payment requests when payments detected
     *     ├── Resets expired claims back to pending
     *     └── Sends fulfillment notifications
     */

    private static final Logger logger = LoggerFactory.getLogger(PaymentVerificationService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TORN_API_RATE_LIMIT_MS = 2000;

    // Pattern to extract deposit information from faction news
    // Example: "PlayerName increased PlayerName's money balance by $16,100,000 from $266,198,012 to $282,298,012"
    private static final Pattern DEPOSIT_PATTERN = Pattern.compile(
            "<a href.*?XID=(\\d+)\">([^<]+)</a> increased <a href.*?XID=(\\d+)\">([^<]+)</a>'s money balance by \\$([\\d,]+)(?:\\s+from.*?)?"
    );

    public static class FactionInfo {
        private final String factionId;
        private final String apiKey;
        private final String ownerName;

        public FactionInfo(String factionId, String apiKey, String ownerName) {
            this.factionId = factionId;
            this.apiKey = apiKey;
            this.ownerName = ownerName;
        }

        public String getFactionId() { return factionId; }
        public String getApiKey() { return apiKey; }
        public String getOwnerName() { return ownerName; }
    }

    /**
     * Main entry point for payment verification and expiry management
     */
    public static void verifyPaymentsAndExpireRequests() throws SQLException {
        logger.info("Starting payment verification and expiry check");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger)) {
            // Create table if it doesn't exist
            PaymentRequestDAO.createTableIfNotExists(configConnection);

            // 1. Handle expired claimed requests first
            int expiredCount = handleExpiredRequests(configConnection);

            // 2. Check for fulfilled payments
            int verifiedCount = checkForFulfilledPayments(configConnection);

            logger.info("Payment verification completed: {} expired requests reset, {} payments verified",
                    expiredCount, verifiedCount);

        } catch (SQLException e) {
            logger.error("Database error during payment verification", e);
            throw e;
        }
    }

    /**
     * Handle expired claimed requests (reset to PENDING and resend notifications)
     */
    private static int handleExpiredRequests(Connection connection) throws SQLException {
        List<PaymentRequest> expiredRequests = PaymentRequestDAO.getExpiredClaimedRequests(connection);

        if (expiredRequests.isEmpty()) {
            logger.debug("No expired claimed requests found");
            return 0;
        }

        logger.info("Found {} expired claimed requests", expiredRequests.size());

        // Reset expired requests back to PENDING
        List<String> expiredRequestIds = expiredRequests.stream()
                .map(PaymentRequest::getRequestId)
                .collect(Collectors.toList());

        int resetCount = PaymentRequestDAO.resetExpiredRequests(connection, expiredRequestIds);
        logger.info("Reset {} expired requests back to PENDING", resetCount);

        // Resend Discord notifications for expired requests
        resendNotificationsForExpiredRequests(expiredRequests);

        return resetCount;
    }

    /**
     * Resend Discord notifications for expired requests
     */
    private static void resendNotificationsForExpiredRequests(List<PaymentRequest> expiredRequests) {
        logger.info("Resending Discord notifications for {} expired requests", expiredRequests.size());

        int resent = 0;
        int failed = 0;

        for (PaymentRequest request : expiredRequests) {
            try {
                boolean success = com.Torn.Discord.Messages.DiscordMessages.sendPayMemberForItem(
                        request.getFactionId(),
                        request.getUsername(),
                        request.getUserId(),
                        request.getRequestId(),
                        request.getItemRequired(),
                        request.getItemValue()
                );

                if (success) {
                    resent++;
                    logger.info("Resent notification for expired request: {} (user: {}, value: ${})",
                            request.getRequestId(),
                            request.getUsername(), request.getItemValue());
                } else {
                    failed++;
                    logger.error("Failed to resend notification for request: {}",
                            request.getRequestId());
                }

            } catch (Exception e) {
                failed++;
                logger.error("Error resending notification for request {}: {}",
                        request.getRequestId(), e.getMessage(), e);
            }
        }

        logger.info("Resent notifications: {} successful, {} failed", resent, failed);
    }

    /**
     * Check faction news for payments and mark matching requests as fulfilled
     */
    private static int checkForFulfilledPayments(Connection connection) throws SQLException {
        List<FactionInfo> factions = getActiveFactions(connection);

        if (factions.isEmpty()) {
            logger.debug("No active factions found for payment verification");
            return 0;
        }

        logger.info("Checking payments for {} factions", factions.size());

        int totalPaymentsVerified = 0;

        for (FactionInfo faction : factions) {
            try {
                int paymentsVerified = checkFactionNewsForPayments(connection, faction);
                totalPaymentsVerified += paymentsVerified;

                if (paymentsVerified > 0) {
                    logger.info("Verified {} payments for faction {} ({})",
                            paymentsVerified, faction.getFactionId(), faction.getOwnerName());
                }

                // Rate limiting between API calls
                Thread.sleep(TORN_API_RATE_LIMIT_MS);

            } catch (InterruptedException e) {
                logger.warn("Payment verification interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error checking payments for faction {} ({}): {}",
                        faction.getFactionId(), faction.getOwnerName(), e.getMessage(), e);
            }
        }

        logger.info("Payment verification completed - verified {} payments across all factions",
                totalPaymentsVerified);
        return totalPaymentsVerified;
    }

    /**
     * Get active factions with API keys for payment verification
     */
    private static List<FactionInfo> getActiveFactions(Connection connection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT DISTINCT ON (f." + Constants.COLUMN_NAME_FACTION_ID + ") " +
                "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                "ak." + Constants.COLUMN_NAME_API_KEY + ", " +
                "ak." + Constants.COLUMN_NAME_OWNER_NAME + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                "AND f.oc2_enabled = true " +
                "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);
                String ownerName = rs.getString(Constants.COLUMN_NAME_OWNER_NAME);

                if (factionId != null && apiKey != null) {
                    factions.add(new FactionInfo(factionId, apiKey, ownerName));
                }
            }
        }

        logger.debug("Found {} active factions for payment verification", factions.size());
        return factions;
    }

    /**
     * Check faction news for payments matching claimed requests
     */
    private static int checkFactionNewsForPayments(Connection connection, FactionInfo faction) {
        try {
            // Get claimed requests for this faction
            List<PaymentRequest> claimedRequests = PaymentRequestDAO.getClaimedRequestsForFaction(
                    connection, faction.getFactionId());

            // Also get pending requests to catch payments made without claiming
            List<PaymentRequest> pendingRequests = PaymentRequestDAO.getPendingRequestsForFaction(
                    connection, faction.getFactionId());

            // Combine both lists
            List<PaymentRequest> allRequests = new ArrayList<>();
            allRequests.addAll(claimedRequests);
            allRequests.addAll(pendingRequests);

            if (allRequests.isEmpty()) {
                logger.debug("No requests to verify for faction {}", faction.getFactionId());
                return 0;
            }

            logger.debug("Checking {} requests ({} claimed, {} pending) for faction {} ({})",
                    allRequests.size(), claimedRequests.size(), pendingRequests.size(),
                    faction.getFactionId(), faction.getOwnerName());

            // Fetch faction news for depositFunds category
            String apiUrl = "https://api.torn.com/v2/faction/news?striptags=false&limit=100&sort=DESC&cat=depositFunds";
            ApiResponse response = TornApiHandler.executeRequest(apiUrl, faction.getApiKey());

            if (!response.isSuccess()) {
                logger.warn("Failed to fetch faction news for faction {} ({}): {}",
                        faction.getFactionId(), faction.getOwnerName(), response.getErrorMessage());
                return 0;
            }

            JsonNode newsData = objectMapper.readTree(response.getBody());
            JsonNode newsArray = newsData.get("news");

            if (newsArray == null || !newsArray.isArray()) {
                logger.debug("No news data found for faction {} ({})",
                        faction.getFactionId(), faction.getOwnerName());
                return 0;
            }

            int paymentsVerified = 0;

            // Check each news item against claimed requests
            for (JsonNode newsItem : newsArray) {
                String newsId = newsItem.get("id").asText();
                String newsText = newsItem.get("text").asText();
                long timestamp = newsItem.get("timestamp").asLong();

                // Parse deposit information from news text
                DepositInfo depositInfo = parseDepositFromNews(newsText);

                if (depositInfo != null) {
                    // Check if this deposit matches any claimed request
                    for (PaymentRequest request : allRequests) {
                        if (doesDepositMatchRequest(depositInfo, request, timestamp)) {
                            // Mark request as fulfilled
                            boolean success = PaymentRequestDAO.fulfillPaymentRequest(
                                    connection, request.getRequestId(), "AUTO_VERIFIED", newsId);

                            if (success) {
                                paymentsVerified++;
                                logger.info("Verified deposit for request {} - ${} by {} (news: {})",
                                        request.getRequestId(),
                                        depositInfo.getAmount(),
                                        depositInfo.getDepositorName(),
                                        newsId.substring(0, 8) + "...");

                                boolean notificationSent = DiscordMessages.paymentFulfilled(
                                        request.getFactionId(),
                                        request.getUsername(),
                                        request.getUserId(),
                                        request.getRequestId(),
                                        request.getItemRequired(),
                                        request.getItemValue(),
                                        depositInfo.getDepositorName()
                                );

                                if (notificationSent) {
                                    logger.info("Sent payment fulfilled notification for request {}",
                                            request.getRequestId());
                                } else {
                                    logger.warn("✗ Failed to send payment fulfilled notification for request {}",
                                            request.getRequestId());
                                }
                            }
                            break; // Move to next news item
                        }
                    }
                }
            }

            return paymentsVerified;

        } catch (Exception e) {
            logger.error("Error checking faction news for payments (faction {} - {}): {}",
                    faction.getFactionId(), faction.getOwnerName(), e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Parse deposit information from faction news text
     */
    private static DepositInfo parseDepositFromNews(String newsText) {
        try {
            Matcher matcher = DEPOSIT_PATTERN.matcher(newsText);

            if (matcher.find()) {
                String depositorId = matcher.group(1);        // First XID
                String depositorName = matcher.group(2);      // First name
                String recipientId = matcher.group(3);        // Second XID
                String recipientName = matcher.group(4);      // Second name
                String amountStr = matcher.group(5).replace(",", ""); // Amount

                long amount = Long.parseLong(amountStr);

                return new DepositInfo(recipientId, recipientName, amount, depositorId, depositorName);
            }

        } catch (Exception e) {
            logger.debug("Error parsing deposit from news text: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Check if a deposit matches a request based on depositor and amount
     */
    private static boolean doesDepositMatchRequest(DepositInfo deposit, PaymentRequest request, long newsTimestamp) {
        // Check if recipient matches the user who made the request
        if (!deposit.getRecipientId().equals(request.getUserId())) {
            return false;
        }

        // Check if amount matches (allow for 5% variance for rounding)
        long expectedAmount = request.getItemValue();
        long actualAmount = deposit.getAmount();

        if (expectedAmount == 0) {
            return false; // Skip zero-value requests
        }

        double variance = Math.abs(actualAmount - expectedAmount) / (double) expectedAmount;
        if (variance > 0.05) { // Allow 5% variance
            logger.debug("Amount mismatch for request {} - expected: ${}, actual: ${}, variance: {:.2%}",
                    request.getRequestId(), expectedAmount, actualAmount, variance);
            return false;
        }

        // Check if deposit occurred after the request was claimed
        if (request.getClaimedAt() != null) {
            long claimedTimestamp = request.getClaimedAt().getTime() / 1000; // Convert to seconds
            if (newsTimestamp < claimedTimestamp) {
                logger.debug("Deposit timestamp {} before claim timestamp {} for request {}",
                        newsTimestamp, claimedTimestamp, request.getRequestId());
                return false;
            }
        }

        logger.debug("Deposit match found: ${} by {} for request {}",
                actualAmount, deposit.getDepositorName(), request.getRequestId());
        return true;
    }

    /**
     * Helper class to hold deposit information parsed from news
     */
    private static class DepositInfo {
        private final String depositorId;
        private final String depositorName;
        private final String recipientId;
        private final String recipientName;
        private final long amount;

        public DepositInfo(String recipientId, String recipientName, long amount, String depositorId, String depositorName) {

            this.recipientId = recipientId;
            this.recipientName = recipientName;
            this.amount = amount;
            this.depositorId = depositorId;
            this.depositorName = depositorName;

        }

        public String getRecipientId() { return recipientId; }
        public String getRecipientName() { return recipientName; }
        public String getDepositorId() { return depositorId; }
        public String getDepositorName() { return depositorName; }
        public long getAmount() { return amount; }

        @Override
        public String toString() {
            return String.format("DepositInfo{%s deposited $%,d}",
                    depositorName, amount);
        }
    }
}