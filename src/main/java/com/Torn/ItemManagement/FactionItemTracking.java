package com.Torn.ItemManagement;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * Utility class for tracking faction item purchases and member payments
 */
public class FactionItemTracking {

    private static final Logger logger = LoggerFactory.getLogger(FactionItemTracking.class);

    public enum ItemActionType {
        FACTION_PURCHASE,    // Faction needs to buy the item
        MEMBER_PAYMENT       // Faction needs to pay member for item they have
    }

    public enum PaymentStatus {
        PENDING,    // Payment request created but not fulfilled
        FULFILLED   // Payment request has been fulfilled
    }

    /**
     * Create faction item tracking table for a specific faction
     */
    public static void createItemTrackingTable(Connection ocDataConnection, String factionSuffix) throws SQLException {
        String tableName = "item_tracking_" + factionSuffix;

        String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "id SERIAL PRIMARY KEY," +
                "crime_name VARCHAR(255) NOT NULL," +
                "log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "user_id VARCHAR(20)," +
                "username VARCHAR(100)," +
                "item_name VARCHAR(255) NOT NULL," +
                "item_price BIGINT," +
                "faction_purchased BOOLEAN," +
                "faction_paid_member VARCHAR(20)," + // NULL, 'PENDING', or 'FULFILLED'
                "payment_request_id VARCHAR(36)," +   // Links to payment_requests table
                "notes TEXT," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = ocDataConnection.createStatement()) {
            stmt.execute(createTableSql);

            // Create indexes for better performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_crime_name ON " + tableName + "(crime_name)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_log_date ON " + tableName + "(log_date DESC)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_user_id ON " + tableName + "(user_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_purchased ON " + tableName + "(faction_purchased)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_faction_paid_member ON " + tableName + "(faction_paid_member)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + tableName + "_payment_request_id ON " + tableName + "(payment_request_id)");

            logger.debug("Item tracking table {} created or verified with indexes", tableName);
        }
    }

    /**
     * Log faction item purchase requirement (from CheckUsersHaveItems)
     */
    public static void logFactionPurchaseRequired(Connection ocDataConnection, String factionSuffix,
                                                  String crimeName, String itemName, Long itemPrice) throws SQLException {
        String tableName = "item_tracking_" + factionSuffix;

        // Create table if it doesn't exist
        createItemTrackingTable(ocDataConnection, factionSuffix);

        String insertSql = "INSERT INTO " + tableName + " (" +
                "crime_name, user_id, username, item_name, item_price, faction_purchased, " +
                "faction_paid_member, notes, last_updated) " +
                "VALUES (?, NULL, NULL, ?, ?, TRUE, NULL, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(insertSql)) {
            pstmt.setString(1, crimeName);
            pstmt.setString(2, itemName);
            if (itemPrice != null) {
                pstmt.setLong(3, itemPrice);
            } else {
                pstmt.setNull(3, Types.BIGINT);
            }
            pstmt.setString(4, "Faction needs to purchase this item for members");

            pstmt.executeUpdate();
            logger.debug("Logged faction purchase requirement: {} for crime {} (price: ${})",
                    itemName, crimeName, itemPrice);
        }
    }

    /**
     * Log member payment requirement (from Overview/Payment system)
     */
    public static void logMemberPaymentRequired(Connection ocDataConnection, String factionSuffix,
                                                String crimeName, String userId, String username,
                                                String itemName, Long itemPrice, String paymentRequestId) throws SQLException {
        String tableName = "item_tracking_" + factionSuffix;

        // Create table if it doesn't exist
        createItemTrackingTable(ocDataConnection, factionSuffix);

        String insertSql = "INSERT INTO " + tableName + " (" +
                "crime_name, user_id, username, item_name, item_price, faction_purchased, " +
                "faction_paid_member, payment_request_id, notes, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, FALSE, 'PENDING', ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(insertSql)) {
            pstmt.setString(1, crimeName);
            pstmt.setString(2, userId);
            pstmt.setString(3, username);
            pstmt.setString(4, itemName);
            if (itemPrice != null) {
                pstmt.setLong(5, itemPrice);
            } else {
                pstmt.setNull(5, Types.BIGINT);
            }
            pstmt.setString(6, paymentRequestId);
            pstmt.setString(7, "Member payment required - request created");

            pstmt.executeUpdate();
            logger.debug("Logged member payment requirement: {} for user {} in crime {} (request: {})",
                    itemName, username, crimeName, paymentRequestId != null ? paymentRequestId.substring(0, 8) + "..." : "none");
        }
    }

    /**
     * Update payment status when payment request is fulfilled
     */
    public static boolean updatePaymentRequestFulfilled(Connection ocDataConnection, String paymentRequestId) throws SQLException {
        if (paymentRequestId == null || paymentRequestId.trim().isEmpty()) {
            return false;
        }

        // Get all faction suffixes to search across all tracking tables
        List<String> factionSuffixes = getAllFactionSuffixes(ocDataConnection);

        boolean updated = false;

        for (String factionSuffix : factionSuffixes) {
            String tableName = "item_tracking_" + factionSuffix;

            try {
                String updateSql = "UPDATE " + tableName + " SET " +
                        "faction_paid_member = 'FULFILLED', " +
                        "notes = COALESCE(notes, '') || ' - Payment fulfilled', " +
                        "last_updated = CURRENT_TIMESTAMP " +
                        "WHERE payment_request_id = ? AND faction_paid_member = 'PENDING'";

                try (PreparedStatement pstmt = ocDataConnection.prepareStatement(updateSql)) {
                    pstmt.setString(1, paymentRequestId);

                    int rowsAffected = pstmt.executeUpdate();
                    if (rowsAffected > 0) {
                        updated = true;
                        logger.info("Updated item tracking: payment request {} marked as fulfilled (table: {})",
                                paymentRequestId.substring(0, 8) + "...", tableName);
                    }
                }
            } catch (SQLException e) {
                logger.debug("Could not update table {} for payment request {}: {}",
                        tableName, paymentRequestId.substring(0, 8) + "...", e.getMessage());
                // Continue to next table
            }
        }

        return updated;
    }

    /**
     * Get faction spending summary for a specific period
     */
    public static FactionSpendingSummary getFactionSpendingSummary(Connection ocDataConnection, String factionSuffix,
                                                                   int days) throws SQLException {
        String tableName = "item_tracking_" + factionSuffix;

        String sql = "SELECT " +
                "COUNT(CASE WHEN faction_purchased = TRUE THEN 1 END) as faction_purchases, " +
                "COUNT(CASE WHEN faction_purchased = FALSE AND faction_paid_member = 'FULFILLED' THEN 1 END) as member_payments_fulfilled, " +
                "COUNT(CASE WHEN faction_purchased = FALSE AND faction_paid_member = 'PENDING' THEN 1 END) as member_payments_pending, " +
                "SUM(CASE WHEN faction_purchased = TRUE THEN COALESCE(item_price, 0) ELSE 0 END) as total_purchase_cost, " +
                "SUM(CASE WHEN faction_purchased = FALSE AND faction_paid_member = 'FULFILLED' THEN COALESCE(item_price, 0) ELSE 0 END) as total_payment_cost " +
                "FROM " + tableName + " " +
                "WHERE log_date > CURRENT_TIMESTAMP - INTERVAL '" + days + " days'";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                return new FactionSpendingSummary(
                        rs.getInt("faction_purchases"),
                        rs.getInt("member_payments_fulfilled"),
                        rs.getInt("member_payments_pending"),
                        rs.getLong("total_purchase_cost"),
                        rs.getLong("total_payment_cost")
                );
            }
        } catch (SQLException e) {
            logger.debug("Could not get spending summary for faction {} (table might not exist): {}",
                    factionSuffix, e.getMessage());
        }

        return new FactionSpendingSummary(0, 0, 0, 0L, 0L);
    }

    /**
     * Get all faction suffixes from existing tables
     */
    private static List<String> getAllFactionSuffixes(Connection ocDataConnection) {
        java.util.List<String> suffixes = new java.util.ArrayList<>();

        // Query information_schema to find all item_tracking tables
        String sql = "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name LIKE 'item_tracking_%'";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String suffix = tableName.substring("item_tracking_".length());
                if (isValidDbSuffix(suffix)) {
                    suffixes.add(suffix);
                }
            }
        } catch (SQLException e) {
            logger.error("Error getting faction suffixes from item tracking tables", e);
        }

        return suffixes;
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
     * Data class for faction spending summary
     */
    public static class FactionSpendingSummary {
        private final int factionPurchases;
        private final int memberPaymentsFulfilled;
        private final int memberPaymentsPending;
        private final long totalPurchaseCost;
        private final long totalPaymentCost;

        public FactionSpendingSummary(int factionPurchases, int memberPaymentsFulfilled, int memberPaymentsPending,
                                      long totalPurchaseCost, long totalPaymentCost) {
            this.factionPurchases = factionPurchases;
            this.memberPaymentsFulfilled = memberPaymentsFulfilled;
            this.memberPaymentsPending = memberPaymentsPending;
            this.totalPurchaseCost = totalPurchaseCost;
            this.totalPaymentCost = totalPaymentCost;
        }

        public int getFactionPurchases() { return factionPurchases; }
        public int getMemberPaymentsFulfilled() { return memberPaymentsFulfilled; }
        public int getMemberPaymentsPending() { return memberPaymentsPending; }
        public long getTotalPurchaseCost() { return totalPurchaseCost; }
        public long getTotalPaymentCost() { return totalPaymentCost; }
        public long getTotalSpending() { return totalPurchaseCost + totalPaymentCost; }

        @Override
        public String toString() {
            return String.format(
                    "FactionSpending{purchases=%d, payments=%d+%d pending, costs=$%,d+$%,d=$%,d total}",
                    factionPurchases, memberPaymentsFulfilled, memberPaymentsPending,
                    totalPurchaseCost, totalPaymentCost, getTotalSpending()
            );
        }
    }
}