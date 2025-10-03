package com.Torn.PaymentRequests;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.Torn.ItemManagement.FactionItemTracking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Data Access Object for PaymentRequest operations
 */
public class PaymentRequestDAO {

    /**
     * ├── PaymentRequestDAO.java
     * │   ├── Database operations for payment requests
     * │   ├── Handles claim/fulfill workflow
     * │   ├── Manages request expiration and reset
     * │   └── Provides system statistics
     */

    private static final Logger logger = LoggerFactory.getLogger(PaymentRequestDAO.class);

    /**
     * Create the payment requests table if it doesn't exist
     */
    public static void createTableIfNotExists(Connection connection) throws SQLException {
        String createTableSql = "CREATE TABLE IF NOT EXISTS payment_requests (" +
                "request_id VARCHAR(6) PRIMARY KEY," + // Changed from VARCHAR(36) to VARCHAR(6)
                "faction_id VARCHAR(20) NOT NULL," +
                "user_id VARCHAR(20) NOT NULL," +
                "username VARCHAR(100) NOT NULL," +
                "crime_id BIGINT," +
                "role VARCHAR(100)," +
                "item_required VARCHAR(255)," +
                "item_value BIGINT," +
                "status VARCHAR(20) NOT NULL DEFAULT 'PENDING'," +
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "claimed_at TIMESTAMP," +
                "claimed_by_user_id VARCHAR(20)," +
                "fulfilled_at TIMESTAMP," +
                "fulfilment_method VARCHAR(50)," +
                "payment_news_id VARCHAR(100)," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            // Create sequence for auto-incrementing IDs if it doesn't exist
            stmt.execute("CREATE SEQUENCE IF NOT EXISTS payment_request_id_seq START 1 MAXVALUE 999999 CYCLE");

            // Create indexes for better performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_payment_requests_faction_id ON payment_requests(faction_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_payment_requests_status ON payment_requests(status)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_payment_requests_user_id ON payment_requests(user_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_payment_requests_created_at ON payment_requests(created_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_payment_requests_claimed_at ON payment_requests(claimed_at)");

            logger.debug("Payment requests table created or verified with indexes and sequence");
        }
    }

    /**
     * Generate next sequential 6-digit request ID
     */
    private static String generateNextRequestId(Connection connection) throws SQLException {
        String sql = "SELECT LPAD(nextval('payment_request_id_seq')::text, 6, '0')";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                return rs.getString(1);
            }
        }

        throw new SQLException("Failed to generate request ID");
    }

    /**
     * Insert a new payment request and return the generated request ID
     */
    public static String insertPaymentRequest(Connection connection, PaymentRequest request) throws SQLException {
        String requestId = generateNextRequestId(connection);
        request.setRequestId(requestId);

        String sql = "INSERT INTO payment_requests (request_id, faction_id, user_id, username, " +
                "crime_id, role, item_required, item_value, status, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, request.getRequestId());
            pstmt.setString(2, request.getFactionId());
            pstmt.setString(3, request.getUserId());
            pstmt.setString(4, request.getUsername());

            if (request.getCrimeId() != null) {
                pstmt.setLong(5, request.getCrimeId());
            } else {
                pstmt.setNull(5, Types.BIGINT);
            }

            pstmt.setString(6, request.getRole());
            pstmt.setString(7, request.getItemRequired());

            if (request.getItemValue() != null) {
                pstmt.setLong(8, request.getItemValue());
            } else {
                pstmt.setNull(8, Types.BIGINT);
            }

            pstmt.setString(9, request.getStatus().name());
            pstmt.setTimestamp(10, request.getCreatedAt());

            int rowsAffected = pstmt.executeUpdate();

            if (rowsAffected > 0) {
                logger.debug("Inserted payment request: {} for user {} (faction: {})",
                        requestId, request.getUsername(), request.getFactionId());
                return requestId;
            } else {
                throw new SQLException("Failed to insert payment request - no rows affected");
            }
        }
    }


    /**
     * Get payment request by ID
     */
    public static PaymentRequest getPaymentRequest(Connection connection, String requestId) throws SQLException {
        String sql = "SELECT * FROM payment_requests WHERE request_id = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, requestId);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToPaymentRequest(rs);
                }
            }
        }

        return null;
    }

    /**
     * Attempt to claim a payment request (atomic operation)
     * Returns true if successfully claimed, false if already claimed
     */
    public static boolean claimPaymentRequest(Connection connection, String requestId, String claimedByUserId) throws SQLException {
        String sql = "UPDATE payment_requests SET " +
                "status = 'CLAIMED', " +
                "claimed_at = CURRENT_TIMESTAMP, " +
                "claimed_by_user_id = ?, " +
                "last_updated = CURRENT_TIMESTAMP " +
                "WHERE request_id = ? AND status = 'PENDING'";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, claimedByUserId);
            pstmt.setString(2, requestId);

            int rowsAffected = pstmt.executeUpdate();
            boolean success = rowsAffected > 0;

            if (success) {
                logger.info("Payment request {} claimed by user {}", requestId, claimedByUserId);
            } else {
                logger.warn("Failed to claim payment request {} - may already be claimed or not exist", requestId);
            }

            return success;
        }
    }

    /**
     * Mark payment request as fulfilled
     */
    public static boolean fulfillPaymentRequest(Connection connection, String requestId,
                                                String method, String newsId) throws SQLException {
        String sql = "UPDATE payment_requests SET " +
                "status = 'FULFILLED', " +
                "fulfilled_at = CURRENT_TIMESTAMP, " +
                "fulfilment_method = ?, " +
                "payment_news_id = ?, " +
                "last_updated = CURRENT_TIMESTAMP " +
                "WHERE request_id = ? AND status IN ('CLAIMED','PENDING')";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, method);
            pstmt.setString(2, newsId);
            pstmt.setString(3, requestId);

            int rowsAffected = pstmt.executeUpdate();
            boolean success = rowsAffected > 0;

            if (success) {
                logger.info("Payment request {} fulfilled via method: {}", requestId, method);

                // Update item tracking tables
                try {
                    String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
                    if (ocDataDatabaseUrl != null && !ocDataDatabaseUrl.isEmpty()) {
                        try (Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl,
                                LoggerFactory.getLogger(PaymentRequestDAO.class))) {
                            FactionItemTracking.updatePaymentRequestFulfilled(ocDataConnection, requestId);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to update item tracking for fulfilled payment {}: {}", requestId, e.getMessage());
                }
            } else {
                logger.warn("Failed to mark payment request {} as fulfilled - may not be in CLAIMED status", requestId);
            }

            return success;
        }
    }

    /**
     * Get expired claimed requests (claimed more than 15 minutes ago)
     */
    public static List<PaymentRequest> getExpiredClaimedRequests(Connection connection) throws SQLException {
        String sql = "SELECT * FROM payment_requests " +
                "WHERE status = 'CLAIMED' " +
                "AND claimed_at < (CURRENT_TIMESTAMP - INTERVAL '1 hour') " +
                "ORDER BY claimed_at ASC";

        List<PaymentRequest> expiredRequests = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                expiredRequests.add(mapResultSetToPaymentRequest(rs));
            }
        }

        logger.debug("Found {} expired claimed requests", expiredRequests.size());
        return expiredRequests;
    }

    /**
     * Reset expired requests back to PENDING status
     */
    public static int resetExpiredRequests(Connection connection, List<String> requestIds) throws SQLException {
        if (requestIds.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder("UPDATE payment_requests SET " +
                "status = 'PENDING', " +
                "claimed_at = NULL, " +
                "claimed_by_user_id = NULL, " +
                "last_updated = CURRENT_TIMESTAMP " +
                "WHERE request_id IN (");

        for (int i = 0; i < requestIds.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");

        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < requestIds.size(); i++) {
                pstmt.setString(i + 1, requestIds.get(i));
            }

            int rowsAffected = pstmt.executeUpdate();
            logger.info("Reset {} expired payment requests back to PENDING", rowsAffected);
            return rowsAffected;
        }
    }

    /**
     * Get pending requests for a faction
     */
    public static List<PaymentRequest> getPendingRequestsForFaction(Connection connection, String factionId) throws SQLException {
        String sql = "SELECT * FROM payment_requests " +
                "WHERE faction_id = ? AND status = 'PENDING' " +
                "ORDER BY created_at ASC";

        List<PaymentRequest> requests = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    requests.add(mapResultSetToPaymentRequest(rs));
                }
            }
        }

        logger.debug("Found {} pending requests for faction {}", requests.size(), factionId);
        return requests;
    }

    /**
     * Get claimed requests for a specific faction (for payment verification)
     */
    public static List<PaymentRequest> getClaimedRequestsForFaction(Connection connection, String factionId) throws SQLException {
        String sql = "SELECT * FROM payment_requests " +
                "WHERE faction_id = ? AND status = 'CLAIMED' " +
                "ORDER BY claimed_at DESC";

        List<PaymentRequest> requests = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    requests.add(mapResultSetToPaymentRequest(rs));
                }
            }
        }

        logger.debug("Found {} claimed requests for faction {}", requests.size(), factionId);
        return requests;
    }

    /**
     * Get system statistics for the last 24 hours
     */
    public static PaymentSystemStats getSystemStats(Connection connection) throws SQLException {
        String sql = "SELECT " +
                "COUNT(*) as total_requests, " +
                "COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_requests, " +
                "COUNT(CASE WHEN status = 'CLAIMED' THEN 1 END) as claimed_requests, " +
                "COUNT(CASE WHEN status = 'FULFILLED' THEN 1 END) as fulfilled_requests, " +
                "COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) as expired_requests, " +
                "AVG(CASE WHEN fulfilled_at IS NOT NULL AND claimed_at IS NOT NULL " +
                "    THEN EXTRACT(EPOCH FROM (fulfilled_at - claimed_at))/60 END) as avg_fulfillment_time_minutes " +
                "FROM payment_requests " +
                "WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                return new PaymentSystemStats(
                        rs.getInt("total_requests"),
                        rs.getInt("pending_requests"),
                        rs.getInt("claimed_requests"),
                        rs.getInt("fulfilled_requests"),
                        rs.getInt("expired_requests"),
                        rs.getDouble("avg_fulfillment_time_minutes")
                );
            }
        }

        return new PaymentSystemStats(0, 0, 0, 0, 0, 0.0);
    }

    /**
     * Helper method to map ResultSet to PaymentRequest object
     */
    private static PaymentRequest mapResultSetToPaymentRequest(ResultSet rs) throws SQLException {
        PaymentRequest request = new PaymentRequest();
        request.setRequestId(rs.getString("request_id"));
        request.setFactionId(rs.getString("faction_id"));
        request.setUserId(rs.getString("user_id"));
        request.setUsername(rs.getString("username"));
        request.setCrimeId(rs.getObject("crime_id", Long.class));
        request.setRole(rs.getString("role"));
        request.setItemRequired(rs.getString("item_required"));
        request.setItemValue(rs.getObject("item_value", Long.class));
        request.setStatus(PaymentRequest.RequestStatus.valueOf(rs.getString("status")));
        request.setCreatedAt(rs.getTimestamp("created_at"));
        request.setClaimedAt(rs.getTimestamp("claimed_at"));
        request.setClaimedByUserId(rs.getString("claimed_by_user_id"));
        request.setFulfilledAt(rs.getTimestamp("fulfilled_at"));
        request.setFulfilmentMethod(rs.getString("fulfilment_method"));
        request.setPaymentNewsId(rs.getString("payment_news_id"));

        return request;
    }

    /**
     * Statistics class for system monitoring
     */
    public static class PaymentSystemStats {
        private final int totalRequests;
        private final int pendingRequests;
        private final int claimedRequests;
        private final int fulfilledRequests;
        private final int expiredRequests;
        private final double avgFulfillmentTimeMinutes;

        public PaymentSystemStats(int totalRequests, int pendingRequests, int claimedRequests,
                                  int fulfilledRequests, int expiredRequests, double avgFulfillmentTimeMinutes) {
            this.totalRequests = totalRequests;
            this.pendingRequests = pendingRequests;
            this.claimedRequests = claimedRequests;
            this.fulfilledRequests = fulfilledRequests;
            this.expiredRequests = expiredRequests;
            this.avgFulfillmentTimeMinutes = avgFulfillmentTimeMinutes;
        }

        // Getters
        public int getTotalRequests() { return totalRequests; }
        public int getPendingRequests() { return pendingRequests; }
        public int getClaimedRequests() { return claimedRequests; }
        public int getFulfilledRequests() { return fulfilledRequests; }
        public int getExpiredRequests() { return expiredRequests; }
        public double getAvgFulfillmentTimeMinutes() { return avgFulfillmentTimeMinutes; }

        @Override
        public String toString() {
            return String.format(
                    "PaymentSystemStats{total=%d, pending=%d, claimed=%d, fulfilled=%d, expired=%d, avgTime=%.1fm}",
                    totalRequests, pendingRequests, claimedRequests, fulfilledRequests,
                    expiredRequests, avgFulfillmentTimeMinutes
            );
        }
    }
}