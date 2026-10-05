package com.Torn.ItemManagement;

import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PendingItemNotificationDAO {

    private static final Logger logger = LoggerFactory.getLogger(PendingItemNotificationDAO.class);

    public static void createTableIfNotExists(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + Constants.TABLE_NAME_PENDING_ITEM_NOTIFICATIONS + " (" +
                "id SERIAL PRIMARY KEY," +
                "faction_id VARCHAR(20) NOT NULL," +
                "user_id VARCHAR(20) NOT NULL," +
                "username VARCHAR(100) NOT NULL," +
                "crime_id BIGINT," +
                "crime_name VARCHAR(255)," +
                "role VARCHAR(100)," +
                "item_required VARCHAR(255)," +
                "item_average_price INTEGER," +
                "detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_pending_item_notif_faction_id ON " +
                    Constants.TABLE_NAME_PENDING_ITEM_NOTIFICATIONS + "(faction_id)");
        }

        logger.debug("Pending item notifications table created or verified");
    }

    public static void insertNotification(Connection connection, String factionId, String userId,
                                          String username, Long crimeId, String crimeName,
                                          String role, String itemRequired, Integer itemAveragePrice) throws SQLException {
        String sql = "INSERT INTO " + Constants.TABLE_NAME_PENDING_ITEM_NOTIFICATIONS +
                " (faction_id, user_id, username, crime_id, crime_name, role, item_required, item_average_price) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);
            pstmt.setString(2, userId);
            pstmt.setString(3, username);
            if (crimeId != null) pstmt.setLong(4, crimeId); else pstmt.setNull(4, Types.BIGINT);
            pstmt.setString(5, crimeName);
            pstmt.setString(6, role);
            pstmt.setString(7, itemRequired);
            if (itemAveragePrice != null) pstmt.setInt(8, itemAveragePrice); else pstmt.setNull(8, Types.INTEGER);
            pstmt.executeUpdate();
        }

        logger.debug("Inserted pending item notification for user {} in faction {}", username, factionId);
    }

    public static List<CheckUsersHaveItems.UserItemRequest> getNotificationsForFaction(
            Connection connection, String factionId) throws SQLException {

        List<CheckUsersHaveItems.UserItemRequest> results = new ArrayList<>();

        String sql = "SELECT user_id, username, item_required, crime_id, crime_name, role, item_average_price " +
                "FROM " + Constants.TABLE_NAME_PENDING_ITEM_NOTIFICATIONS + " " +
                "WHERE faction_id = ? " +
                "ORDER BY crime_id, username";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new CheckUsersHaveItems.UserItemRequest(
                            rs.getString("user_id"),
                            rs.getString("username"),
                            rs.getString("item_required"),
                            rs.getObject("crime_id", Long.class),
                            rs.getString("crime_name"),
                            rs.getString("role"),
                            rs.getObject("item_average_price", Integer.class),
                            false
                    ));
                }
            }
        }

        logger.debug("Retrieved {} pending item notifications for faction {}", results.size(), factionId);
        return results;
    }

    public static void deleteNotificationsForFaction(Connection connection, String factionId) throws SQLException {
        String sql = "DELETE FROM " + Constants.TABLE_NAME_PENDING_ITEM_NOTIFICATIONS + " WHERE faction_id = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, factionId);
            int deleted = pstmt.executeUpdate();
            logger.debug("Deleted {} pending item notifications for faction {}", deleted, factionId);
        }
    }
}
