package com.Torn.FactionMembers;

import com.Torn.Discord.Members.GetDiscordMembers;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFactionMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetFactionMembers.class);
    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class FactionInfo {
        private final String factionId;
        private final String dbPrefix;
        private final String apiKey;

        public FactionInfo(String factionId, String dbPrefix, String apiKey) {
            this.factionId = factionId;
            this.dbPrefix = dbPrefix;
            this.apiKey = apiKey;
        }

        // Getters
        public String getFactionId() { return factionId; }
        public String getDbPrefix() { return dbPrefix; }
        public String getApiKey() { return apiKey; }
    }

    public static class FactionMember {
        private final String userId;
        private final String username;
        private boolean userInDiscord = false;
        private String userDiscordId = null;

        public FactionMember(String userId, String username) {
            this.userId = userId;
            this.username = username;
        }

        // Getters and setters
        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public boolean isUserInDiscord() { return userInDiscord; }
        public void setUserInDiscord(boolean userInDiscord) { this.userInDiscord = userInDiscord; }
        public String getUserDiscordId() { return userDiscordId; }
        public void setUserDiscordId(String userDiscordId) { this.userDiscordId = userDiscordId; }

        @Override
        public String toString() {
            return "FactionMember{" +
                    "userId='" + userId + '\'' +
                    ", username='" + username + '\'' +
                    ", userInDiscord=" + userInDiscord +
                    ", userDiscordId='" + userDiscordId + '\'' +
                    '}';
        }
    }

    public static void fetchAndProcessAllFactionMembers(Connection connection) throws SQLException, IOException {
        logger.info("Starting to fetch faction members for all factions");

        List<GetDiscordMembers.DiscordMember> discordMembers = GetDiscordMembers.fetchDiscordMembers();
        Map<String, GetDiscordMembers.DiscordMember> discordMemberMap = createDiscordMemberMap(discordMembers);

        // Step 2: Get all faction information
        List<FactionInfo> factions = getFactionInfo(connection);

        // Step 3: Process each faction
        for (FactionInfo factionInfo : factions) {
            try {
                logger.info("Processing faction: {}", factionInfo.getFactionId());
                // Fetch faction members
                List<FactionMember> factionMembers = fetchFactionMembers(factionInfo);

                // Join with Discord data
                joinWithDiscordData(factionMembers, discordMemberMap);

                // Write to database
                writeFactionMembersToDatabase(connection, factionInfo, factionMembers);

                logger.info("Successfully processed {} members for faction {}",
                        factionMembers.size(), factionInfo.getFactionId());

            } catch (Exception e) {
                logger.error("Error processing faction {}: {}", factionInfo.getFactionId(), e.getMessage(), e);
            }
        }
    }

    private static Map<String, GetDiscordMembers.DiscordMember> createDiscordMemberMap(List<GetDiscordMembers.DiscordMember> discordMembers) {
        Map<String, GetDiscordMembers.DiscordMember> map = new HashMap<>();
        for (GetDiscordMembers.DiscordMember member : discordMembers) {
            map.put(member.getUsername().toLowerCase(), member);
        }
        return map;
    }

    private static List<FactionInfo> getFactionInfo(Connection connection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT f.id as faction_id, f.db_prefix, ak.value as api_key " +
                "FROM factions f " +
                "JOIN api_keys ak ON f.id = ak.faction_id " +
                "WHERE ak.active = true " +
                "ORDER BY f.id";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbPrefix = rs.getString(Constants.COLUMN_NAME_DB_PREFIX);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);

                factions.add(new FactionInfo(factionId, dbPrefix, apiKey));
                break;
            }
        }

        logger.info("Found {} factions to process", factions.size());
        return factions;
    }

    private static List<FactionMember> fetchFactionMembers(FactionInfo factionInfo) throws IOException {
        List<FactionMember> members = new ArrayList<>();

        String url = Constants.API_URL_TORN_BASE_URL + "/faction/" + factionInfo.getFactionId() +
                Constants.API_URL_FACTION_MEMBERS + factionInfo.getApiKey();

        Request request = new Request.Builder()
                .url(url)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.error("Failed to fetch faction members for faction {}: HTTP {}",
                        factionInfo.getFactionId(), response.code());
                throw new IOException("API request failed: " + response.code());
            }

            assert response.body() != null;
            String responseBody = response.body().string();
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error
            if (jsonResponse.has("error")) {
                logger.error("API Error for faction {}: {}",
                        factionInfo.getFactionId(), jsonResponse.get("error").asText());
                throw new IOException("API Error: " + jsonResponse.get("error").asText());
            }

            // Parse members
            JsonNode membersNode = jsonResponse.get(Constants.MEMBERS);
            if (membersNode != null) {
                membersNode.fields().forEachRemaining(entry -> {
                    JsonNode memberData = entry.getValue();
                    String userId = entry.getKey();
                    String username = memberData.get("name").asText();

                    members.add(new FactionMember(userId, username));
                });
            }
        }

        logger.info("Fetched {} members for faction {}", members.size(), factionInfo.getFactionId());
        return members;
    }

    private static void joinWithDiscordData(List<FactionMember> factionMembers,
                                            Map<String, GetDiscordMembers.DiscordMember> discordMemberMap) {
        for (FactionMember member : factionMembers) {
            GetDiscordMembers.DiscordMember discordMember = discordMemberMap.get(member.getUsername().toLowerCase());
            if (discordMember != null) {
                member.setUserInDiscord(true);
                member.setUserDiscordId(discordMember.getDiscordId());
                logger.debug("Matched faction member {} with Discord user {}",
                        member.getUsername(), discordMember.getDiscordId());
            }
        }
    }

    private static void writeFactionMembersToDatabase(Connection connection, FactionInfo factionInfo,
                                                      List<FactionMember> members) throws SQLException {
        String tableName = factionInfo.getDbPrefix() + "_faction_members";

        // Create table if it doesn't exist
        createTableIfNotExists(connection, tableName);

        // Clear existing data
        clearExistingData(connection, tableName);

        // Insert new data
        insertFactionMembers(connection, tableName, members);
    }

    private static void createTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "user_id VARCHAR(20) PRIMARY KEY, " +
                "username VARCHAR(100) NOT NULL, " +
                "user_in_discord BOOLEAN DEFAULT FALSE, " +
                "user_discord_id VARCHAR(50), " +
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
            logger.debug("Table {} created or verified", tableName);
        }
    }

    private static void clearExistingData(Connection connection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int deletedRows = pstmt.executeUpdate();
            logger.debug("Cleared {} existing rows from {}", deletedRows, tableName);
        }
    }

    private static void insertFactionMembers(Connection connection, String tableName,
                                             List<FactionMember> members) throws SQLException {
        String sql = "INSERT INTO " + tableName + " (user_id, username, user_in_discord, user_discord_id) " +
                "VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (FactionMember member : members) {
                pstmt.setString(1, member.getUserId());
                pstmt.setString(2, member.getUsername());
                pstmt.setBoolean(3, member.isUserInDiscord());
                pstmt.setString(4, member.getUserDiscordId());
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.info("Inserted {} members into {}", results.length, tableName);
        }
    }
}