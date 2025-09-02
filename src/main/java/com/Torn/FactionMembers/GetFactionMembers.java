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
import java.util.concurrent.TimeUnit;

public class GetFactionMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetFactionMembers.class);

    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TORN_API_RATE_LIMIT_MS = 2000;

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

        // Step 1: Get Discord members
        List<GetDiscordMembers.DiscordMember> discordMembers = GetDiscordMembers.fetchDiscordMembers().join();
        assert discordMembers != null;
        Map<String, GetDiscordMembers.DiscordMember> discordMemberMap = createDiscordMemberMap(discordMembers);
        logger.info("Created Discord member map with {} members", discordMembers.size());

        // Step 2: Get all faction information
        List<FactionInfo> factions = getFactionInfo(connection);
        if (factions.isEmpty()) {
            logger.warn("No active factions found to process");
            return;
        }

        // Step 3: Process each faction with rate limiting
        int processedCount = 0;
        for (FactionInfo factionInfo : factions) {
            try {
                logger.info("Processing faction: {} ({}/{})",
                        factionInfo.getFactionId(), processedCount + 1, factions.size());

                // Fetch faction members
                List<FactionMember> factionMembers = fetchFactionMembers(factionInfo);

                // Join with Discord data
                joinWithDiscordData(factionMembers, discordMemberMap);

                // Write to database
                writeFactionMembersToDatabase(connection, factionInfo, factionMembers);

                logger.info("Successfully processed {} members for faction {}",
                        factionMembers.size(), factionInfo.getFactionId());

                processedCount++;

                // Rate limiting between factions (except for the last one)
                if (processedCount < factions.size()) {
                    logger.debug("Waiting {}ms before processing next faction", TORN_API_RATE_LIMIT_MS);
                    Thread.sleep(TORN_API_RATE_LIMIT_MS);
                }

            } catch (InterruptedException e) {
                logger.warn("Faction processing interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing faction {}: {}", factionInfo.getFactionId(), e.getMessage(), e);
                // Continue with other factions
            }
        }

        logger.info("Completed processing {} of {} factions", processedCount, factions.size());
    }

    private static Map<String, GetDiscordMembers.DiscordMember> createDiscordMemberMap(List<GetDiscordMembers.DiscordMember> discordMembers) {
        Map<String, GetDiscordMembers.DiscordMember> map = new HashMap<>();
        for (GetDiscordMembers.DiscordMember member : discordMembers) {
            // Use lowercase for case-insensitive matching
            map.put(member.getUsername().toLowerCase().trim(), member);
        }
        logger.debug("Created Discord member lookup map with {} entries", map.size());
        return map;
    }

    private static List<FactionInfo> getFactionInfo(Connection connection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT DISTINCT ON (f." + Constants.COLUMN_NAME_FACTION_ID + ") " +
                "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                "f." + Constants.COLUMN_NAME_DB_PREFIX + ", " +
                "ak." + Constants.COLUMN_NAME_API_KEY + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbPrefix = rs.getString(Constants.COLUMN_NAME_DB_PREFIX);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);

                // Validate data
                if (factionId == null || dbPrefix == null || apiKey == null) {
                    logger.warn("Skipping faction with null data: factionId={}, dbPrefix={}, apiKey={}",
                            factionId, dbPrefix, (apiKey == null ? "null" : "***"));
                    continue;
                }

                // Validate dbPrefix for SQL injection prevention
                if (!isValidDbPrefix(dbPrefix)) {
                    logger.error("Invalid db_prefix for faction {}: {}", factionId, dbPrefix);
                    continue;
                }

                factions.add(new FactionInfo(factionId, dbPrefix, apiKey));
            }
        }

        logger.info("Found {} active factions to process", factions.size());
        return factions;
    }


    private static boolean isValidDbPrefix(String dbPrefix) {
        // Allow only alphanumeric characters and underscores, no spaces or special chars
        return dbPrefix != null && dbPrefix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") && dbPrefix.length() <= 50;
    }

    private static List<FactionMember> fetchFactionMembers(FactionInfo factionInfo) throws IOException {
        List<FactionMember> members = new ArrayList<>();

        // Construct proper Torn API URL
        String url = Constants.API_URL_TORN_BASE_URL + "faction/" + factionInfo.getFactionId() +
                "/members?key=" + factionInfo.getApiKey();

        logger.debug("Fetching faction members from: {}", url.replaceAll("key=[^&]+", "key=***"));

        Request request = new Request.Builder()
                .url(url)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.error("Failed to fetch faction members for faction {}: HTTP {}",
                        factionInfo.getFactionId(), response.code());

                if (response.code() == 401) {
                    logger.error("API key may be invalid for faction {}", factionInfo.getFactionId());
                } else if (response.code() == 429) {
                    logger.warn("Rate limited by Torn API for faction {}", factionInfo.getFactionId());
                }

                throw new IOException("API request failed: " + response.code());
            }

            if (response.body() == null) {
                throw new IOException("Torn API response body is null");
            }

            String responseBody = response.body().string();
            logger.debug("Raw API response for faction {}: {}", factionInfo.getFactionId(), responseBody);

            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error
            if (jsonResponse.has("error")) {
                String errorMsg = jsonResponse.get("error").asText();
                logger.error("Torn API Error for faction {}: {}", factionInfo.getFactionId(), errorMsg);
                throw new IOException("Torn API Error: " + errorMsg);
            }

            // Parse members - Torn API returns members as an ARRAY, not an object
            JsonNode membersNode = jsonResponse.get("members");
            if (membersNode != null && membersNode.isArray()) {
                for (JsonNode memberNode : membersNode) {
                    try {
                        // Get ID and name from each member object
                        JsonNode idNode = memberNode.get("id");
                        JsonNode nameNode = memberNode.get("name");

                        if (idNode != null && nameNode != null) {
                            String userId = idNode.asText();
                            String username = nameNode.asText();
                            members.add(new FactionMember(userId, username));
                        } else {
                            logger.warn("Missing id or name in member data for faction {}", factionInfo.getFactionId());
                        }
                    } catch (Exception e) {
                        logger.warn("Error parsing member data for faction {}: {}", factionInfo.getFactionId(), e.getMessage());
                    }
                }
            } else {
                logger.warn("No members array found in API response for faction {}", factionInfo.getFactionId());
            }
        }

        logger.info("Fetched {} members for faction {}", members.size(), factionInfo.getFactionId());
        return members;
    }

    private static void joinWithDiscordData(List<FactionMember> factionMembers,
                                            Map<String, GetDiscordMembers.DiscordMember> discordMemberMap) {
        int matchedCount = 0;
        for (FactionMember member : factionMembers) {
            String lookupKey = member.getUsername().toLowerCase().trim();
            GetDiscordMembers.DiscordMember discordMember = discordMemberMap.get(lookupKey);

            if (discordMember != null) {
                member.setUserInDiscord(true);
                member.setUserDiscordId(discordMember.getDiscordId());
                matchedCount++;
                logger.debug("Matched faction member '{}' with Discord user {}",
                        member.getUsername(), discordMember.getDiscordId());
            }
        }

        logger.info("Matched {} of {} faction members with Discord users", matchedCount, factionMembers.size());
    }

    private static void writeFactionMembersToDatabase(Connection connection, FactionInfo factionInfo,
                                                      List<FactionMember> members) throws SQLException {
        String tableName = factionInfo.getDbPrefix() + Constants.FACTION_MEMBERS_TABLE_SUFFIX;

        logger.debug("Writing {} members to table: {}", members.size(), tableName);

        // Create table if it doesn't exist
        createTableIfNotExists(connection, tableName);

        // Clear existing data and insert new data in a transaction
        connection.setAutoCommit(false);
        try {
            clearExistingData(connection, tableName);
            insertFactionMembers(connection, tableName, members);
            connection.commit();
            logger.info("Successfully updated table {} with {} members", tableName, members.size());
        } catch (SQLException e) {
            connection.rollback();
            logger.error("Failed to update table {}, rolling back", tableName, e);
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private static void createTableIfNotExists(Connection connection, String tableName) throws SQLException {
        // Use parameterized table name safely (already validated)
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
        if (members.isEmpty()) {
            logger.info("No members to insert into {}", tableName);
            return;
        }

        String sql = "INSERT INTO " + tableName + " (user_id, username, user_in_discord, user_discord_id, updated_at) " +
                "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)";

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