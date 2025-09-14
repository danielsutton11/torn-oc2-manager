package com.Torn.FactionMembers;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
├── GetFactionMembers.java
│   ├── Fetches faction member data from Torn API
│   ├── Syncs with Discord data to identify Discord users
│   └── Stores enriched member data in faction-specific tables
 **/

public class GetFactionMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetFactionMembers.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TORN_API_RATE_LIMIT_MS = 2000;
    private static final int TORNSTATS_API_RATE_LIMIT_MS = 1000; // Be respectful to TornStats API

    // HTTP client for TornStats API calls
    private static final OkHttpClient tornStatsClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;
        private final String apiKey;

        public FactionInfo(String factionId, String dbPrefix, String apiKey) {
            this.factionId = factionId;
            this.dbSuffix = dbPrefix;
            this.apiKey = apiKey;
        }

        // Getters
        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
        public String getApiKey() { return apiKey; }
    }

    public static class FactionMember {
        private final String userId;
        private final String username;
        private boolean userInDiscord = false;
        private String userDiscordId = null;
        private String userDiscordMentionId = null;
        private int crimeExpRank = 100; // Default to 100 if not found

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
        public String getUserDiscordMentionId() { return userDiscordMentionId; }
        public void setUserDiscordMentionId(String userDiscordMentionId) { this.userDiscordMentionId = userDiscordMentionId; }
        public int getCrimeExpRank() { return crimeExpRank; }
        public void setCrimeExpRank(int crimeExpRank) { this.crimeExpRank = crimeExpRank; }

        @Override
        public String toString() {
            return "FactionMember{" +
                    "userId='" + userId + '\'' +
                    ", username='" + username + '\'' +
                    ", userInDiscord=" + userInDiscord +
                    ", userDiscordId='" + userDiscordId +
                    ", userDiscordMentionId='" + userDiscordMentionId + '\'' +
                    ", crimeExpRank=" + crimeExpRank +
                    '}';
        }
    }

    public static void fetchAndProcessAllFactionMembers(Connection connection) throws SQLException, IOException {
        logger.info("Starting to fetch faction members for all factions");

        // Step 1: Get Discord members (with error handling)
        List<GetDiscordMembers.DiscordMember> discordMembers;
        try {
            CompletableFuture<List<GetDiscordMembers.DiscordMember>> discordFuture = GetDiscordMembers.fetchDiscordMembers();
            discordMembers = discordFuture != null ? discordFuture.join() : new ArrayList<>();
            if (discordMembers == null) {
                logger.error("Failed to fetch Discord members - continuing with faction sync only");
                discordMembers = new ArrayList<>();
            }
        } catch (Exception e) {
            logger.error("Discord API failed: {} - continuing with faction sync only", e.getMessage());
            discordMembers = new ArrayList<>();
        }

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
        int successfulCount = 0;
        int failedCount = 0;

        for (FactionInfo factionInfo : factions) {
            try {
                logger.info("Processing faction: {} ({}/{})",
                        factionInfo.getFactionId(), processedCount + 1, factions.size());

                // Fetch faction members
                List<FactionMember> factionMembers = fetchFactionMembers(factionInfo);

                if (factionMembers != null) {
                    // Join with Discord data
                    joinWithDiscordData(factionMembers, discordMemberMap);

                    // Fetch crime experience ranks from TornStats
                    fetchCrimeExpRanks(factionMembers, factionInfo);

                    // Write to database
                    writeFactionMembersToDatabase(connection, factionInfo, factionMembers);

                    logger.info("Successfully processed {} members for faction {}",
                            factionMembers.size(), factionInfo.getFactionId());
                    successfulCount++;
                } else {
                    logger.error("Failed to fetch members for faction {} - skipping", factionInfo.getFactionId());
                    failedCount++;
                }

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
                failedCount++;
                // Continue with other factions instead of failing completely
            }
        }

        logger.info("Faction processing summary: {} processed, {} successful, {} failed",
                processedCount, successfulCount, failedCount);

        // If more than half failed, this might indicate a systemic issue
        if (failedCount > successfulCount && processedCount > 2) {
            logger.error("More than half of factions failed - Torn API may be experiencing issues");
        }
    }

    /**
     * Fetch crime experience ranks from TornStats API
     */
    private static void fetchCrimeExpRanks(List<FactionMember> factionMembers, FactionInfo factionInfo) {
        if (factionInfo.getApiKey() == null || factionInfo.getApiKey().trim().isEmpty()) {
            logger.warn("No TornStats API key configured for faction {} - using default crime_exp_rank of 100",
                    factionInfo.getFactionId());
            return;
        }

        try {
            logger.info("Fetching crime experience ranks from TornStats for faction {}", factionInfo.getFactionId());

            String url = "https://www.tornstats.com/api/v2/" + factionInfo.getApiKey() + "/faction/crimes";

            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("User-Agent", "TornBot/1.0")
                    .build();

            try (Response response = tornStatsClient.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    String responseBody = response.body().string();
                    Map<String, Integer> crimeExpRanks = parseTornStatsCrimeRanks(responseBody);

                    // Match crime exp ranks with faction members
                    int matchedCount = 0;
                    for (FactionMember member : factionMembers) {
                        Integer crimeExpRank = crimeExpRanks.get(member.getUserId());
                        if (crimeExpRank != null) {
                            member.setCrimeExpRank(crimeExpRank);
                            matchedCount++;
                        } else {
                            // Keep default value of 100
                            logger.debug("No crime exp rank found for user {} [{}] - using default 100",
                                    member.getUsername(), member.getUserId());
                        }
                    }

                    logger.info("Matched crime exp ranks for {} of {} faction members",
                            matchedCount, factionMembers.size());

                } else {
                    logger.warn("TornStats API request failed for faction {} - HTTP {}: {} - using default ranks",
                            factionInfo.getFactionId(), response.code(), response.message());
                }
            }

            // Rate limiting for TornStats API
            Thread.sleep(TORNSTATS_API_RATE_LIMIT_MS);

        } catch (Exception e) {
            logger.error("Error fetching crime exp ranks from TornStats for faction {}: {} - using default ranks",
                    factionInfo.getFactionId(), e.getMessage(), e);
        }
    }

    /**
     * Parse TornStats crime ranks response
     */
    private static Map<String, Integer> parseTornStatsCrimeRanks(String responseBody) throws IOException {
        Map<String, Integer> crimeExpRanks = new HashMap<>();

        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error
            if (jsonResponse.has("status") && !jsonResponse.get("status").asBoolean()) {
                String message = jsonResponse.has("message") ? jsonResponse.get("message").asText() : "Unknown error";
                logger.warn("TornStats API error: {}", message);
                return crimeExpRanks; // Return empty map
            }

            // Parse members data - assuming the response structure matches the provided document
            JsonNode membersNode = jsonResponse.get("members");
            if (membersNode != null && membersNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = membersNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String userId = entry.getKey();
                    JsonNode memberData = entry.getValue();

                    if (memberData.has("crime_exp_rank")) {
                        int crimeExpRank = memberData.get("crime_exp_rank").asInt(100); // Default to 100 if invalid
                        crimeExpRanks.put(userId, crimeExpRank);
                    }
                }
            }

            logger.debug("Parsed {} crime exp ranks from TornStats response", crimeExpRanks.size());

        } catch (Exception e) {
            logger.error("Error parsing TornStats response: {}", e.getMessage());
            throw new IOException("Failed to parse TornStats response: " + e.getMessage());
        }

        return crimeExpRanks;
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
                "f." + Constants.COLUMN_NAME_DB_SUFFIX + ", " +
                "ak." + Constants.COLUMN_NAME_API_KEY + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);

                // Validate data
                if (factionId == null || dbSuffix == null || apiKey == null) {
                    logger.warn("Skipping faction with null data: factionId={}, dbSuffix={}, apiKey={}",
                            factionId, dbSuffix, (apiKey == null ? "null" : "***"));
                    continue;
                }

                // Validate dbSuffix for SQL injection prevention
                if (!isValidDbSuffix(dbSuffix)) {
                    logger.error("Invalid db_suffix for faction {}: {}", factionId, dbSuffix);
                    continue;
                }

                factions.add(new FactionInfo(factionId, dbSuffix, apiKey));
            }
        }

        logger.info("Found {} active factions to process", factions.size());
        return factions;
    }

    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }

    private static List<FactionMember> fetchFactionMembers(FactionInfo factionInfo) throws IOException {
        List<FactionMember> members = new ArrayList<>();

        // Construct proper Torn API URL
        String url = Constants.API_URL_FACTION + "/" + factionInfo.getFactionId() + Constants.API_URL_FACTION_MEMBERS;

        logger.debug("Fetching faction members for faction: {}", factionInfo.getFactionId());

        // Use the robust API handler instead of direct HTTP call
        ApiResponse response = TornApiHandler.executeRequest(url, factionInfo.getApiKey());

        // Handle different response types
        if (response.isSuccess()) {
            logger.info("Successfully fetched faction members for faction {}", factionInfo.getFactionId());
            return parseFactionMembers(response.getBody(), factionInfo);

        } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
            logger.error("Circuit breaker is open - skipping faction {} to prevent further API failures",
                    factionInfo.getFactionId());
            throw new IOException("Circuit breaker open - API calls suspended");

        } else if (response.isAuthenticationIssue()) {
            logger.error("API key authentication issue for faction {}: {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("API key authentication failed: " + response.getErrorMessage());

        } else if (response.isTemporaryError()) {
            logger.warn("Temporary API error for faction {} (will retry on next run): {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("Temporary API error: " + response.getErrorMessage());

        } else {
            logger.error("Permanent API error for faction {}: {}",
                    factionInfo.getFactionId(), response.getErrorMessage());
            throw new IOException("API error: " + response.getErrorMessage());
        }
    }

    private static List<FactionMember> parseFactionMembers(String responseBody, FactionInfo factionInfo) throws IOException {
        List<FactionMember> members = new ArrayList<>();

        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for API error in response
            if (jsonResponse.has("error")) {
                String errorMsg = jsonResponse.get("error").asText();
                logger.error("Torn API Error for faction {}: {}", factionInfo.getFactionId(), errorMsg);
                throw new IOException("Torn API Error: " + errorMsg);
            }

            // Parse members - Torn API returns members as an ARRAY, not an object
            JsonNode membersNode = jsonResponse.get(Constants.NODE_MEMBERS);
            if (membersNode != null && membersNode.isArray()) {
                for (JsonNode memberNode : membersNode) {
                    try {
                        JsonNode idNode = memberNode.get(Constants.NODE_ID);
                        JsonNode nameNode = memberNode.get(Constants.NODE_NAME);

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

        } catch (Exception e) {
            logger.error("Error parsing JSON response for faction {}: {}", factionInfo.getFactionId(), e.getMessage());
            throw new IOException("JSON parsing error: " + e.getMessage());
        }

        logger.info("Parsed {} members for faction {}", members.size(), factionInfo.getFactionId());
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
                member.setUserDiscordMentionId(discordMember.getMention());
                matchedCount++;
                logger.debug("Matched faction member '{}' with Discord user {}",
                        member.getUsername(), discordMember.getDiscordId());
            }
        }

        logger.info("Matched {} of {} faction members with Discord users", matchedCount, factionMembers.size());
    }

    private static void writeFactionMembersToDatabase(Connection connection, FactionInfo factionInfo,
                                                      List<FactionMember> members) throws SQLException {
        String tableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionInfo.getDbSuffix();

        logger.debug("Writing {} members to table: {}", members.size(), tableName);

        // Create table if it doesn't exist
        createTableIfNotExists(connection, tableName);

        // Clear existing data and insert new data in a transaction
        connection.setAutoCommit(false);
        try {
            clearExistingData(connection, tableName);
            insertFactionMembers(connection, tableName, members, factionInfo.getFactionId());
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
        // Updated to include crime_exp_rank column
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "user_id VARCHAR(20) PRIMARY KEY, " +
                "username VARCHAR(100) NOT NULL, " +
                "faction_id BIGINT NOT NULL, " +
                "user_in_discord BOOLEAN DEFAULT FALSE, " +
                "user_discord_id VARCHAR(50), " +
                "user_discord_mention_id VARCHAR(50), " +
                "crime_exp_rank INTEGER DEFAULT 100, " + // New column
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
                                             List<FactionMember> members, String factionId) throws SQLException {
        if (members.isEmpty()) {
            logger.info("No members to insert into {}", tableName);
            return;
        }

        // Updated SQL to include crime_exp_rank column
        String sql = "INSERT INTO " + tableName + " (user_id, username, faction_id, user_in_discord, user_discord_id, user_discord_mention_id, crime_exp_rank, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            long factionIdLong;
            try {
                factionIdLong = Long.parseLong(factionId);
            } catch (NumberFormatException e) {
                logger.error("Invalid faction ID format: {} - cannot convert to number", factionId);
                throw new SQLException("Invalid faction ID format: " + factionId, e);
            }

            for (FactionMember member : members) {
                pstmt.setString(1, member.getUserId());                    // Parameter 1: user_id
                pstmt.setString(2, member.getUsername());                  // Parameter 2: username
                pstmt.setLong(3, factionIdLong);                          // Parameter 3: faction_id (BIGINT)
                pstmt.setBoolean(4, member.isUserInDiscord());            // Parameter 4: user_in_discord
                pstmt.setString(5, member.getUserDiscordId());            // Parameter 5: user_discord_id
                pstmt.setString(6, member.getUserDiscordMentionId());     // Parameter 6: user_discord_mention_id
                pstmt.setInt(7, member.getCrimeExpRank());                // Parameter 7: crime_exp_rank
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.info("Inserted {} members into {} with faction_id {}", results.length, tableName, factionId);
        }
    }
}