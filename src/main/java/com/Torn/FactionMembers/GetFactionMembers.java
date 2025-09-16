package com.Torn.FactionMembers;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Discord.Members.GetDiscordMembers;
import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.Torn.Helpers.FactionInfo;
import com.Torn.Postgres.Postgres;
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

    // HTTP client for TornStats API calls
    private static final OkHttpClient tornStatsClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

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

    public static void fetchAndProcessAllFactionMembers(Connection connection) {
        logger.info("Starting to fetch faction members for all factions");

        //Get Discord members
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

        // Get all faction information
        List<FactionInfo> factions = Execute.factionInfo;
        if (factions.isEmpty()) {
            logger.warn("No active factions found to process");
            return;
        }

        //Process each faction
        int processedCount = 0;
        int successfulCount = 0;
        int failedCount = 0;

        for (FactionInfo factionInfo : factions) {
            try {
                logger.info("Processing faction: {} ({}/{})",
                        factionInfo.getFactionId(), processedCount + 1, factions.size());

                // Fetch faction members
                List<FactionMember> factionMembers = fetchFactionMembers(factionInfo);

                // Join with Discord data
                joinWithDiscordData(factionMembers, discordMemberMap);

                // Fetch crime experience ranks from TornStats
                fetchCrimeExpRanks(factionMembers, factionInfo);

                // Write to database
                writeFactionMembersToDatabase(connection, factionInfo, factionMembers);

                logger.info("Successfully processed {} members for faction {}",
                        factionMembers.size(), factionInfo.getFactionId());
                successfulCount++;

                processedCount++;

                // Rate limiting between factions (except for the last one)
                if (processedCount < factions.size()) {
                    logger.debug("Waiting {}ms before processing next faction", Constants.TORN_API_RATE_LIMIT_MS);
                    Thread.sleep(Constants.TORN_API_RATE_LIMIT_MS);
                }

            } catch (InterruptedException e) {
                logger.warn("Faction processing interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing faction {}: {}", factionInfo.getFactionId(), e.getMessage(), e);
                failedCount++;
            }
        }

        logger.info("Faction processing summary: {} processed, {} successful, {} failed",
                processedCount, successfulCount, failedCount);

        // If more than half failed, this might indicate a systemic issue
        if (failedCount > successfulCount && processedCount > 2) {
            logger.error("More than half of factions failed - Torn API may be experiencing issues");
        }
    }


    private static Map<String, GetDiscordMembers.DiscordMember> createDiscordMemberMap(List<GetDiscordMembers.DiscordMember> discordMembers) {
        Map<String, GetDiscordMembers.DiscordMember> map = new HashMap<>();
        for (GetDiscordMembers.DiscordMember member : discordMembers) {
            map.put(member.getUsername().toLowerCase().trim(), member);
        }
        logger.debug("Created Discord member lookup map with {} entries", map.size());
        return map;
    }

    private static List<FactionMember> fetchFactionMembers(FactionInfo factionInfo) throws Exception {
        List<String> apiKeys = factionInfo.getApiKeys();
        String url = Constants.API_URL_FACTION + "/" + factionInfo.getFactionId() + Constants.API_URL_FACTION_MEMBERS;

        logger.debug("Fetching faction members for faction: {} with {} API key(s)",
                factionInfo.getFactionId(), apiKeys.size());

        Exception lastException = null;

        // Try each API key until one succeeds
        for (int i = 0; i < apiKeys.size(); i++) {
            String apiKey = apiKeys.get(i);
            logger.debug("Trying API key {} of {} for faction {}", i + 1, apiKeys.size(), factionInfo.getFactionId());

            try {
                ApiResponse response = TornApiHandler.executeRequest(url, apiKey);

                // Handle different response types
                if (response.isSuccess()) {
                    logger.info("Successfully fetched faction members for faction {} using API key {} of {}",
                            factionInfo.getFactionId(), i + 1, apiKeys.size());
                    return parseFactionMembers(response.getBody(), factionInfo);

                } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
                    logger.error("Circuit breaker is open - skipping faction {} to prevent further API failures",
                            factionInfo.getFactionId());
                    throw new IOException("Circuit breaker open - API calls suspended");

                } else if (response.isAuthenticationIssue()) {
                    logger.warn("API key {} of {} authentication failed for faction {}: {} - trying next key",
                            i + 1, apiKeys.size(), factionInfo.getFactionId(), response.getErrorMessage());
                    lastException = new IOException("API key authentication failed: " + response.getErrorMessage());

                } else if (response.isTemporaryError()) {
                    logger.warn("Temporary API error with key {} of {} for faction {}: {} - trying next key",
                            i + 1, apiKeys.size(), factionInfo.getFactionId(), response.getErrorMessage());
                    lastException = new IOException("Temporary API error: " + response.getErrorMessage());

                } else {
                    logger.warn("Permanent API error with key {} of {} for faction {}: {} - trying next key",
                            i + 1, apiKeys.size(), factionInfo.getFactionId(), response.getErrorMessage());
                    lastException = new IOException("API error: " + response.getErrorMessage());
                }

            } catch (Exception e) {
                logger.warn("Exception with API key {} of {} for faction {}: {} - trying next key",
                        i + 1, apiKeys.size(), factionInfo.getFactionId(), e.getMessage());
                lastException = e instanceof IOException ? (IOException) e : new IOException(e.getMessage(), e);
            }
        }

        // All API keys failed
        logger.error("All {} API key(s) failed for faction {}", apiKeys.size(), factionInfo.getFactionId());

        if (lastException != null) {
            throw lastException;
        } else {
            throw new IOException("All API keys failed for faction " + factionInfo.getFactionId());
        }
    }


    private static List<FactionMember> parseFactionMembers(String responseBody, FactionInfo factionInfo) throws IOException {
        List<FactionMember> members = new ArrayList<>();

        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            if (jsonResponse.has(Constants.RESPONSE_ERROR)) {
                String errorMsg = jsonResponse.get(Constants.RESPONSE_ERROR).asText();
                logger.error("Torn API Error for faction {}: {}", factionInfo.getFactionId(), errorMsg);
                throw new IOException("Torn API Error: " + errorMsg);
            }

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


    /**
     * Fetch crime experience ranks from TornStats API with API key fallback
     */
    private static void fetchCrimeExpRanks(List<FactionMember> factionMembers, FactionInfo factionInfo) {
        List<String> apiKeys = factionInfo.getApiKeys();

        if (apiKeys.isEmpty()) {
            logger.warn("No TornStats API keys configured for faction {} - using default crime_exp_rank of 100",
                    factionInfo.getFactionId());
            return;
        }

        logger.info("Fetching crime experience ranks from TornStats for faction {} with {} API key(s)",
                factionInfo.getFactionId(), apiKeys.size());

        Exception lastException = null;

        // Try each API key until one succeeds
        for (int i = 0; i < apiKeys.size(); i++) {
            String apiKey = apiKeys.get(i).trim();

            if (apiKey.isEmpty()) {
                logger.warn("Empty API key {} of {} for faction {} - skipping",
                        i + 1, apiKeys.size(), factionInfo.getFactionId());
                continue;
            }

            logger.debug("Trying TornStats API key {} of {} for faction {}",
                    i + 1, apiKeys.size(), factionInfo.getFactionId());

            try {
                String url = Constants.API_URL_TORN_STATS + apiKey + Constants.API_URL_TORN_STATS_FACTION_CRIMES;

                Request request = new Request.Builder()
                        .url(url)
                        .addHeader("User-Agent", "TornBot/1.0")
                        .build();

                try (Response response = tornStatsClient.newCall(request).execute()) {
                    if (response.isSuccessful() && response.body() != null) {
                        String responseBody = response.body().string();
                        Map<String, Integer> crimeExpRanks = parseTornStatsCrimeRanks(responseBody);

                        // If we got valid data
                        if (!crimeExpRanks.isEmpty() || isValidTornStatsResponse(responseBody)) {

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

                            logger.info("Successfully fetched crime exp ranks using API key {} of {} - matched {} of {} faction members",
                                    i + 1, apiKeys.size(), matchedCount, factionMembers.size());

                            // Rate limiting for TornStats API
                            Thread.sleep(Constants.TORN_STATS_API_RATE_LIMIT_MS);
                            return;

                        } else {
                            logger.warn("TornStats API key {} of {} returned empty/invalid data for faction {} - trying next key",
                                    i + 1, apiKeys.size(), factionInfo.getFactionId());
                            lastException = new IOException("Empty or invalid TornStats response");

                        }

                    } else {
                        String errorMsg = String.format("HTTP %d: %s", response.code(), response.message());
                        logger.warn("TornStats API key {} of {} failed for faction {} - {} - trying next key",
                                i + 1, apiKeys.size(), factionInfo.getFactionId(), errorMsg);

                        // Check if it's an authentication error (401/403) or other error
                        if (response.code() == 401 || response.code() == 403) {
                            lastException = new IOException("Authentication failed: " + errorMsg);
                        } else if (response.code() >= 500) {
                            lastException = new IOException("Server error: " + errorMsg);
                        } else {
                            lastException = new IOException("API error: " + errorMsg);
                        }
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("TornStats API request interrupted for faction {}", factionInfo.getFactionId());
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                logger.warn("Exception with TornStats API key {} of {} for faction {}: {} - trying next key",
                        i + 1, apiKeys.size(), factionInfo.getFactionId(), e.getMessage());
                lastException = e;

            }
        }

        // All API keys failed
        logger.error("All {} TornStats API key(s) failed for faction {} - using default crime_exp_rank of 100 for all members. Last error: {}",
                apiKeys.size(), factionInfo.getFactionId(),
                lastException != null ? lastException.getMessage() : "Unknown error");

    }

    /**
     * Helper method to check if TornStats response is valid (even if empty)
     */
    private static boolean isValidTornStatsResponse(String responseBody) {
        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            // Check for explicit API error
            if (jsonResponse.has(Constants.RESPONSE_STATUS) && !jsonResponse.get(Constants.RESPONSE_STATUS).asBoolean()) {
                return false;
            }

            // If we have a members node (even if empty), it's a valid response
            return jsonResponse.has(Constants.NODE_MEMBERS);

        } catch (Exception e) {
            return false;
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
            if (jsonResponse.has(Constants.RESPONSE_STATUS) && !jsonResponse.get(Constants.RESPONSE_STATUS).asBoolean()) {
                String message = jsonResponse.has(Constants.RESPONSE_MESSAGE) ? jsonResponse.get(Constants.RESPONSE_MESSAGE).asText() : "Unknown error";
                logger.warn("TornStats API error: {}", message);
                return crimeExpRanks;
            }

            // Parse members data
            JsonNode membersNode = jsonResponse.get(Constants.NODE_MEMBERS);
            if (membersNode != null && membersNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = membersNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String userId = entry.getKey();
                    JsonNode memberData = entry.getValue();

                    if (memberData.has(Constants.NODE_CRIME_EXP_RANK)) {
                        int crimeExpRank = memberData.get(Constants.NODE_CRIME_EXP_RANK).asInt(100);
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


    /**
     * Push information to database
     */
    private static void writeFactionMembersToDatabase(Connection connection, FactionInfo factionInfo,
                                                      List<FactionMember> members) throws SQLException {

        String tableName = Constants.TABLE_NAME_FACTION_MEMBERS + factionInfo.getDbSuffix();

        logger.debug("Writing {} members to table: {}", members.size(), tableName);

        createTableIfNotExists(connection, tableName);

        // Clear existing data and insert new data in a transaction
        connection.setAutoCommit(false);
        try {
            Postgres.clearExistingData(connection, tableName);
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