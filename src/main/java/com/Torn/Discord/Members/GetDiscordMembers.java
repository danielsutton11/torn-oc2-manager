package com.Torn.Discord.Members;

import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetDiscordMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetDiscordMembers.class);
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int RATE_LIMIT_DELAY_MS = 1000; // 1 second between requests

    public static class DiscordMember {
        private String userId;
        private String username;
        private String discordId;

        public DiscordMember(String userId, String username) {
            this.userId = userId;
            this.username = username;
            this.discordId = userId; // Store raw Discord ID
        }

        // Getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getDiscordId() { return discordId; }
        public void setDiscordId(String discordId) { this.discordId = discordId; }

        // Helper method to get mention format
        public String getMention() {
            return "<@" + discordId + ">";
        }

        @Override
        public String toString() {
            return "DiscordMember{" +
                    "userId='" + userId + '\'' +
                    ", username='" + username + '\'' +
                    ", discordId='" + discordId + '\'' +
                    '}';
        }
    }

    public static List<DiscordMember> fetchDiscordMembers() throws IOException {
        // Validate environment variables
        String botToken = System.getenv(Constants.DISCORD_BOT_TOKEN);
        if (botToken == null || botToken.isEmpty()) {
            throw new IllegalStateException("DISCORD_BOT_TOKEN environment variable not set");
        }

        String guildId = System.getenv(Constants.DISCORD_GUILD_ID);
        if (guildId == null || guildId.isEmpty()) {
            throw new IllegalStateException("DISCORD_GUILD_ID environment variable not set");
        }

        List<DiscordMember> discordMembers = new ArrayList<>();
        String after = null;
        boolean hasMore = true;
        int requestCount = 0;

        logger.info("Starting to fetch Discord members for guild: {}", guildId);

        while (hasMore) {
            String url = buildDiscordApiUrl(guildId, after);
            requestCount++;

            Request request = new Request.Builder()
                    .url(url)
                    .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_DISCORD_AUTHORIZATION_VALUE + botToken)
                    .addHeader(Constants.HEADER_CONTENT_TYPE, Constants.HEADER_CONTENT_TYPE_VALUE)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    logger.error("Discord API request failed with code: {} for URL: {}", response.code(), url);

                    // Handle rate limiting specifically
                    if (response.code() == 429) {
                        logger.warn("Rate limited by Discord API. Consider increasing delays between requests.");
                    }

                    throw new IOException("Discord API request failed: " + response.code());
                }

                if (response.body() == null) {
                    throw new IOException("Discord API response body is null");
                }

                String responseBody = response.body().string();
                JsonNode jsonResponse = objectMapper.readTree(responseBody);

                // Process members from response
                int membersProcessed = processMembers(jsonResponse, discordMembers);
                logger.debug("Processed {} members in request #{}", membersProcessed, requestCount);

                // Check for pagination
                after = getNextPageToken(jsonResponse);
                hasMore = (after != null);

                // Rate limiting - wait between requests
                if (hasMore) {
                    try {
                        Thread.sleep(RATE_LIMIT_DELAY_MS);
                    } catch (InterruptedException e) {
                        logger.warn("Thread interrupted while waiting for rate limit");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        logger.info("Successfully fetched {} Discord members in {} requests", discordMembers.size(), requestCount);
        return discordMembers;
    }

    private static int processMembers(JsonNode jsonResponse, List<DiscordMember> discordMembers) {
        JsonNode members = jsonResponse.get(Constants.MEMBERS);
        if (members == null || !members.isArray()) {
            logger.warn("No members array found in Discord API response");
            return 0;
        }

        int processed = 0;
        for (JsonNode member : members) {
            JsonNode user = member.get(Constants.USER);
            if (user == null) {
                logger.debug("Skipping member with null user object");
                continue;
            }

            JsonNode userIdNode = user.get(Constants.ID);
            JsonNode usernameNode = user.get(Constants.USERNAME);

            if (userIdNode == null || usernameNode == null) {
                logger.warn("Skipping member with missing ID or username");
                continue;
            }

            String userId = userIdNode.asText();
            String username = usernameNode.asText();

            // Check if user has nickname (display name override)
            JsonNode nickNode = member.get(Constants.NICKNAME);
            String displayName = (nickNode != null && !nickNode.isNull()) ? nickNode.asText() : username;

            DiscordMember discordMember = new DiscordMember(userId, displayName);
            discordMembers.add(discordMember);
            processed++;

            logger.debug("Added Discord member: {} (ID: {})", displayName, userId);
        }

        return processed;
    }

    private static String getNextPageToken(JsonNode jsonResponse) {
        JsonNode members = jsonResponse.get(Constants.MEMBERS);
        if (members != null && members.isArray() && members.size() >= 1000) {
            // Get the last member's user ID for pagination
            JsonNode lastMember = members.get(members.size() - 1);
            if (lastMember != null && lastMember.has(Constants.USER)) {
                JsonNode user = lastMember.get(Constants.USER);
                if (user != null && user.has(Constants.ID)) {
                    return user.get(Constants.ID).asText();
                }
            }
        }
        return null; // No more pages
    }

    private static String buildDiscordApiUrl(String guildId, String after) {
        String baseUrl = Constants.API_URL_DISCORD_BASE_URL + guildId + Constants.API_URL_DISCORD_MEMBERS;

        if (after != null && !after.isEmpty()) {
            baseUrl += Constants.AFTER_JOIN + after;
        }

        return baseUrl;
    }
}