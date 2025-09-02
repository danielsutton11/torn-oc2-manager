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

public class GetDiscordMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetDiscordMembers.class);
    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class DiscordMember {
        private String userId;
        private String username;
        private String discordId; // This will be formatted as <@&{discord_id}>

        public DiscordMember(String userId, String username, String discordId) {
            this.userId = userId;
            this.username = username;
            this.discordId = "<@&" + discordId + ">";
        }

        // Getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getDiscordId() { return discordId; }
        public void setDiscordId(String discordId) { this.discordId = discordId; }

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
        List<DiscordMember> discordMembers = new ArrayList<>();
        String after = null;
        boolean hasMore = true;

        while (hasMore) {
            String url = buildDiscordApiUrl(after);

            Request request = new Request.Builder()
                    .url(url)
                    .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_DISCORD_AUTHORIZATION_VALUE + System.getenv(Constants.DISCORD_BOT_TOKEN))
                    .addHeader(Constants.HEADER_CONTENT_TYPE, Constants.HEADER_CONTENT_TYPE_VALUE)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    logger.error("Discord API request failed with code: {}", response.code());
                    throw new IOException("Discord API request failed: " + response.code());
                }

                assert response.body() != null;
                String responseBody = response.body().string();
                JsonNode jsonResponse = objectMapper.readTree(responseBody);

                // Process members from response
                JsonNode members = jsonResponse.get(Constants.MEMBERS);
                if (members != null && members.isArray()) {
                    for (JsonNode member : members) {
                        JsonNode user = member.get(Constants.USER);
                        if (user != null) {
                            String userId = user.get(Constants.ID).asText();
                            String username = user.get(Constants.USERNAME).asText();

                            // Check if user has nickname
                            JsonNode nick = member.get(Constants.NICKNAME);
                            String displayName = nick != null ? nick.asText() : username;

                            DiscordMember discordMember = new DiscordMember(userId, displayName, userId);
                            discordMembers.add(discordMember);

                            logger.debug("Added Discord member: {} ({})", displayName, userId);
                        }
                    }
                }

                // Check for pagination
                JsonNode metaNode = jsonResponse.get(Constants.META);
                if (metaNode != null) {
                    JsonNode afterNode = metaNode.get(Constants.AFTER);
                    if (afterNode != null && !afterNode.isNull()) {
                        after = afterNode.asText();
                    } else {
                        hasMore = false;
                    }
                } else {
                    hasMore = false;
                }
            }
        }

        logger.info("Fetched {} Discord members", discordMembers.size());
        return discordMembers;
    }

    private static String buildDiscordApiUrl(String after) {
        String guildId = System.getenv(Constants.DISCORD_GUILD_ID);
        String baseUrl = Constants.API_URL_DISCORD_BASE_URL + guildId + Constants.API_URL_DISCORD_MEMBERS;

        if (after != null) {
            baseUrl += Constants.AFTER_JOIN + after;
        }

        return baseUrl;
    }
}
