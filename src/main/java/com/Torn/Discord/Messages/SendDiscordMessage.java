package com.Torn.Discord.Messages;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//TODO: To be tested.

/**
 * Enhanced Discord webhook message sender with database-driven role and webhook support
 */
public class SendDiscordMessage {

    private static final Logger logger = LoggerFactory.getLogger(SendDiscordMessage.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // HTTP client specifically configured for Discord webhooks
    private static final OkHttpClient discordClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    /**
     * Discord configuration loaded from database
     */
    public static class DiscordConfig {
        private final String factionId;
        private final String leadershipRoleId;
        private final String ocManagerRoleId;
        private final String bankerRoleId;
        private final String armourerRoleId;
        private final String webhookUrl;

        public DiscordConfig(String factionId, String leadershipRoleId, String ocManagerRoleId,
                             String bankerRoleId, String armourerRoleId, String webhookUrl) {
            this.factionId = factionId;
            this.leadershipRoleId = leadershipRoleId;
            this.ocManagerRoleId = ocManagerRoleId;
            this.bankerRoleId = bankerRoleId;
            this.armourerRoleId = armourerRoleId;
            this.webhookUrl = webhookUrl;
        }

        public String getFactionId() { return factionId; }
        public String getLeadershipRoleId() { return leadershipRoleId; }
        public String getOcManagerRoleId() { return ocManagerRoleId; }
        public String getBankerRoleId() { return bankerRoleId; }
        public String getArmourerRoleId() { return armourerRoleId; }
        public String getWebhookUrl() { return webhookUrl; }

        // Helper methods to get role mentions
        public String getLeadershipMention() {
            return leadershipRoleId != null ? "<@&" + leadershipRoleId + ">" : null;
        }

        public String getOcManagerMention() {
            return ocManagerRoleId != null ? "<@&" + ocManagerRoleId + ">" : null;
        }

        public String getBankerMention() {
            return bankerRoleId != null ? "<@&" + bankerRoleId + ">" : null;
        }

        public String getArmourerMention() {
            return armourerRoleId != null ? "<@&" + armourerRoleId + ">" : null;
        }
    }

    /**
     * Role types for easy reference
     */
    public enum RoleType {
        LEADERSHIP,
        OC_MANAGER,
        BANKER,
        ARMOURER
    }

    /**
     * Send a message to faction leadership
     */
    public static boolean sendToLeadership(String factionId, String message) {
        return sendToRole(factionId, RoleType.LEADERSHIP, message, null, null);
    }

    /**
     * Send a message to OC managers
     */
    public static boolean sendToOCManagers(String factionId, String message) {
        return sendToRole(factionId, RoleType.OC_MANAGER, message, null, null);
    }

    /**
     * Send a message to bankers
     */
    public static boolean sendToBankers(String factionId, String message) {
        return sendToRole(factionId, RoleType.BANKER, message, null, null);
    }

    /**
     * Send a message to armourer
     */
    public static boolean sendToArmourer(String factionId, String message) {
        return sendToRole(factionId, RoleType.ARMOURER, message, null, null);
    }


    /**
     * Send a message with embed to specific role
     */
    public static boolean sendToRole(String factionId, RoleType roleType, String message,
                                     DiscordEmbed embed, String customUsername) {
        if (factionId == null || factionId.trim().isEmpty()) {
            logger.error("Faction ID is required for Discord messaging");
            return false;
        }

        try {
            // Load Discord configuration from database
            DiscordConfig config = loadDiscordConfig(factionId);
            if (config == null) {
                logger.error("No Discord configuration found for faction {}", factionId);
                return false;
            }

            if (config.getWebhookUrl() == null || config.getWebhookUrl().trim().isEmpty()) {
                logger.error("No webhook URL configured for faction {}", factionId);
                return false;
            }

            // Get role mention based on type
            String roleMention = getRoleMention(config, roleType);

            // Prepare the message content
            String finalMessage = message;
            if (roleMention != null) {
                finalMessage = roleMention + " " + (message != null ? message : "");
            }

            // Create Discord payload
            Map<String, Object> payload = new HashMap<>();

            if (finalMessage != null && !finalMessage.trim().isEmpty()) {
                payload.put("content", finalMessage.trim());
            }

            if (embed != null) {
                payload.put("embeds", new Object[]{embed.toMap()});
            }

            if (customUsername != null && !customUsername.trim().isEmpty()) {
                payload.put("username", customUsername.trim());
            } else {
                payload.put("username", "TornBot OC Manager");
            }

            return sendDiscordWebhook(payload, config.getWebhookUrl());

        } catch (Exception e) {
            logger.error("Error sending Discord message to {} for faction {}: {}",
                    roleType, factionId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Send a message to multiple roles
     */
    public static boolean sendToMultipleRoles(String factionId, RoleType[] roleTypes, String message,
                                              DiscordEmbed embed, String customUsername) {
        if (factionId == null || factionId.trim().isEmpty()) {
            logger.error("Faction ID is required for Discord messaging");
            return false;
        }

        if (roleTypes == null || roleTypes.length == 0) {
            logger.error("At least one role type is required");
            return false;
        }

        try {
            // Load Discord configuration from database
            DiscordConfig config = loadDiscordConfig(factionId);
            if (config == null) {
                logger.error("No Discord configuration found for faction {}", factionId);
                return false;
            }

            if (config.getWebhookUrl() == null || config.getWebhookUrl().trim().isEmpty()) {
                logger.error("No webhook URL configured for faction {}", factionId);
                return false;
            }

            // Build combined role mentions
            StringBuilder roleMentions = new StringBuilder();
            for (RoleType roleType : roleTypes) {
                String roleMention = getRoleMention(config, roleType);
                if (roleMention != null) {
                    if (roleMentions.length() > 0) {
                        roleMentions.append(" ");
                    }
                    roleMentions.append(roleMention);
                }
            }

            // Prepare the message content
            String finalMessage = message;
            if (roleMentions.length() > 0) {
                finalMessage = roleMentions.toString() + " " + (message != null ? message : "");
            }

            // Create Discord payload
            Map<String, Object> payload = new HashMap<>();

            if (finalMessage != null && !finalMessage.trim().isEmpty()) {
                payload.put("content", finalMessage.trim());
            }

            if (embed != null) {
                payload.put("embeds", new Object[]{embed.toMap()});
            }

            if (customUsername != null && !customUsername.trim().isEmpty()) {
                payload.put("username", customUsername.trim());
            } else {
                payload.put("username", "TornBot OC Manager");
            }

            return sendDiscordWebhook(payload, config.getWebhookUrl());

        } catch (Exception e) {
            logger.error("Error sending Discord message to multiple roles for faction {}: {}",
                    factionId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Load Discord configuration from database for a specific faction
     */
    private static DiscordConfig loadDiscordConfig(String factionId) throws SQLException {
        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        String sql = "SELECT faction_id, leadership_role_id, oc_manager_role_id, banker_role_id, armourer_role_id, oc_webhook_url " +
                "FROM " + Constants.TABLE_NAME_DISCORD_ROLES_WEBHOOKS + " " +
                "WHERE faction_id = ?";

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger);
             PreparedStatement pstmt = connection.prepareStatement(sql)) {

            try {
                long factionIdLong = Long.parseLong(factionId);
                pstmt.setLong(1, factionIdLong);
            } catch (NumberFormatException e) {
                pstmt.setString(1, factionId);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String dbFactionId = rs.getString("faction_id");
                    String leadershipRoleId = rs.getString("leadership_role_id");
                    String ocManagerRoleId = rs.getString("oc_manager_role_id");
                    String bankerRoleId = rs.getString("banker_role_id");
                    String armourerRoleId = rs.getString("armourer_role_id");
                    String webhookUrl = rs.getString("oc_webhook_url");

                    return new DiscordConfig(dbFactionId, leadershipRoleId, ocManagerRoleId, bankerRoleId, armourerRoleId, webhookUrl);
                } else {
                    logger.warn("No Discord configuration found for faction {}", factionId);
                    return null;
                }
            }
        }
    }

    /**
     * Get role mention string for a specific role type
     */
    private static String getRoleMention(DiscordConfig config, RoleType roleType) {
        switch (roleType) {
            case LEADERSHIP:
                return config.getLeadershipMention();
            case OC_MANAGER:
                return config.getOcManagerMention();
            case BANKER:
                return config.getBankerMention();
            case ARMOURER:
                return config.getArmourerMention();
            default:
                return null;
        }
    }

    // Original methods for backward compatibility (without database lookup)

    /**
     * Send a simple text message to Discord webhook (direct URL)
     */
    public static boolean sendMessage(String message, String webhookUrl) {
        if (message == null || message.trim().isEmpty()) {
            logger.warn("Cannot send empty message to Discord");
            return false;
        }

        if (webhookUrl == null || webhookUrl.trim().isEmpty()) {
            logger.error("Discord webhook URL is required");
            return false;
        }

        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("content", message);
            return sendDiscordWebhook(payload, webhookUrl);
        } catch (Exception e) {
            logger.error("Error sending Discord message: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Core method to send webhook payload to Discord
     */
    private static boolean sendDiscordWebhook(Map<String, Object> payload, String webhookUrl) {
        try {
            // Convert payload to JSON
            String jsonPayload = objectMapper.writeValueAsString(payload);

            logger.debug("Sending Discord webhook to: {}", maskWebhookUrl(webhookUrl));
            logger.debug("Payload: {}", jsonPayload);

            // Create HTTP request
            RequestBody body = RequestBody.create(
                    jsonPayload,
                    MediaType.get("application/json; charset=utf-8")
            );

            Request request = new Request.Builder()
                    .url(webhookUrl)
                    .post(body)
                    .addHeader("Content-Type", "application/json")
                    .addHeader("User-Agent", "TornBot/1.0")
                    .build();

            // Execute request
            try (Response response = discordClient.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    logger.debug("✓ Discord message sent successfully (HTTP {})", response.code());
                    return true;
                } else {
                    logger.error("✗ Discord webhook failed: HTTP {} - {}",
                            response.code(), response.message());

                    // Log response body for debugging if available
                    if (response.body() != null) {
                        try {
                            String errorBody = response.body().string();
                            logger.error("Discord error response: {}", errorBody);
                        } catch (Exception e) {
                            logger.debug("Could not read Discord error response body");
                        }
                    }

                    return false;
                }
            }

        } catch (IOException e) {
            logger.error("Network error sending Discord webhook: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error sending Discord webhook: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Mask webhook URL for safe logging
     */
    private static String maskWebhookUrl(String webhookUrl) {
        if (webhookUrl == null) return "null";

        if (webhookUrl.contains("/webhooks/")) {
            String[] parts = webhookUrl.split("/");
            if (parts.length >= 2) {
                parts[parts.length - 1] = "***";
                return String.join("/", parts);
            }
        }

        return webhookUrl.substring(0, Math.min(webhookUrl.length(), 50)) + "***";
    }

    /**
     * Utility class for creating Discord embeds
     */
    public static class DiscordEmbed {
        private String title;
        private String description;
        private String color;
        private String url;
        private String timestamp;
        private Map<String, Object> footer;
        private Map<String, Object> author;
        private Map<String, Object> thumbnail;
        private Object[] fields;

        public DiscordEmbed setTitle(String title) {
            this.title = title;
            return this;
        }

        public DiscordEmbed setDescription(String description) {
            this.description = description;
            return this;
        }

        public DiscordEmbed setColor(String color) {
            this.color = color;
            return this;
        }

        public DiscordEmbed setColor(int colorInt) {
            this.color = String.valueOf(colorInt);
            return this;
        }

        public DiscordEmbed setUrl(String url) {
            this.url = url;
            return this;
        }

        public DiscordEmbed setTimestamp(String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public DiscordEmbed setFooter(String text, String iconUrl) {
            this.footer = new HashMap<>();
            this.footer.put("text", text);
            if (iconUrl != null) {
                this.footer.put("icon_url", iconUrl);
            }
            return this;
        }

        public DiscordEmbed setAuthor(String name, String url, String iconUrl) {
            this.author = new HashMap<>();
            this.author.put("name", name);
            if (url != null) {
                this.author.put("url", url);
            }
            if (iconUrl != null) {
                this.author.put("icon_url", iconUrl);
            }
            return this;
        }

        public DiscordEmbed setThumbnail(String url) {
            this.thumbnail = new HashMap<>();
            this.thumbnail.put("url", url);
            return this;
        }

        public DiscordEmbed addField(String name, String value, boolean inline) {
            Map<String, Object> field = new HashMap<>();
            field.put("name", name);
            field.put("value", value);
            field.put("inline", inline);

            if (this.fields == null) {
                this.fields = new Object[]{field};
            } else {
                Object[] newFields = new Object[this.fields.length + 1];
                System.arraycopy(this.fields, 0, newFields, 0, this.fields.length);
                newFields[this.fields.length] = field;
                this.fields = newFields;
            }

            return this;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> embedMap = new HashMap<>();

            if (title != null) embedMap.put("title", title);
            if (description != null) embedMap.put("description", description);
            if (color != null) embedMap.put("color", color);
            if (url != null) embedMap.put("url", url);
            if (timestamp != null) embedMap.put("timestamp", timestamp);
            if (footer != null) embedMap.put("footer", footer);
            if (author != null) embedMap.put("author", author);
            if (thumbnail != null) embedMap.put("thumbnail", thumbnail);
            if (fields != null) embedMap.put("fields", fields);

            return embedMap;
        }
    }

    /**
     * Utility method to create common color values
     */
    public static class Colors {
        public static final int RED = 0xFF0000;
        public static final int GREEN = 0x00FF00;
        public static final int BLUE = 0x0000FF;
        public static final int YELLOW = 0xFFFF00;
        public static final int ORANGE = 0xFFA500;
        public static final int PURPLE = 0x800080;
        public static final int PINK = 0xFFC0CB;
        public static final int CYAN = 0x00FFFF;
        public static final int LIME = 0x32CD32;
        public static final int GOLD = 0xFFD700;
    }
}