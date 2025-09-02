package com.Torn.Discord.Members;

import com.Torn.Helpers.Constants;
import net.dv8tion.jda.api.*;
import net.dv8tion.jda.api.entities.*;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.utils.MemberCachePolicy;
import net.dv8tion.jda.api.utils.ChunkingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDiscordMembers {

    private static final Logger logger = LoggerFactory.getLogger(GetDiscordMembers.class);

    public static class DiscordMember {
        private String userId;
        private String username;
        private String discordId;

        public DiscordMember(String userId, String username, String discordId) {
            this.userId = userId;
            this.username = username;
            this.discordId = discordId; // Store raw Discord ID
        }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getDiscordId() { return discordId; }
        public void setDiscordId(String discordId) { this.discordId = discordId; }

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

    public static CompletableFuture<List<DiscordMember>> fetchDiscordMembers() {
        CompletableFuture<List<DiscordMember>> future = new CompletableFuture<>();
        try {

            String botToken = System.getenv(Constants.DISCORD_BOT_TOKEN);
            if (botToken == null || botToken.isEmpty()) {
                logger.error("DISCORD_BOT_TOKEN not set");
                return null;
            }

            String guildId = System.getenv(Constants.DISCORD_GUILD_ID);
            if (guildId == null || guildId.isEmpty()) {
                logger.error("DISCORD_GUILD_ID not set");
                return null;
            }

            JDA jda = JDABuilder.createDefault(botToken)
                    .enableIntents(GatewayIntent.GUILD_MEMBERS)
                    .setChunkingFilter(ChunkingFilter.ALL)
                    .setMemberCachePolicy(MemberCachePolicy.ALL)
                    .build();

            jda.awaitReady();

            Guild guild = jda.getGuildById(guildId);
            if (guild == null) {
                logger.error("Discord Server not found for ID: {}", guildId);
                jda.shutdown();
                future.completeExceptionally(new IllegalStateException("Discord Server not found"));
                return future;
            }

            guild.loadMembers()
                    .onSuccess(members -> {
                        List<DiscordMember> discordMembers = new ArrayList<>();

                        for (Member member : members) {
                            try {
                                String fullName = member.getNickname();
                                if (fullName == null) {
                                    User user = member.getUser();
                                    user.getName();
                                    fullName = user.getName();
                                }

                                String extractedUsername = extractBeforeBracket(fullName);
                                String extractedCustomId = extractBetweenBrackets(fullName);

                                discordMembers.add(new DiscordMember(extractedCustomId, extractedUsername, member.getId()));
                            } catch (Exception e) {
                                logger.warn("Error processing member: {}", member.getId(), e);
                            }
                        }

                        logger.info("Fetched and processed {} members from guild '{}'", discordMembers.size(), guild.getName());
                        future.complete(discordMembers);
                        jda.shutdown();
                    })
                    .onError(error -> {
                        logger.error("Failed to load members", error);
                        future.completeExceptionally(error);
                        jda.shutdown();
                    });

        } catch (Exception e) {
            logger.error("Unexpected error", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    // Utility: extract value before first [
    private static String extractBeforeBracket(String value) {
        int index = value.indexOf("[");
        return index == -1 ? value.trim() : value.substring(0, index).trim();
    }

    // Utility: extract value between [brackets]
    private static String extractBetweenBrackets(String value) {
        Pattern pattern = Pattern.compile("\\[(.*?)\\]");
        Matcher matcher = pattern.matcher(value);
        return matcher.find() ? matcher.group(1) : value;
    }
}
