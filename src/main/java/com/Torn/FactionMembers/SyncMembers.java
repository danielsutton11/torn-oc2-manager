package com.Torn.FactionMembers;

import com.Torn.ApiKeys.ValidateApiKeys;
import com.Torn.Helpers.Constants;
import com.Torn.Postgres.Postgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class SyncMembers {

    private static final Logger logger = LoggerFactory.getLogger(SyncMembers.class);

    public static void syncFactionMembers() {
        logger.info("Starting faction members and Discord sync process");

        try {
            String databaseUrl = System.getenv(Constants.DATABASE_URL);
            if (databaseUrl == null) {
                logger.error("No database URL found!");
                return;
            }

            // Validate Discord environment variables
            String discordBotToken = System.getenv(Constants.DISCORD_BOT_TOKEN);
            String discordGuildId = System.getenv(Constants.DISCORD_GUILD_ID);

            if (discordBotToken == null || discordGuildId == null) {
                logger.error("Discord bot token or guild ID not found in environment variables!");
                return;
            }

            try (Connection connection = new Postgres().connect(databaseUrl, logger)) {
                if (connection == null) {
                    logger.error("Failed to connect to database!");
                    return;
                }

                // Execute the main sync process
                GetFactionMembers.fetchAndProcessAllFactionMembers(connection);

                logger.info("Faction members and Discord sync completed successfully");

            } catch (SQLException e) {
                logger.error("Database error during sync process", e);
            } catch (IOException e) {
                logger.error("IO error during sync process (likely API or Discord related)", e);
            }

        } catch (Exception e) {
            logger.error("Unexpected error during faction Discord sync", e);
        }
    }
}
