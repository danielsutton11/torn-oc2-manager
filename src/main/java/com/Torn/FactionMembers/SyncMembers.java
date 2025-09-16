package com.Torn.FactionMembers;

import com.Torn.Execute; // Import Execute class instead of static import
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class SyncMembers {

    /**
     ├── SyncMembers.java
     │   └── Orchestrates the member synchronization between Torn and Discord
     */

    private static final Logger logger = LoggerFactory.getLogger(SyncMembers.class);

    public static void syncFactionMembers() throws SQLException, IOException {

        logger.info("Starting faction members and Discord sync process");

        // Environment variable checks
        logger.info("Checking environment variables...");
        logger.info("DISCORD_BOT_TOKEN present: {}", System.getenv(Constants.DISCORD_BOT_TOKEN) != null);
        logger.info("DISCORD_GUILD_ID present: {}", System.getenv(Constants.DISCORD_GUILD_ID) != null);
        logger.info("DATABASE_URL_CONFIG present: {}", System.getenv(Constants.DATABASE_URL_CONFIG) != null);

        String databaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        logger.info("Creating database connection...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");
            logger.info("Calling GetFactionMembers...");

            GetFactionMembers.fetchAndProcessAllFactionMembers(connection);

            logger.info("GetFactionMembers completed successfully");

        } catch (Exception e) {
            logger.error("Error in syncFactionMembers", e);
            throw e;
        }
    }
}