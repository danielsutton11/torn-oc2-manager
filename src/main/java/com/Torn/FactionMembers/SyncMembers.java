package com.Torn.FactionMembers;

import com.Torn.ApiKeys.ValidateApiKeys;
import com.Torn.Helpers.Constants;
import com.Torn.Postgres.Postgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.Torn.Execute.postgres;

public class SyncMembers {

    private static final Logger logger = LoggerFactory.getLogger(SyncMembers.class);

    public static void syncFactionMembers() throws SQLException, IOException {
        logger.info("Starting faction members and Discord sync process");

        // Add this debug block
        logger.info("Checking environment variables...");
        logger.info("DISCORD_BOT_TOKEN present: {}", System.getenv("DISCORD_BOT_TOKEN") != null);
        logger.info("DISCORD_GUILD_ID present: {}", System.getenv("DISCORD_GUILD_ID") != null);
        logger.info("DATABASE_URL present: {}", System.getenv("DATABASE_URL") != null);

        String databaseUrl = System.getenv(Constants.DATABASE_URL);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL environment variable not set");
        }

        logger.info("Creating database connection...");
        try (Connection connection = postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            // Test the database connection
            logger.info("Testing database with simple query...");
            try (PreparedStatement pstmt = connection.prepareStatement("SELECT 1")) {
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    logger.info("Database test query successful");
                }
            }

            logger.info("About to call GetFactionMembers...");
            GetFactionMembers.fetchAndProcessAllFactionMembers(connection);
            logger.info("GetFactionMembers completed successfully");

        } catch (Exception e) {
            logger.error("Error in syncFactionMembers", e);
            throw e;
        }
    }
}
