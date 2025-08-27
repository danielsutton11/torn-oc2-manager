package com.Torn.FactionCrimes;

import com.Torn.FactionCrimes.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// SQL imports - be specific
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

// Other imports
import java.time.Instant;
import java.time.LocalDateTime;
import java.io.IOException;
import okhttp3.*;



public class TornCrimesPoller {
    private static final Logger logger = LoggerFactory.getLogger(TornCrimesPoller.class);
    private static final String TORN_API_URL = "https://api.torn.com/v2/faction/crimes?cat=available&offset=0&sort=DESC";
    private static final String API_KEY = System.getenv("TORN_API_KEY");
    private static final int POLLING_INTERVAL_MINUTES = getPollingInterval();

    private static int getPollingInterval() {
        String interval = System.getenv("POLLING_INTERVAL_MINUTES");
        if (interval != null && !interval.isEmpty()) {
            try {
                return Integer.parseInt(interval);
            } catch (NumberFormatException e) {
                logger.warn("Invalid POLLING_INTERVAL_MINUTES value: {}. Using default 5 minutes.", interval);
            }
        }
        return 5; // Default to 5 minutes
    }

    public static void main(String[] args) {

        logger.info("Application starting...");
        logger.info("TORN_API_KEY present: {}", (API_KEY != null && !API_KEY.isEmpty()));
        logger.info("DATABASE_URL present: {}", (System.getenv("DATABASE_URL") != null));
        logger.info("Polling interval: {} minutes", POLLING_INTERVAL_MINUTES);

        System.out.println("=== SO APPLICATION STARTING ===");
        System.out.println("SO API KEY present: " + (API_KEY != null && !API_KEY.isEmpty()));
        System.out.println("SO DATABASE_URL present: " + (System.getenv("DATABASE_URL") != null));

        // Explicitly load PostgreSQL JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
            logger.info("PostgreSQL JDBC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC driver not found in classpath", e);
            System.exit(1);
        }

        if (API_KEY == null || API_KEY.isEmpty()) {
            logger.error("TORN_API_KEY environment variable not set");
            System.exit(1);
        }

        // Check for immediate run request
        if (args.length > 0 && "run-now".equals(args[0])) {
            logger.info("Running immediate data fetch...");
            CrimesPollingJob job = new CrimesPollingJob();
            try {
                job.execute(null);
                logger.info("Immediate fetch completed successfully");
                System.exit(0);
            } catch (JobExecutionException e) {
                logger.error("Immediate fetch failed", e);
                System.exit(1);
            }
        }

        try {
            // Create shared polling job instance
            CrimesPollingJob pollingJobInstance = new CrimesPollingJob();

            // Start HTTP server for manual triggers
            HttpTriggerServer httpServer = new HttpTriggerServer(pollingJobInstance);
            httpServer.start();

            // Create Quartz scheduler
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            // Define job
            JobDetail job = JobBuilder.newJob(CrimesPollingJob.class)
                    .withIdentity("crimesPollingJob", "torn")
                    .build();

            // Define trigger with configurable interval
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("crimesTrigger", "torn")
                    .startNow()
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInMinutes(POLLING_INTERVAL_MINUTES)
                            .repeatForever())
                    .build();

            // Schedule the job
            scheduler.scheduleJob(job, trigger);
            logger.info("Torn crimes polling job scheduled successfully. Polling every {} minutes.", POLLING_INTERVAL_MINUTES);

            // Shutdown hook to clean up resources
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down application...");
                try {
                    scheduler.shutdown();
                    httpServer.stop();
                } catch (Exception e) {
                    logger.error("Error during shutdown", e);
                }
            }));

            // Keep the application running
            while (true) {
                Thread.sleep(60000);
            }

        } catch (Exception e) {
            logger.error("Error starting crimes polling job", e);
            System.exit(1);
        }
    }

    public static class CrimesPollingJob implements Job {
        private static final Logger logger = LoggerFactory.getLogger(CrimesPollingJob.class);
        private static final OkHttpClient client = new OkHttpClient();
        private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            logger.info("Starting Torn crimes polling job at {}", LocalDateTime.now());

            try {
                // Step 1: Fetch data from Torn API
                TornCrimesResponse crimesData = fetchCrimesFromAPI();

                // Step 2: Store data in database
                storeCrimesInDatabase(crimesData);

                logger.info("Crimes polling job completed successfully. Processed {} crimes",
                        crimesData.getCrimes().size());

            } catch (Exception e) {
                logger.error("Error in crimes polling job", e);
                throw new JobExecutionException(e);
            }
        }

        private TornCrimesResponse fetchCrimesFromAPI() throws IOException {
            Request request = new Request.Builder()
                    .url(TORN_API_URL)
                    .addHeader("accept", "application/json")
                    .addHeader("Authorization", "ApiKey " + API_KEY)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected response code: " + response.code() + " - " + response.message());
                }

                String responseBody = response.body().string();
                logger.info("Successfully fetched data from Torn API");

                return objectMapper.readValue(responseBody, TornCrimesResponse.class);
            }
        }

        private void storeCrimesInDatabase(TornCrimesResponse crimesResponse) throws SQLException {
            String databaseUrl = System.getenv("DATABASE_URL");

            if (databaseUrl == null) {
                logger.error("DATABASE_URL environment variable not set");
                return;
            }

            // Fix the connection string format for JDBC
            if (!databaseUrl.startsWith("jdbc:")) {
                databaseUrl = "jdbc:" + databaseUrl;
            }

            logger.info("Attempting to connect to database...");

            try (Connection conn = DriverManager.getConnection(databaseUrl)) {
                logger.info("Database connection successful");
                createTablesIfNotExist(conn);

                for (Crime crime : crimesResponse.getCrimes()) {
                    processCrime(conn, crime);
                }

                logger.info("Successfully stored crimes data in database");
            } catch (SQLException e) {
                logger.error("Database connection failed. URL format: {}", databaseUrl.replaceAll(":[^:/@]+@", ":***@"));
                throw e;
            }
        }

        private void createTablesIfNotExist(Connection conn) throws SQLException {
            // Create crimes table
            String createCrimesTable = "CREATE TABLE IF NOT EXISTS crimes (" +
                    "id BIGINT PRIMARY KEY," +
                    "previous_crime_id BIGINT," +
                    "name VARCHAR(255) NOT NULL," +
                    "difficulty INTEGER," +
                    "status VARCHAR(50)," +
                    "created_at TIMESTAMP," +
                    "planning_at TIMESTAMP," +
                    "executed_at TIMESTAMP," +
                    "ready_at TIMESTAMP," +
                    "expired_at TIMESTAMP," +
                    "rewards TEXT," +
                    "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")";

            // Create slots table
            String createSlotsTable = "CREATE TABLE IF NOT EXISTS crime_slots (" +
                    "id SERIAL PRIMARY KEY," +
                    "crime_id BIGINT NOT NULL," +
                    "position VARCHAR(100)," +
                    "position_id VARCHAR(10)," +
                    "position_number INTEGER," +
                    "checkpoint_pass_rate INTEGER," +
                    "item_requirement_id BIGINT," +
                    "item_is_reusable BOOLEAN," +
                    "item_is_available BOOLEAN," +
                    "user_id BIGINT," +
                    "user_outcome VARCHAR(50)," +
                    "user_joined_at TIMESTAMP," +
                    "user_progress DOUBLE PRECISION," +
                    "user_item_outcome VARCHAR(50)," +
                    "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "FOREIGN KEY (crime_id) REFERENCES crimes(id) ON DELETE CASCADE" +
                    ")";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createCrimesTable);
                stmt.execute(createSlotsTable);

                // Create indexes for better performance
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_crimes_status ON crimes(status)");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_crimes_created_at ON crimes(created_at)");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_slots_crime_id ON crime_slots(crime_id)");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_slots_user_id ON crime_slots(user_id)");

                logger.info("Database tables created or verified");
            }
        }

        private void processCrime(Connection conn, Crime crime) throws SQLException {
            // Insert or update crime
            String upsertCrimeSQL = "INSERT INTO crimes (id, previous_crime_id, name, difficulty, status, created_at, " +
                    "planning_at, executed_at, ready_at, expired_at, rewards, last_updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TEXT, CURRENT_TIMESTAMP) " +
                    "ON CONFLICT (id) DO UPDATE SET " +
                    "status = EXCLUDED.status, " +
                    "planning_at = EXCLUDED.planning_at, " +
                    "executed_at = EXCLUDED.executed_at, " +
                    "ready_at = EXCLUDED.ready_at, " +
                    "last_updated = CURRENT_TIMESTAMP";

            try (PreparedStatement crimeStmt = conn.prepareStatement(upsertCrimeSQL)) {
                crimeStmt.setLong(1, crime.getId());
                crimeStmt.setObject(2, crime.getPreviousCrimeId());
                crimeStmt.setString(3, crime.getName());
                crimeStmt.setObject(4, crime.getDifficulty());
                crimeStmt.setString(5, crime.getStatus());
                crimeStmt.setTimestamp(6, timestampFromEpoch(crime.getCreatedAt()));
                crimeStmt.setTimestamp(7, timestampFromEpoch(crime.getPlanningAt()));
                crimeStmt.setTimestamp(8, timestampFromEpoch(crime.getExecutedAt()));
                crimeStmt.setTimestamp(9, timestampFromEpoch(crime.getReadyAt()));
                crimeStmt.setTimestamp(10, timestampFromEpoch(crime.getExpiredAt()));
                crimeStmt.setString(11, crime.getRewards() != null ? crime.getRewards().toString() : null);

                crimeStmt.executeUpdate();
            }

            // Delete existing slots for this crime and insert new ones
            try (PreparedStatement deleteSlots = conn.prepareStatement("DELETE FROM crime_slots WHERE crime_id = ?")) {
                deleteSlots.setLong(1, crime.getId());
                deleteSlots.executeUpdate();
            }

            // Insert slots
            if (crime.getSlots() != null) {
                String insertSlotSQL = "INSERT INTO crime_slots (crime_id, position, position_id, position_number, " +
                        "checkpoint_pass_rate, item_requirement_id, item_is_reusable, " +
                        "item_is_available, user_id, user_outcome, user_joined_at, " +
                        "user_progress, user_item_outcome, last_updated) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

                try (PreparedStatement slotStmt = conn.prepareStatement(insertSlotSQL)) {
                    for (Slot slot : crime.getSlots()) {
                        slotStmt.setLong(1, crime.getId());
                        slotStmt.setString(2, slot.getPosition());
                        slotStmt.setString(3, slot.getPositionId());
                        slotStmt.setObject(4, slot.getPositionNumber());
                        slotStmt.setObject(5, slot.getCheckpointPassRate());

                        // Item requirement data
                        if (slot.getItemRequirement() != null) {
                            slotStmt.setObject(6, slot.getItemRequirement().getId());
                            slotStmt.setObject(7, slot.getItemRequirement().getIsReusable());
                            slotStmt.setObject(8, slot.getItemRequirement().getIsAvailable());
                        } else {
                            slotStmt.setNull(6, Types.BIGINT);
                            slotStmt.setNull(7, Types.BOOLEAN);
                            slotStmt.setNull(8, Types.BOOLEAN);
                        }

                        // User data
                        if (slot.getUser() != null) {
                            slotStmt.setObject(9, slot.getUser().getId());
                            slotStmt.setString(10, slot.getUser().getOutcome());
                            slotStmt.setTimestamp(11, timestampFromEpoch(slot.getUser().getJoinedAt()));
                            slotStmt.setObject(12, slot.getUser().getProgress());
                            slotStmt.setString(13, slot.getUser().getItemOutcome());
                        } else {
                            slotStmt.setNull(9, Types.BIGINT);
                            slotStmt.setNull(10, Types.VARCHAR);
                            slotStmt.setNull(11, Types.TIMESTAMP);
                            slotStmt.setNull(12, Types.DOUBLE);
                            slotStmt.setNull(13, Types.VARCHAR);
                        }

                        slotStmt.executeUpdate();
                    }
                }
            }
        }

        private Timestamp timestampFromEpoch(Long epochSeconds) {
            if (epochSeconds == null) return null;
            return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
        }
    }
}