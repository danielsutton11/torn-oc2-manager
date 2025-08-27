package com.Torn.FactionCrimes;

import com.Torn.FactionCrimes.CrimesModel.*;
import com.Torn.FactionCrimes.Helpers.Constants;
import com.Torn.FactionCrimes.ItemMarketModel.ItemMarket;
import com.Torn.FactionCrimes.ItemMarketModel.ItemMarketResponse;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import okhttp3.*;



public class AvailableCrimesPoller {

    private static final Logger logger = LoggerFactory.getLogger(AvailableCrimesPoller.class);
    private static final int POLLING_INTERVAL_MINUTES = getPollingInterval();

    public static void main(String[] args) {

        logger.info("Application starting...");
        logger.info("TORN_API_KEY present: {}", (System.getenv(Constants.TORN_LIMITED_API_KEY) != null && !System.getenv(Constants.TORN_LIMITED_API_KEY).isEmpty()));
        logger.info("DATABASE_URL present: {}", (System.getenv(Constants.DATABASE_URL) != null));
        logger.info("Polling interval: {} minutes", POLLING_INTERVAL_MINUTES);

        // Explicitly load PostgreSQL JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
            logger.info("PostgreSQL JDBC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC driver not found in classpath", e);
            System.exit(1);
        }

        if (System.getenv(Constants.TORN_LIMITED_API_KEY) == null || System.getenv(Constants.TORN_LIMITED_API_KEY).isEmpty()) {
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

    public static class CrimesPollingJob implements Job {
        private static final Logger logger = LoggerFactory.getLogger(CrimesPollingJob.class);
        private static final OkHttpClient client = new OkHttpClient();
        private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            logger.info("Starting Torn crimes polling job at {}", LocalDateTime.now());

            try {

                CrimesResponse crimesData = fetchCrimesFromAPI();
                storeCrimesInDatabase(crimesData);
                logger.info("Crimes polling job completed successfully. Processed {} crimes",
                        crimesData.getCrimes().size());

            } catch (Exception e) {
                logger.error("Error in crimes polling job", e);
                throw new JobExecutionException(e);
            }
        }

        private CrimesResponse fetchCrimesFromAPI() throws IOException {
            Request request = new Request.Builder()
                    .url(Constants.API_URL_AVAILABLE_FACTION_CRIMES)
                    .addHeader(Constants.ACCEPT_HEADER, Constants.ACCEPT_HEADER_VALUE)
                    .addHeader(Constants.AUTHORIZATION_HEADER, Constants.AUTHORIZATION_HEADER_VALUE + System.getenv(Constants.TORN_LIMITED_API_KEY))
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected response code: " + response.code() + " - " + response.message());
                }

                assert response.body() != null;
                String responseBody = response.body().string();
                logger.info("Successfully fetched data from Torn API");

                return objectMapper.readValue(responseBody, CrimesResponse.class);
            }
        }

        private void storeCrimesInDatabase(CrimesResponse crimesResponse) throws SQLException {
            String databaseUrl = System.getenv(Constants.DATABASE_URL);

            if (databaseUrl == null) {
                logger.error("DATABASE_URL environment variable not set");
                return;
            }

            String jdbcUrl = databaseUrl;
            String user = null;
            String password = null;

            if (databaseUrl.startsWith(Constants.POSTGRES_URL)) {
                String cleaned = databaseUrl.substring(Constants.POSTGRES_URL.length());
                String[] parts = cleaned.split("@");
                String[] userInfo = parts[0].split(":");
                user = userInfo[0];
                password = userInfo[1];

                jdbcUrl = Constants.POSTGRES_JDBC_URL + parts[1];
            }

            // Log safe version of the URL
            logger.info("Attempting to connect to database...");

            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
                logger.info("Database connection successful");
                createTablesIfNotExist(conn);

                List<Crime> sortedCrimes = crimesResponse.getCrimes().stream()
                        .filter(crime -> crime.getSlots() != null && !crime.getSlots().isEmpty()) // Filter to only crimes with slots
                        .sorted(Comparator
                                .comparing((Crime c) -> getStatusPriority(c.getStatus()))
                                .thenComparing(Crime::getDifficulty, Comparator.nullsLast(Comparator.reverseOrder()))
                                .thenComparing(Crime::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder()))
                                .thenComparing(Crime::getId))
                        .collect(Collectors.toList());

                for (Crime crime : sortedCrimes) {
                    processCrime(conn, crime);
                }

                logger.info("Successfully stored crimes data in database");
            } catch (SQLException e) {
                logger.error("Database connection failed. URL format: {}",
                        jdbcUrl.replaceAll(":[^:/@]+@", ":***@"));
                throw e;
            }
        }

        private void createTablesIfNotExist(Connection conn) throws SQLException {
            String createUnifiedTable = "CREATE TABLE IF NOT EXISTS Available_Faction_Crimes_Slots (" +
                    "crime_id BIGINT," +
                    "name VARCHAR(255) NOT NULL," +
                    "difficulty INTEGER," +
                    "status VARCHAR(50)," +
                    "created_at TIMESTAMP," +
                    "ready_at TIMESTAMP," +
                    "expired_at TIMESTAMP," +

                    // Slot-specific fields
                    "slot_position VARCHAR(100)," +
                    "slot_position_id VARCHAR(10)," +
                    "slot_position_number INTEGER," +
                    "item_required_id BIGINT," +
                    "item_required_name VARCHAR(255)," +
                    "item_required_avg_cost BIGINT," +
                    "item_is_reusable BOOLEAN," +

                    "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "PRIMARY KEY (crime_id, slot_position_id)" +
                    ")";


            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createUnifiedTable);

                stmt.execute("CREATE INDEX IF NOT EXISTS idx_available_faction_crimes_status ON Available_Faction_Crimes_Slots(status)");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_available_faction_crimes_created_at ON Available_Faction_Crimes_Slots(created_at)");

                logger.info("Unified faction_crimes table created or verified");
            }
        }

        private final ConcurrentHashMap<Long, ItemMarket> itemCache = new ConcurrentHashMap<>();

        private void processCrime(Connection conn, Crime crime) throws SQLException {
            // Delete existing rows for this crime (in case of update)
            try (PreparedStatement delete = conn.prepareStatement("DELETE FROM Available_Faction_Crimes_Slots WHERE crime_id = ?")) {
                delete.setLong(1, crime.getId());
                delete.executeUpdate();
            }

            if (crime.getSlots() == null || crime.getSlots().isEmpty()) {
                logger.info("Skipping crime ID {} - no available slots", crime.getId());
                return;
            }

            String insertSQL = "INSERT INTO Available_Faction_Crimes_Slots (" +
                    "crime_id, name, difficulty, status, created_at, ready_at, expired_at, " + // 1-7
                    "slot_position, slot_position_id, slot_position_number, " +                // 8-10
                    "item_required_id, item_required_name, item_required_avg_cost, item_is_reusable, " + // 11-14
                    "last_updated) " + // 15
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

            try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                Map<String, List<Slot>> groupedSlots = crime.getSlots().stream()
                        .collect(Collectors.groupingBy(Slot::getPosition));

                List<Slot> orderedSlots = new ArrayList<>();
                for (Map.Entry<String, List<Slot>> entry : groupedSlots.entrySet()) {
                    List<Slot> group = entry.getValue();
                    group.sort(Comparator.comparing(Slot::getPositionId, Comparator.nullsLast(String::compareTo)));

                    for (int i = 0; i < group.size(); i++) {
                        Slot slot = group.get(i);
                        String renamedPosition = slot.getPosition() + " #" + (i + 1);
                        slot.setPosition(renamedPosition);
                        orderedSlots.add(slot);
                    }
                }

                for (Slot slot : orderedSlots) {
                    if (slot.getUser() != null) continue;

                    // Crime fields
                    stmt.setLong(1, crime.getId());
                    stmt.setString(2, crime.getName());
                    stmt.setObject(3, crime.getDifficulty());
                    stmt.setString(4, crime.getStatus());
                    stmt.setTimestamp(5, timestampFromEpoch(crime.getCreatedAt()));
                    stmt.setTimestamp(6, timestampFromEpoch(crime.getReadyAt()));
                    stmt.setTimestamp(7, timestampFromEpoch(crime.getExpiredAt()));

                    // Slot fields
                    stmt.setString(8, slot.getPosition());
                    stmt.setString(9, slot.getPositionId());
                    stmt.setObject(10, slot.getPositionNumber());

                    if (slot.getItemRequirement() != null) {
                        Long itemId = slot.getItemRequirement().getId();
                        stmt.setObject(11, itemId);

                        // Use cache if available
                        ItemMarket itemMarket = itemCache.computeIfAbsent(itemId, this::fetchItemMarketSafe);

                        if (itemMarket != null) {
                            stmt.setString(12, itemMarket.getName());
                            stmt.setObject(13, itemMarket.getAveragePrice());
                        } else {
                            stmt.setNull(12, Types.VARCHAR);
                            stmt.setNull(13, Types.INTEGER);
                        }

                        stmt.setObject(14, slot.getItemRequirement().getIsReusable());
                    } else {
                        stmt.setNull(11, Types.BIGINT);
                        stmt.setNull(12, Types.VARCHAR);
                        stmt.setNull(13, Types.INTEGER);
                        stmt.setNull(14, Types.BOOLEAN);
                    }

                    try {
                        stmt.executeUpdate();
                    } catch (SQLException e) {
                        logger.warn("Failed to insert slot for crime {}: {}", crime.getId(), e.getMessage());
                    }
                }
            }
        }

        private ItemMarketResponse fetchItemInfo(Long itemId) {
            if (itemId == null) return null;

            Request request = new Request.Builder()
                    .url(Constants.API_URL_ITEM_MARKET + itemId + Constants.API_URL_ITEM_MARKET_JOIN)
                    .addHeader(Constants.ACCEPT_HEADER, Constants.ACCEPT_HEADER_VALUE)
                    .addHeader(Constants.AUTHORIZATION_HEADER, Constants.AUTHORIZATION_HEADER_VALUE + System.getenv(Constants.TORN_LIMITED_API_KEY))
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    logger.warn("Failed to fetch item info for ID {}: {}", itemId, response.message());
                    return null;
                }

                assert response.body() != null;
                String json = response.body().string();
                return objectMapper.readValue(json, ItemMarketResponse.class);

            } catch (IOException e) {
                logger.warn("Error fetching item info for ID {}", itemId, e);
                return null;
            }
        }

        private ItemMarket fetchItemMarketSafe(Long itemId) {
            try {
                ItemMarketResponse response = fetchItemInfo(itemId);
                return response != null ? response.getItemMarket() : null;
            } catch (Exception e) {
                logger.warn("Failed to fetch market data for item {}", itemId, e);
                return null;
            }
        }

        private Timestamp timestampFromEpoch(Long epochSeconds) {
            if (epochSeconds == null) return null;
            return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
        }

        private int getStatusPriority(String status) {
            if (Constants.PLANNING.equalsIgnoreCase(status)) return 1;
            if (Constants.RECRUITING.equalsIgnoreCase(status)) return 2;
            return 3;
        }
    }
}