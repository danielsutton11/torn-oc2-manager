package com.Torn.FactionCrimes.Available;

import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.FactionCrimes.Models.CrimesModel.Slot;
import com.Torn.Helpers.Constants;
import com.Torn.FactionCrimes.Models.ItemMarketModel.Item;
import com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import java.time.Instant;
import java.time.LocalDateTime;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import okhttp3.*;



public class GetAvailableCrimes {

    private static final Logger logger = LoggerFactory.getLogger(GetAvailableCrimes.class);
    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public GetAvailableCrimes(){
        try {

            CrimesResponse crimesData = fetchCrimesFromAPI();
            storeCrimesInDatabase(crimesData);
            logger.info("Crimes polling job completed successfully. Processed {} crimes",
                    crimesData.getCrimes().size());

        } catch (Exception e) {
            logger.error("Error in crimes polling job", e);
        }
    }

    private CrimesResponse fetchCrimesFromAPI() throws IOException {
        Request request = new Request.Builder()
                .url(Constants.API_URL_AVAILABLE_FACTION_CRIMES)
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                //.addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + System.getenv(Constants.TORN_LIMITED_API_KEY))
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
        String databaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);

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

    private final ConcurrentHashMap<Long, Item> itemCache = new ConcurrentHashMap<>();

    private void processCrime(Connection conn, Crime crime) throws SQLException {

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

                if (group.size() == 1) {
                    orderedSlots.add(group.get(0)); // Keep original position name
                } else {
                    for (int i = 0; i < group.size(); i++) {
                        Slot slot = group.get(i);
                        String renamedPosition = slot.getPosition() + " #" + (i + 1);
                        slot.setPosition(renamedPosition);
                        orderedSlots.add(slot);
                    }
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
                    Item itemMarket = itemCache.computeIfAbsent(itemId, this::fetchItemMarketSafe);

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
                .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                //.addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + System.getenv(Constants.TORN_LIMITED_API_KEY))
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

    private Item fetchItemMarketSafe(Long itemId) {
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
