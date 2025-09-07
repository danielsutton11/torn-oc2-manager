package com.Torn.FactionCrimes.AllCrimes;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.ItemMarketModel.Item;
import com.Torn.FactionCrimes.Models.ItemMarketModel.ItemMarketResponse;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GetAllOc2CrimesData {

    private static final Logger logger = LoggerFactory.getLogger(GetAllOc2CrimesData.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Cache for item market data to avoid repeated API calls
    private static final ConcurrentHashMap<Long, Item> itemCache = new ConcurrentHashMap<>();

    public static class OC2Crime {
        private final String name;
        private final int difficulty;
        private final int scopeCost;
        private final int scopeReturn;
        private final int totalSlots;
        private final List<OC2Slot> slots;

        public OC2Crime(String name, int difficulty, int scopeCost, int scopeReturn, int totalSlots, List<OC2Slot> slots) {
            this.name = name;
            this.difficulty = difficulty;
            this.scopeCost = scopeCost;
            this.scopeReturn = scopeReturn;
            this.totalSlots = totalSlots;
            this.slots = slots;
        }

        // Getters
        public String getName() { return name; }
        public int getDifficulty() { return difficulty; }
        public int getScopeCost() { return scopeCost; }
        public int getScopeReturn() { return scopeReturn; }
        public int getTotalSlots() { return totalSlots; }
        public List<OC2Slot> getSlots() { return slots; }
    }

    public static class OC2Slot {
        private final String slotId;
        private final String roleName;
        private final Long requiredItemId;
        private final String requiredItemName;
        private final boolean isReusable;
        private final Integer averagePrice;

        public OC2Slot(String slotId, String roleName, Long requiredItemId, String requiredItemName, boolean isReusable, Integer averagePrice) {
            this.slotId = slotId;
            this.roleName = roleName;
            this.requiredItemId = requiredItemId;
            this.requiredItemName = requiredItemName;
            this.isReusable = isReusable;
            this.averagePrice = averagePrice;
        }

        // Getters
        public String getSlotId() { return slotId; }
        public String getRoleName() { return roleName; }
        public Long getRequiredItemId() { return requiredItemId; }
        public String getRequiredItemName() { return requiredItemName; }
        public boolean isReusable() { return isReusable; }
        public Integer getAveragePrice() { return averagePrice; }
    }

    public static class OC2ItemData {
        private final String crimeName;
        private final String itemName;
        private final boolean isReusable;
        private final Integer averagePrice;
        private final boolean shouldBeTransferred;

        public OC2ItemData(String crimeName, String itemName, boolean isReusable,
                           Integer averagePrice, boolean shouldBeTransferred) {
            this.crimeName = crimeName;
            this.itemName = itemName;
            this.isReusable = isReusable;
            this.averagePrice = averagePrice;
            this.shouldBeTransferred = shouldBeTransferred;
        }

        public String getCrimeName() { return crimeName; }
        public String getItemName() { return itemName; }
        public boolean isReusable() { return isReusable; }
        public Integer getAveragePrice() { return averagePrice; }
        public boolean shouldBeTransferred() { return shouldBeTransferred; }
    }

    public static class RewardsRange {
        private final Long lowValue;
        private final Long highValue;

        public RewardsRange(Long lowValue, Long highValue) {
            this.lowValue = lowValue;
            this.highValue = highValue;
        }

        public Long getLowValue() { return lowValue; }
        public Long getHighValue() { return highValue; }
    }

    /**
     * Main entry point for fetching and processing all OC2 crimes data
     */
    public static void fetchAndProcessAllOC2Crimes() throws SQLException, IOException {
        logger.info("Starting OC2 crimes data fetch and processing");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to config database...");
        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("Database connections established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping OC2 crimes fetch to prevent further failures");
                return;
            }

            // Get admin API key
            String adminApiKey = getAdminApiKey(configConnection);
            if (adminApiKey == null) {
                logger.error("No admin API key found - cannot proceed");
                return;
            }

            // Fetch OC2 crimes data from Torn API
            logger.info("Fetching OC2 crimes data from Torn API...");
            String crimesJsonResponse = fetchOC2CrimesFromApi(adminApiKey);

            if (crimesJsonResponse == null) {
                logger.error("Failed to fetch OC2 crimes data from API");
                return;
            }

            // Parse and process the response
            List<OC2Crime> crimes = parseOC2CrimesResponse(crimesJsonResponse, adminApiKey);
            if (crimes.isEmpty()) {
                logger.warn("No crimes found in API response");
                return;
            }

            logger.info("Successfully parsed {} OC2 crimes", crimes.size());

            // Create tables if they don't exist
            createOC2CrimesTablesIfNotExists(configConnection);

            // Process crimes and populate tables
            processCrimesData(configConnection, ocDataConnection, crimes);

            logger.info("OC2 crimes data processing completed successfully");

        } catch (SQLException e) {
            logger.error("Database error during OC2 crimes processing", e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during OC2 crimes processing", e);
            throw e;
        }
    }

    /**
     * Get admin API key from database
     */
    private static String getAdminApiKey(Connection connection) throws SQLException {
        String sql = "SELECT " + Constants.COLUMN_NAME_API_KEY + " FROM " + Constants.TABLE_NAME_API_KEYS +
                " WHERE admin = true AND " + Constants.COLUMN_NAME_ACTIVE + " = true LIMIT 1";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);
                logger.info("Found admin API key");
                return apiKey;
            } else {
                logger.error("No admin API key found in database");
                return null;
            }
        }
    }

    /**
     * Fetch OC2 crimes data from Torn API
     */
    private static String fetchOC2CrimesFromApi(String apiKey) {
        String apiUrl = Constants.API_URL_TORN_ORGANISED_CRIMES;

        logger.debug("Fetching OC2 crimes from: {}", apiUrl);

        ApiResponse response = TornApiHandler.executeRequest(apiUrl, apiKey);

        if (response.isSuccess()) {
            logger.info("Successfully fetched OC2 crimes data from API");
            return response.getBody();
        } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
            logger.error("Circuit breaker is open - cannot fetch OC2 crimes data");
            return null;
        } else {
            logger.error("Failed to fetch OC2 crimes data: {}", response.getErrorMessage());
            return null;
        }
    }

    /**
     * Parse OC2 crimes response from API
     */
    private static List<OC2Crime> parseOC2CrimesResponse(String jsonResponse, String apiKey) throws IOException {
        List<OC2Crime> crimes = new ArrayList<>();

        try {
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            JsonNode organizedCrimesNode = rootNode.get(Constants.NODE_ORGANIZED_CRIMES);

            if (organizedCrimesNode == null || !organizedCrimesNode.isArray()) {
                logger.error("Invalid response format - missing or invalid organisedcrimes array");
                return crimes;
            }

            for (JsonNode crimeNode : organizedCrimesNode) {
                try {
                    OC2Crime crime = parseSingleCrime(crimeNode, apiKey);
                    if (crime != null) {
                        crimes.add(crime);
                    }
                } catch (Exception e) {
                    logger.warn("Error parsing individual crime: {}", e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Error parsing OC2 crimes JSON response", e);
            throw new IOException("Failed to parse OC2 crimes response", e);
        }

        return crimes;
    }

    /**
     * Parse a single crime from JSON
     */
    private static OC2Crime parseSingleCrime(JsonNode crimeNode, String apiKey) {
        try {
            String name = crimeNode.get(Constants.NODE_NAME).asText();
            int difficulty = crimeNode.get(Constants.NODE_DIFFICULTY).asInt();

            JsonNode scopeNode = crimeNode.get(Constants.NODE_SCOPE);
            int scopeCost = scopeNode.get(Constants.NODE_COST).asInt();
            int scopeReturn = scopeNode.get(Constants.NODE_RETURN).asInt();

            JsonNode slotsNode = crimeNode.get(Constants.NODE_SLOTS);
            List<OC2Slot> slots = parseCrimeSlots(slotsNode, apiKey);

            int totalSlots = slots.size();

            logger.debug("Parsed crime: {} (difficulty: {}, slots: {})", name, difficulty, totalSlots);

            return new OC2Crime(name, difficulty, scopeCost, scopeReturn, totalSlots, slots);

        } catch (Exception e) {
            logger.warn("Error parsing crime node: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Parse slots for a crime
     */
    private static List<OC2Slot> parseCrimeSlots(JsonNode slotsNode, String apiKey) {
        List<OC2Slot> slots = new ArrayList<>();

        if (slotsNode == null || !slotsNode.isArray()) {
            return slots;
        }

        // Group slots by position name to handle duplicates with #1, #2 logic
        Map<String, List<JsonNode>> groupedSlots = new HashMap<>();

        for (JsonNode slotNode : slotsNode) {
            String slotId = slotNode.get(Constants.NODE_ID).asText();
            String position = slotNode.get(Constants.NODE_NAME).asText();

            groupedSlots.computeIfAbsent(position, k -> new ArrayList<>()).add(slotNode);
        }

        // Process grouped slots with numbering logic
        for (Map.Entry<String, List<JsonNode>> entry : groupedSlots.entrySet()) {
            String basePosition = entry.getKey();
            List<JsonNode> group = entry.getValue();

            // Sort by slot ID for consistent ordering
            group.sort((a, b) -> a.get("id").asText().compareTo(b.get("id").asText()));

            for (int i = 0; i < group.size(); i++) {
                JsonNode slotNode = group.get(i);
                String slotId = slotNode.get(Constants.NODE_ID).asText();

                // Apply naming logic: single slot keeps original name, multiple get #1, #2, etc.
                String roleName = group.size() > 1 ? basePosition + " #" + (i + 1) : basePosition;

                // Parse required item
                Long requiredItemId = null;
                String requiredItemName = null;
                boolean isReusable = true;
                Integer averagePrice = null;

                JsonNode requiredItemNode = slotNode.get(Constants.NODE_REQUIRED_ITEMS);
                if (requiredItemNode != null && !requiredItemNode.isNull()) {
                    requiredItemId = requiredItemNode.get(Constants.NODE_ID).asLong();
                    isReusable = !requiredItemNode.get(Constants.NODE_IS_USED).asBoolean();

                    // Fetch item details from market
                    Item itemDetails = fetchItemMarketSafe(requiredItemId, apiKey);
                    if (itemDetails != null) {
                        requiredItemName = itemDetails.getName();
                        averagePrice = itemDetails.getAveragePrice();
                    }
                }

                slots.add(new OC2Slot(slotId, roleName, requiredItemId, requiredItemName, isReusable, averagePrice));
            }
        }

        return slots;
    }

    /**
     * Fetch item market data with caching and error handling
     */
    private static Item fetchItemMarketSafe(Long itemId, String apiKey) {
        if (itemId == null) return null;

        // Check cache first
        Item cachedItem = itemCache.get(itemId);
        if (cachedItem != null) {
            return cachedItem;
        }

        try {
            String itemUrl = Constants.API_URL_MARKET + "/" + itemId + Constants.API_URL_ITEM_MARKET;

            ApiResponse response = TornApiHandler.executeRequest(itemUrl, apiKey);

            if (response.isSuccess()) {
                ItemMarketResponse marketResponse = objectMapper.readValue(response.getBody(), ItemMarketResponse.class);
                if (marketResponse != null && marketResponse.getItemMarket() != null) {
                    Item item = marketResponse.getItemMarket();
                    itemCache.put(itemId, item);
                    return item;
                }
            } else {
                logger.debug("Failed to fetch item market data for item {}: {}", itemId, response.getErrorMessage());
            }

        } catch (Exception e) {
            logger.debug("Error fetching market data for item {}: {}", itemId, e.getMessage());
        }

        return null;
    }

    /**
     * Create the three OC2 crimes tables if they don't exist
     */
    private static void createOC2CrimesTablesIfNotExists(Connection connection) throws SQLException {
        // Create all_oc2_crimes table
        String createCrimesTableSql = "CREATE TABLE IF NOT EXISTS " + Constants.TABLE_NAME_OC2_CRIMES + "(" +
                "crime_name VARCHAR(255) PRIMARY KEY," +
                "difficulty INTEGER NOT NULL," +
                "scope_cost INTEGER NOT NULL," +
                "scope_return INTEGER NOT NULL," +
                "total_slots INTEGER NOT NULL," +
                "total_item_cost BIGINT," +
                "non_reusable_cost BIGINT," +
                "rewards_value_low BIGINT," +
                "rewards_value_high BIGINT," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ")";

        // Create all_oc2_crimes_slots table
        String createSlotsTableSql = "CREATE TABLE IF NOT EXISTS " + Constants.TABLE_NAME_OC2_CRIMES_SLOTS + "(" +
                "crime_name VARCHAR(255) NOT NULL," +
                "slot_1 VARCHAR(100)," +
                "slot_2 VARCHAR(100)," +
                "slot_3 VARCHAR(100)," +
                "slot_4 VARCHAR(100)," +
                "slot_5 VARCHAR(100)," +
                "slot_6 VARCHAR(100)," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (crime_name)" +
                ")";

        // CREATE NEW ITEMS TABLE
        String createItemsTableSql = "CREATE TABLE IF NOT EXISTS " + Constants.TABLE_NAME_OC2_ITEMS + " (" +
                "id SERIAL PRIMARY KEY," +
                "crime_name VARCHAR(255) NOT NULL," +
                "item_name VARCHAR(255) NOT NULL," +
                "is_reusable BOOLEAN NOT NULL," +
                "average_price INTEGER," +
                "should_be_transferred BOOLEAN NOT NULL DEFAULT FALSE," +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "UNIQUE(crime_name, item_name)" +
                ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createCrimesTableSql);
            stmt.execute(createSlotsTableSql);
            stmt.execute(createItemsTableSql); // ADD THIS LINE

            // Create indexes for existing tables (existing code)
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_all_oc2_crimes_difficulty ON all_oc2_crimes(difficulty)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_all_oc2_crimes_scope_cost ON all_oc2_crimes(scope_cost)");

            // CREATE INDEXES FOR NEW ITEMS TABLE
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + Constants.TABLE_NAME_OC2_ITEMS + "_crime_name ON " + Constants.TABLE_NAME_OC2_ITEMS + "(crime_name)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + Constants.TABLE_NAME_OC2_ITEMS + "_is_reusable ON " + Constants.TABLE_NAME_OC2_ITEMS + "(is_reusable)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_" + Constants.TABLE_NAME_OC2_ITEMS + "_should_be_transferred ON " + Constants.TABLE_NAME_OC2_ITEMS + "(should_be_transferred)");

            logger.info("OC2 crimes and items tables created or verified successfully");
        }
    }

    /**
     * Process crimes data and populate both tables
     */
    private static void processCrimesData(Connection configConnection, Connection ocDataConnection, List<OC2Crime> crimes) throws SQLException {
        logger.info("Processing {} crimes for database insertion", crimes.size());

        // Clear existing data and insert new data in transaction
        configConnection.setAutoCommit(false);
        try {
            clearExistingOC2Data(configConnection);

            // Collect all items while processing crimes
            List<OC2ItemData> allItems = new ArrayList<>();

            for (OC2Crime crime : crimes) {
                // Calculate costs (existing code)
                long totalItemCost = calculateTotalItemCost(crime.getSlots());
                long nonReusableCost = calculateNonReusableCost(crime.getSlots());

                // Get rewards range from OC data database (existing code)
                RewardsRange rewardsRange = getRewardsRange(configConnection, ocDataConnection, crime.getName());

                // Insert into all_oc2_crimes table (existing code)
                insertCrimeData(configConnection, crime, totalItemCost, nonReusableCost, rewardsRange);

                // Insert into all_oc2_crimes_slots table (existing code)
                insertCrimeSlotsData(configConnection, crime);

                // COLLECT ITEMS DATA
                List<OC2ItemData> crimeItems = extractItemsFromCrime(crime);
                allItems.addAll(crimeItems);
            }

            // INSERT ALL ITEMS DATA
            insertItemsData(configConnection, allItems);

            configConnection.commit();
            logger.info("Successfully processed and stored {} crimes and {} items", crimes.size(), allItems.size());

        } catch (SQLException e) {
            configConnection.rollback();
            logger.error("Failed to process crimes data, rolling back", e);
            throw e;
        } finally {
            configConnection.setAutoCommit(true);
        }
    }

    /**
     * Extract items data from a single crime
     */
    private static List<OC2ItemData> extractItemsFromCrime(OC2Crime crime) {
        List<OC2ItemData> items = new ArrayList<>();
        Set<String> processedItems = new HashSet<>(); // Avoid duplicates

        for (OC2Slot slot : crime.getSlots()) {
            if (slot.getRequiredItemName() != null && !slot.getRequiredItemName().isEmpty()) {
                String itemKey = crime.getName() + "|" + slot.getRequiredItemName();

                if (!processedItems.contains(itemKey)) {
                    processedItems.add(itemKey);

                    boolean shouldBeTransferred = slot.isReusable() &&
                            slot.getAveragePrice() != null &&
                            slot.getAveragePrice() >= Constants.ITEM_TRANSFER_THRESHOLD;

                    items.add(new OC2ItemData(
                            crime.getName(),
                            slot.getRequiredItemName(),
                            slot.isReusable(),
                            slot.getAveragePrice(),
                            shouldBeTransferred
                    ));
                }
            }
        }

        return items;
    }

    /**
     * Insert items data into the all_oc2_items table
     */
    private static void insertItemsData(Connection connection, List<OC2ItemData> items) throws SQLException {
        if (items.isEmpty()) {
            logger.info("No items to insert");
            return;
        }

        // Clear existing items data
        String clearSql = "DELETE FROM " + Constants.TABLE_NAME_OC2_ITEMS;
        try (PreparedStatement clearStmt = connection.prepareStatement(clearSql)) {
            clearStmt.executeUpdate();
            logger.debug("Cleared existing items data");
        }

        String insertSql = "INSERT INTO " + Constants.TABLE_NAME_OC2_ITEMS + " (" +
                "crime_name, item_name, is_reusable, average_price, should_be_transferred, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (OC2ItemData item : items) {
                pstmt.setString(1, item.getCrimeName());
                pstmt.setString(2, item.getItemName());
                pstmt.setBoolean(3, item.isReusable());

                if (item.getAveragePrice() != null) {
                    pstmt.setInt(4, item.getAveragePrice());
                } else {
                    pstmt.setNull(4, Types.INTEGER);
                }

                pstmt.setBoolean(5, item.shouldBeTransferred());
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            logger.info("Inserted {} items into {}", results.length, Constants.TABLE_NAME_OC2_ITEMS);

            // Log items that should be transferred
            long transferCount = items.stream().mapToLong(item -> item.shouldBeTransferred() ? 1 : 0).sum();
            logger.info("Items marked for transfer: {} (value >= ${})", transferCount, Constants.ITEM_TRANSFER_THRESHOLD);
        }
    }

    /**
     * Clear existing OC2 data from both tables
     */
    private static void clearExistingOC2Data(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("DELETE FROM " +Constants.TABLE_NAME_OC2_CRIMES);
            stmt.executeUpdate("DELETE FROM " + Constants.TABLE_NAME_OC2_CRIMES_SLOTS);
            stmt.executeUpdate("DELETE FROM " + Constants.TABLE_NAME_OC2_ITEMS);
            logger.debug("Cleared existing OC2 data from all tables");
        }
    }

    /**
     * Calculate total item cost for a crime
     */
    private static long calculateTotalItemCost(List<OC2Slot> slots) {
        return slots.stream()
                .filter(slot -> slot.getAveragePrice() != null)
                .mapToLong(slot -> slot.getAveragePrice().longValue())
                .sum();
    }

    /**
     * Calculate non-reusable item cost for a crime
     */
    private static long calculateNonReusableCost(List<OC2Slot> slots) {
        long totalCost = calculateTotalItemCost(slots);
        long reusableCost = slots.stream()
                .filter(slot -> slot.isReusable() && slot.getAveragePrice() != null)
                .mapToLong(slot -> slot.getAveragePrice().longValue())
                .sum();

        return totalCost - reusableCost;
    }

    /**
     * Get all faction db_suffix values from the config database
     */
    private static List<String> getFactionSuffixes(Connection configConnection) {
        List<String> suffixes = new ArrayList<>();

        String sql = "SELECT DISTINCT " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE oc2_enabled = true";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);

                if (isValidDbSuffix(dbSuffix)) {
                    suffixes.add(dbSuffix);
                } else {
                    logger.warn("Invalid or null db_suffix found: {}", dbSuffix);
                }
            }

        } catch (SQLException e) {
            logger.error("Error fetching faction suffixes from config database", e);
        }

        logger.debug("Found {} valid faction suffixes for rewards lookup", suffixes.size());
        return suffixes;
    }

    /**
     * Generate SQL query to get all rewards tables dynamically
     */
    private static String getAllRewardsTableQuery(Connection configConnection) {
        List<String> factionSuffixes = getFactionSuffixes(configConnection);

        if (factionSuffixes.isEmpty()) {
            logger.debug("No faction suffixes found - returning dummy query");
            return "SELECT NULL as crime_name, NULL as crime_value WHERE 1=0";
        }

        StringBuilder unionQuery = new StringBuilder();

        for (int i = 0; i < factionSuffixes.size(); i++) {
            if (i > 0) {
                unionQuery.append(" UNION ALL ");
            }

            unionQuery.append("SELECT crime_name, crime_value FROM r_crimes_")
                    .append(factionSuffixes.get(i))
                    .append(" WHERE crime_value IS NOT NULL");
        }

        logger.debug("Generated rewards table query for {} factions", factionSuffixes.size());
        return unionQuery.toString();
    }

    /**
     * Get rewards range for a crime from historical data
     */
    private static RewardsRange getRewardsRange(Connection configConnection, Connection ocDataConnection, String crimeName) {
        String sql = "SELECT MIN(crime_value) as min_value, MAX(crime_value) as max_value FROM (" +
                getAllRewardsTableQuery(configConnection) + ") AS all_rewards " +
                "WHERE crime_name = ?";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql)) {
            pstmt.setString(1, crimeName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Long lowValue = rs.getLong("min_value");
                    Long highValue = rs.getLong("max_value");

                    // Handle case where no data found (nulls)
                    if (rs.wasNull()) {
                        return new RewardsRange(null, null);
                    }

                    logger.debug("Found rewards range for {}: ${} - ${}", crimeName, lowValue, highValue);
                    return new RewardsRange(lowValue, highValue);
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not fetch rewards range for crime {}: {}", crimeName, e.getMessage());
        }

        return new RewardsRange(null, null);
    }

    /**
     * Insert crime data into all_oc2_crimes table
     */
    private static void insertCrimeData(Connection connection, OC2Crime crime, long totalItemCost,
                                        long nonReusableCost, RewardsRange rewardsRange) throws SQLException {
        String sql = "INSERT INTO all_oc2_crimes (" +
                "crime_name, difficulty, scope_cost, scope_return, total_slots, " +
                "total_item_cost, non_reusable_cost, rewards_value_low, rewards_value_high, " +
                "last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, crime.getName());
            pstmt.setInt(2, crime.getDifficulty());
            pstmt.setInt(3, crime.getScopeCost());
            pstmt.setInt(4, crime.getScopeReturn());
            pstmt.setInt(5, crime.getTotalSlots());
            pstmt.setLong(6, totalItemCost);
            pstmt.setLong(7, nonReusableCost);

            if (rewardsRange.getLowValue() != null) {
                pstmt.setLong(8, rewardsRange.getLowValue());
            } else {
                pstmt.setNull(8, java.sql.Types.BIGINT);
            }

            if (rewardsRange.getHighValue() != null) {
                pstmt.setLong(9, rewardsRange.getHighValue());
            } else {
                pstmt.setNull(9, java.sql.Types.BIGINT);
            }

            pstmt.executeUpdate();
            logger.debug("Inserted crime data for: {}", crime.getName());
        }
    }

    /**
     * Insert crime slots data into all_oc2_crimes_slots table
     */
    private static void insertCrimeSlotsData(Connection connection, OC2Crime crime) throws SQLException {
        String sql = "INSERT INTO all_oc2_crimes_slots (" +
                "crime_name, slot_1, slot_2, slot_3, slot_4, slot_5, slot_6, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, crime.getName());

            // Warn if crime has more than 6 slots
            if (crime.getSlots().size() > 6) {
                logger.warn("ATTENTION: Crime '{}' has {} slots but database only supports 6! " +
                                "Slots 7+ will be lost. Consider updating schema.",
                        crime.getName(), crime.getSlots().size());
            }

            // Set slots 1-6 (null if not present)
            for (int i = 1; i <= 6; i++) {
                if (i <= crime.getSlots().size()) {
                    pstmt.setString(i + 1, crime.getSlots().get(i - 1).getRoleName());
                } else {
                    pstmt.setNull(i + 1, java.sql.Types.VARCHAR);
                }
            }

            pstmt.executeUpdate();
            logger.debug("Inserted slots data for: {} ({} slots)", crime.getName(), crime.getSlots().size());
        }
    }

    /**
     * Validate db suffix for SQL injection prevention
     */
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }
}