package com.Torn.FactionCrimes.Completed;

import com.Torn.Api.ApiResponse;
import com.Torn.Api.TornApiHandler;
import com.Torn.Execute;
import com.Torn.FactionCrimes.Models.CrimesModel.Crime;
import com.Torn.FactionCrimes.Models.CrimesModel.CrimesResponse;
import com.Torn.Helpers.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class GetPaidCrimesData {

    /**
     * ├── GetPaidCrimesData.java
     * │   ├── Fetches payout data for completed crimes
     * │   ├── Updates member payout percentages and amounts
     * │   └── Calculates faction vs member splits
     */

    private static final Logger logger = LoggerFactory.getLogger(GetPaidCrimesData.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final int TORN_API_RATE_LIMIT_MS = 2000;

    // Default lookback times (can be overridden by environment variables)
    private static final int DEFAULT_PAYOUT_LOOKBACK_HOURS = 168; // Default 168 hours (7 days) for payout look back
    private static final int MAX_PAGES_PAYOUT = 15; // Reasonable limit for payout sync

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;
        private final String apiKey;
        private final String ownerName;

        public FactionInfo(String factionId, String dbSuffix, String apiKey, String ownerName) {
            this.factionId = factionId;
            this.dbSuffix = dbSuffix;
            this.apiKey = apiKey;
            this.ownerName = ownerName;
        }

        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
        public String getApiKey() { return apiKey; }
        public String getOwnerName() { return ownerName; }
    }

    public static class UnpaidCrime {
        private final Long crimeId;
        private final String crimeName;
        private final Long crimeValue;

        public UnpaidCrime(Long crimeId, String crimeName, Long crimeValue) {
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.crimeValue = crimeValue;
        }

        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public Long getCrimeValue() { return crimeValue; }
    }

    /**
     * Main entry point for processing payout data for all factions
     */
    public static void fetchAndProcessAllPaidCrimes() throws SQLException, IOException {
        logger.info("Starting paid crimes processing for all factions with configurable timestamp filtering");

        String databaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);
        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Connecting to paid crimes database...");
        try (Connection connection = Execute.postgres.connect(databaseUrl, logger)) {
            logger.info("Database connection established successfully");

            // Check circuit breaker status before starting
            TornApiHandler.CircuitBreakerStatus cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Circuit breaker status: {}", cbStatus);

            if (cbStatus.isOpen()) {
                logger.error("Circuit breaker is OPEN - skipping paid crimes processing to prevent further failures");
                return;
            }

            // Get faction information from config database
            List<FactionInfo> factions = getFactionInfo();
            if (factions.isEmpty()) {
                logger.warn("No active OC2-enabled factions found for paid crimes processing");
                return;
            }

            logger.info("Found {} OC2-enabled factions to process paid crimes for", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;
            int totalPayoutsProcessed = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Processing paid crimes for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    // Process paid crimes for this faction
                    PaidCrimesResult result = fetchAndProcessPaidCrimesForFaction(connection, factionInfo);

                    if (result.isSuccess()) {
                        logger.info("✓ Successfully processed {} payout updates for faction {} ({})",
                                result.getPayoutsProcessed(), factionInfo.getFactionId(),
                                factionInfo.getOwnerName());
                        successfulCount++;
                        totalPayoutsProcessed += result.getPayoutsProcessed();
                    } else if (result.isCircuitBreakerOpen()) {
                        logger.error("Circuit breaker opened during processing - stopping remaining factions");
                        break;
                    } else {
                        logger.error("Failed to process paid crimes for faction {} ({}): {}",
                                factionInfo.getFactionId(), factionInfo.getOwnerName(),
                                result.getErrorMessage());
                        failedCount++;
                    }

                    processedCount++;

                    // Rate limiting between factions
                    if (processedCount < factions.size()) {
                        logger.debug("Waiting {}ms before processing next faction", TORN_API_RATE_LIMIT_MS);
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } catch (InterruptedException e) {
                    logger.warn("Paid crimes processing interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Unexpected error processing paid crimes for faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("Paid crimes processing finished:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Total payouts processed: {}", totalPayoutsProcessed);

            // Log final circuit breaker status
            cbStatus = TornApiHandler.getCircuitBreakerStatus();
            logger.info("Final circuit breaker status: {}", cbStatus);

            if (failedCount > successfulCount && processedCount > 2) {
                logger.error("More than half of factions failed - Torn API may be experiencing issues");
            }

        } catch (SQLException e) {
            logger.error("Database error during paid crimes processing", e);
            throw e;
        }
    }

    /**
     * Get faction information from the config database (OC2 enabled factions only)
     */
    private static List<FactionInfo> getFactionInfo() throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger)) {
            String sql = "SELECT DISTINCT ON (f." + Constants.COLUMN_NAME_FACTION_ID + ") " +
                    "f." + Constants.COLUMN_NAME_FACTION_ID + ", " +
                    "f." + Constants.COLUMN_NAME_DB_SUFFIX + ", " +
                    "ak." + Constants.COLUMN_NAME_API_KEY + ", " +
                    "ak." + Constants.COLUMN_NAME_OWNER_NAME + " " +
                    "FROM " + Constants.TABLE_NAME_FACTIONS + " f " +
                    "JOIN " + Constants.TABLE_NAME_API_KEYS + " ak ON f." + Constants.COLUMN_NAME_FACTION_ID + " = ak.faction_id " +
                    "WHERE ak." + Constants.COLUMN_NAME_ACTIVE + " = true " +
                    "AND f.oc2_enabled = true " +
                    "ORDER BY f." + Constants.COLUMN_NAME_FACTION_ID + ", ak." + Constants.COLUMN_NAME_API_KEY;

            try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {

                while (rs.next()) {
                    String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                    String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);
                    String apiKey = rs.getString(Constants.COLUMN_NAME_API_KEY);
                    String ownerName = rs.getString(Constants.COLUMN_NAME_OWNER_NAME);

                    if (factionId == null || dbSuffix == null || apiKey == null) {
                        logger.warn("Skipping faction with null data: factionId={}, dbSuffix={}, apiKey={}",
                                factionId, dbSuffix, (apiKey == null ? "null" : "***"));
                        continue;
                    }

                    if (!isValidDbSuffix(dbSuffix)) {
                        logger.error("Invalid db_suffix for faction {}: {}", factionId, dbSuffix);
                        continue;
                    }

                    factions.add(new FactionInfo(factionId, dbSuffix, apiKey, ownerName));
                }
            }
        }

        logger.info("Found {} OC2-enabled factions for paid crimes processing", factions.size());
        return factions;
    }

    /**
     * Fetch and process paid crimes for a single faction
     */
    private static PaidCrimesResult fetchAndProcessPaidCrimesForFaction(Connection connection, FactionInfo factionInfo) {
        try {
            String rewardsTableName = Constants.TABLE_NAME_REWARDS_CRIMES + factionInfo.getDbSuffix();

            // Get crimes that need payout data
            List<UnpaidCrime> unpaidCrimes = getUnpaidCrimes(connection, rewardsTableName);
            if (unpaidCrimes.isEmpty()) {
                logger.debug("No unpaid crimes found for faction {}", factionInfo.getFactionId());
                return PaidCrimesResult.success(0);
            }

            logger.info("Found {} crimes needing payout data for faction {}",
                    unpaidCrimes.size(), factionInfo.getFactionId());

            // FIXED: Get timestamp for API filtering based on environment variables and database state
            PayoutTimestampConfig timestampConfig = getPayoutTimestampConfig(connection, rewardsTableName);
            logger.info("Fetching payout data for faction {} ({})",
                    factionInfo.getFactionId(),
                    timestampConfig.isInitialSync() ? "Initial sync - getting ALL payout data" :
                            "Incremental update from timestamp: " + formatTimestamp(timestampConfig.getFromTimestamp()));

            // Fetch payout data from Torn API with timestamp filtering
            Map<Long, PayoutData> payoutDataMap = fetchPayoutData(factionInfo, timestampConfig);

            if (payoutDataMap.isEmpty()) {
                logger.debug("No payout data found in API for faction {}", factionInfo.getFactionId());
                return PaidCrimesResult.success(0);
            }

            // Update database with payout information
            int updatedCount = updatePayoutData(connection, rewardsTableName, unpaidCrimes, payoutDataMap);

            logger.info("Updated {} crimes with payout data for faction {}", updatedCount, factionInfo.getFactionId());
            return PaidCrimesResult.success(updatedCount);

        } catch (Exception e) {
            logger.error("Exception processing paid crimes for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return PaidCrimesResult.failure("Processing exception: " + e.getMessage());
        }
    }

    /**
     * Get timestamp for payout data filtering based on environment variables and database state
     */
    private static PayoutTimestampConfig getPayoutTimestampConfig(Connection connection, String rewardsTableName) {
        // Check for environment variable override first
        String overrideFromTimestamp = System.getenv(Constants.OVERRIDE_PAYOUT_CRIMES_FROM_TIMESTAMP);
        if (overrideFromTimestamp != null && !overrideFromTimestamp.trim().isEmpty()) {
            try {
                long fromTimestamp = Long.parseLong(overrideFromTimestamp.trim());
                logger.info("Using override payout FROM timestamp from environment: {} ({})",
                        fromTimestamp, formatTimestamp(fromTimestamp));
                return new PayoutTimestampConfig(false, fromTimestamp);
            } catch (NumberFormatException e) {
                logger.error("Invalid OVERRIDE_PAYOUT_CRIMES_FROM_TIMESTAMP format: {}, using default", overrideFromTimestamp);
            }
        }

        // Check if this is initial sync (no payout data exists yet)
        try {
            String countSql = "SELECT COUNT(*) FROM " + rewardsTableName + " WHERE paid_date IS NOT NULL AND paid_date != ''";
            try (PreparedStatement pstmt = connection.prepareStatement(countSql);
                 ResultSet rs = pstmt.executeQuery()) {

                if (rs.next() && rs.getInt(1) == 0) {
                    // No payout data exists - do initial sync to get ALL data
                    logger.info("No existing payout data found - performing initial sync (getting ALL payout data)");
                    return new PayoutTimestampConfig(true, null);
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not check existing payout data, defaulting to incremental: {}", e.getMessage());
        }

        // Check for custom look back period for incremental updates
        int lookbackHours = getEnvironmentInt(Constants.OVERRIDE_PAYOUT_CRIMES_LOOKBACK_HOURS, DEFAULT_PAYOUT_LOOKBACK_HOURS);
        if(lookbackHours == 0) {lookbackHours = DEFAULT_PAYOUT_LOOKBACK_HOURS;}

        // FIXED: Calculate cutoff time ONCE and use it consistently
        Instant cutoffTime = Instant.now().minusSeconds(lookbackHours * 3600L);
        long fromTimestamp = cutoffTime.getEpochSecond();

        logger.info("Using calculated payout FROM timestamp: {} ({} hours ago)",
                fromTimestamp, lookbackHours);
        return new PayoutTimestampConfig(false, fromTimestamp);
    }

    /**
     * Helper class to hold payout timestamp configuration
     */
    private static class PayoutTimestampConfig {
        private final boolean isInitialSync;
        private final Long fromTimestamp;

        public PayoutTimestampConfig(boolean isInitialSync, Long fromTimestamp) {
            this.isInitialSync = isInitialSync;
            this.fromTimestamp = fromTimestamp;
        }

        public boolean isInitialSync() { return isInitialSync; }
        public Long getFromTimestamp() { return fromTimestamp; }
    }

    /**
     * Helper to get integer from environment with default fallback
     */
    private static int getEnvironmentInt(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value != null && !value.trim().isEmpty()) {
            try {
                int parsed = Integer.parseInt(value.trim());
                logger.info("Using {} = {} from environment", envVar, parsed);
                return parsed;
            } catch (NumberFormatException e) {
                logger.warn("Invalid {} format: {}, using default: {}", envVar, value, defaultValue);
            }
        }
        return defaultValue;
    }

    /**
     * Format timestamp for logging
     */
    private static String formatTimestamp(Long timestamp) {
        if (timestamp == null) return "null";
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT));
    }

    /**
     * Get crimes that need payout data (missing paid_date)
     */
    private static List<UnpaidCrime> getUnpaidCrimes(Connection connection, String rewardsTableName) throws SQLException {
        List<UnpaidCrime> unpaidCrimes = new ArrayList<>();

        String sql = "SELECT crime_id, crime_name, crime_value FROM " + rewardsTableName +
                " WHERE paid_date IS NULL OR paid_date = ''";

        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                Long crimeId = rs.getLong(Constants.COLUMN_NAME_CRIME_ID);
                String crimeName = rs.getString(Constants.COLUMN_NAME_CRIME_NAME);
                Long crimeValue = rs.getLong(Constants.COLUMN_NAME_CRIME_VALUE);

                unpaidCrimes.add(new UnpaidCrime(crimeId, crimeName, crimeValue));
            }
        }

        return unpaidCrimes;
    }

    /**
     * FIXED: Fetch payout data from Torn API with 'from' timestamp parameter
     */
    private static Map<Long, PayoutData> fetchPayoutData(FactionInfo factionInfo, PayoutTimestampConfig timestampConfig) {
        Map<Long, PayoutData> payoutDataMap = new HashMap<>();

        try {
            int currentPage = 0;
            boolean hasMoreData = true;

            while (hasMoreData && currentPage < MAX_PAGES_PAYOUT) {
                // FIXED: Construct API URL with 'from' parameter for timestamp filtering
                String apiUrl = buildPayoutApiUrl(factionInfo.getApiKey(), currentPage, timestampConfig.getFromTimestamp());

                logger.debug("Fetching payout data page {} for faction: {} ({})",
                        currentPage, factionInfo.getFactionId(),
                        timestampConfig.getFromTimestamp() != null ?
                                "from timestamp: " + timestampConfig.getFromTimestamp() : "getting ALL data");

                // Use robust API handler
                ApiResponse response = TornApiHandler.executeRequest(apiUrl, factionInfo.getApiKey());

                if (response.isSuccess()) {
                    CrimesResponse crimesResponse = objectMapper.readValue(response.getBody(), CrimesResponse.class);

                    if (crimesResponse.getCrimes() == null || crimesResponse.getCrimes().isEmpty()) {
                        logger.debug("No more crimes found for faction {} at page {}",
                                factionInfo.getFactionId(), currentPage);
                        hasMoreData = false;
                        break;
                    }

                    // Process crimes for payout data
                    for (Crime crime : crimesResponse.getCrimes()) {
                        PayoutData payout = extractPayoutData(crime);
                        if (payout != null) {
                            payoutDataMap.put(crime.getId(), payout);
                        }
                    }

                    // Check if we got less than a full page
                    if (crimesResponse.getCrimes().size() < 10) { // CRIMES_PER_PAGE
                        hasMoreData = false;
                    }

                    currentPage++;

                    // Rate limiting between API calls
                    if (hasMoreData && currentPage < MAX_PAGES_PAYOUT) {
                        Thread.sleep(TORN_API_RATE_LIMIT_MS);
                    }

                } else if (response.getType() == ApiResponse.ResponseType.CIRCUIT_BREAKER_OPEN) {
                    logger.error("Circuit breaker is open - stopping payout fetch for faction {}",
                            factionInfo.getFactionId());
                    break;
                } else {
                    logger.warn("API error fetching payout data for faction {} at page {}: {}",
                            factionInfo.getFactionId(), currentPage, response.getErrorMessage());
                    break;
                }
            }

        } catch (Exception e) {
            logger.error("Error fetching payout data for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
        }

        logger.debug("Found payout data for {} crimes for faction {}",
                payoutDataMap.size(), factionInfo.getFactionId());
        return payoutDataMap;
    }

    /**
     * Build API URL with 'from' parameter for timestamp filtering (only for incremental updates)
     */
    private static String buildPayoutApiUrl(String apiKey, int currentPage, Long fromTimestamp) {
        StringBuilder url = new StringBuilder();
        url.append(Constants.API_URL_COMPLETED_FACTION_CRIMES)
                .append(currentPage)
                .append(Constants.API_URL_TORN_PARAMETER_JOIN_AND + Constants.API_URL_TORN_PARAMETER_SORT + Constants.API_URL_TORN_PARAMETER_DESC);

        if (fromTimestamp != null) {
            url.append(Constants.API_URL_TORN_PARAMETER_JOIN_AND + Constants.API_URL_TORN_PARAMETER_FROM).append(fromTimestamp);
        }

        return url.toString();
    }

    /**
     * Extract payout data from a crime's rewards
     */
    private static PayoutData extractPayoutData(Crime crime) {
        try {
            if (crime.getRewards() == null || !(crime.getRewards() instanceof Map)) {
                return null;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> rewards = (Map<String, Object>) crime.getRewards();

            @SuppressWarnings("unchecked")
            Map<String, Object> payout = (Map<String, Object>) rewards.get("payout");

            if (payout == null) {
                return null;
            }

            // Extract payout information
            Number percentageNum = (Number) payout.get(Constants.NODE_PERCENTAGE);
            Number paidAtNum = (Number) payout.get(Constants.NODE_PAID_AT);

            if (percentageNum != null && paidAtNum != null) {
                double percentage = percentageNum.doubleValue();
                long paidAtTimestamp = paidAtNum.longValue();
                String paidDateFriendly = formatFriendlyDate(paidAtTimestamp);

                return new PayoutData(percentage, paidAtTimestamp, paidDateFriendly);
            }

        } catch (Exception e) {
            logger.debug("Error extracting payout data from crime {}: {}", crime.getId(), e.getMessage());
        }

        return null;
    }

    /**
     * Update database with payout information
     */
    private static int updatePayoutData(Connection connection, String rewardsTableName,
                                        List<UnpaidCrime> unpaidCrimes, Map<Long, PayoutData> payoutDataMap) throws SQLException {

        String updateSql = "UPDATE " + rewardsTableName + " SET " +
                "member_payout_percentage = ?, member_payout_amount = ?, " +
                "paid_date = ?, faction_payout = ?, last_updated = CURRENT_TIMESTAMP " +
                "WHERE crime_id = ?";

        int updatedCount = 0;

        try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
            for (UnpaidCrime unpaidCrime : unpaidCrimes) {
                PayoutData payoutData = payoutDataMap.get(unpaidCrime.getCrimeId());

                if (payoutData != null) {
                    // Calculate payout amounts
                    double memberPayoutPercentage = payoutData.getPercentage();
                    long memberPayoutAmount = Math.round(unpaidCrime.getCrimeValue() * (memberPayoutPercentage / 100.0));
                    long factionPayout = unpaidCrime.getCrimeValue() - memberPayoutAmount;

                    pstmt.setDouble(1, memberPayoutPercentage);
                    pstmt.setLong(2, memberPayoutAmount);
                    pstmt.setString(3, payoutData.getPaidDateFriendly());
                    pstmt.setLong(4, factionPayout);
                    pstmt.setLong(5, unpaidCrime.getCrimeId());

                    pstmt.addBatch();
                    updatedCount++;
                }
            }

            if (updatedCount > 0) {
                pstmt.executeBatch();
            }
        }

        return updatedCount;
    }

    /**
     * Format timestamp to friendly date string
     */
    private static String formatFriendlyDate(Long epochSeconds) {
        if (epochSeconds == null) {
            return null;
        }

        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(epochSeconds),
                ZoneId.systemDefault()
        );

        return dateTime.format(DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT));
    }

    // Utility methods
    private static boolean isValidDbSuffix(String dbSuffix) {
        return dbSuffix != null &&
                dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$") &&
                !dbSuffix.isEmpty() &&
                dbSuffix.length() <= 50;
    }

    /**
     * Payout data holder
     */
    private static class PayoutData {
        private final double percentage;
        private final long paidAtTimestamp;
        private final String paidDateFriendly;

        public PayoutData(double percentage, long paidAtTimestamp, String paidDateFriendly) {
            this.percentage = percentage;
            this.paidAtTimestamp = paidAtTimestamp;
            this.paidDateFriendly = paidDateFriendly;
        }

        public double getPercentage() { return percentage; }
        public long getPaidAtTimestamp() { return paidAtTimestamp; }
        public String getPaidDateFriendly() { return paidDateFriendly; }
    }

    /**
     * Result wrapper for paid crimes processing
     */
    private static class PaidCrimesResult {
        private final boolean success;
        private final boolean circuitBreakerOpen;
        private final int payoutsProcessed;
        private final String errorMessage;

        private PaidCrimesResult(boolean success, boolean circuitBreakerOpen, int payoutsProcessed, String errorMessage) {
            this.success = success;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.payoutsProcessed = payoutsProcessed;
            this.errorMessage = errorMessage;
        }

        public static PaidCrimesResult success(int payoutsProcessed) {
            return new PaidCrimesResult(true, false, payoutsProcessed, null);
        }

        public static PaidCrimesResult failure(String errorMessage) {
            return new PaidCrimesResult(false, false, 0, errorMessage);
        }

        public static PaidCrimesResult circuitBreakerOpen() {
            return new PaidCrimesResult(false, true, 0, "Circuit breaker is open");
        }

        public boolean isSuccess() { return success; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public int getPayoutsProcessed() { return payoutsProcessed; }
        public String getErrorMessage() { return errorMessage; }
    }
}