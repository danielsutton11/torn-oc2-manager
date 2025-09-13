package com.Torn.FactionCrimes._Algorithms;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import com.Torn.Discord.Messages.DiscordMessages;

/**
 * Algorithmic optimizer for assigning available members to available crime slots
 * based on CPR, crime value, and slot priority
 */
public class CrimeAssignmentOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(CrimeAssignmentOptimizer.class);

    // Weighting factors for the optimization algorithm
    private static final double CPR_WEIGHT = 0.3;           // 30% - Member's CPR for the slot
    private static final double CRIME_VALUE_WEIGHT = 0.2;   // 20% - Expected crime value
    private static final double SLOT_PRIORITY_WEIGHT = 0.50; // 50% - Slot importance in crime

    public static class FactionInfo {
        private final String factionId;
        private final String dbSuffix;

        public FactionInfo(String factionId, String dbSuffix) {
            this.factionId = factionId;
            this.dbSuffix = dbSuffix;
        }

        public String getFactionId() { return factionId; }
        public String getDbSuffix() { return dbSuffix; }
    }

    public static class AvailableMember {
        private final String userId;
        private final String username;
        private final Map<String, Integer> crimeSlotCPR;
        private final Timestamp lastJoinedCrimeDate;

        public AvailableMember(String userId, String username, Map<String, Integer> crimeSlotCPR,
                               Timestamp lastJoinedCrimeDate) {
            this.userId = userId;
            this.username = username;
            this.crimeSlotCPR = crimeSlotCPR != null ? crimeSlotCPR : new HashMap<>();
            this.lastJoinedCrimeDate = lastJoinedCrimeDate;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public Map<String, Integer> getCrimeSlotCPR() { return crimeSlotCPR; }
        public Timestamp getLastJoinedCrimeDate() { return lastJoinedCrimeDate; }

        public Integer getCPRForSlot(String crimeName, String slotName) {
            String key = crimeName + "|" + slotName;
            return crimeSlotCPR.getOrDefault(key, 0);
        }
    }

    public static class AvailableCrimeSlot {
        private final Long crimeId;
        private final String crimeName;
        private final String slotPosition;
        private final String slotPositionId;
        private final Integer crimeDifficulty;
        private final Timestamp expiredAt;
        private final Long expectedValue;
        private final Double slotPriority; // Higher = more important slot

        public AvailableCrimeSlot(Long crimeId, String crimeName, String slotPosition, String slotPositionId,
                                  Integer crimeDifficulty, Timestamp expiredAt, Long expectedValue, Double slotPriority) {
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.slotPosition = slotPosition;
            this.slotPositionId = slotPositionId;
            this.crimeDifficulty = crimeDifficulty;
            this.expiredAt = expiredAt;
            this.expectedValue = expectedValue != null ? expectedValue : 0L;
            this.slotPriority = slotPriority != null ? slotPriority : 1.0;
        }

        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getSlotPosition() { return slotPosition; }
        public String getSlotPositionId() { return slotPositionId; }
        public Integer getCrimeDifficulty() { return crimeDifficulty; }
        public Timestamp getExpiredAt() { return expiredAt; }
        public Long getExpectedValue() { return expectedValue; }
        public Double getSlotPriority() { return slotPriority; }

        public String getSlotKey() {
            return crimeId + "|" + slotPositionId;
        }
    }

    public static class MemberSlotAssignment {
        private final AvailableMember member;
        private final AvailableCrimeSlot slot;
        private final double optimizationScore;
        private final String reasoning;

        public MemberSlotAssignment(AvailableMember member, AvailableCrimeSlot slot,
                                    double optimizationScore, String reasoning) {
            this.member = member;
            this.slot = slot;
            this.optimizationScore = optimizationScore;
            this.reasoning = reasoning;
        }

        public AvailableMember getMember() { return member; }
        public AvailableCrimeSlot getSlot() { return slot; }
        public double getOptimizationScore() { return optimizationScore; }
        public String getReasoning() { return reasoning; }

        @Override
        public String toString() {
            return String.format("%s -> %s (%s) [Score: %.3f] %s",
                    member.getUsername(), slot.getCrimeName(), slot.getSlotPosition(),
                    optimizationScore, reasoning);
        }
    }

    public static class OptimizationResult {
        private final List<MemberSlotAssignment> assignments;
        private final double totalScore;
        private final int unfilledSlots;
        private final int unassignedMembers;

        public OptimizationResult(List<MemberSlotAssignment> assignments, double totalScore,
                                  int unfilledSlots, int unassignedMembers) {
            this.assignments = assignments;
            this.totalScore = totalScore;
            this.unfilledSlots = unfilledSlots;
            this.unassignedMembers = unassignedMembers;
        }

        public List<MemberSlotAssignment> getAssignments() { return assignments; }
        public double getTotalScore() { return totalScore; }
        public int getUnfilledSlots() { return unfilledSlots; }
        public int getUnassignedMembers() { return unassignedMembers; }

        @Override
        public String toString() {
            return String.format("OptimizationResult{assignments=%d, totalScore=%.3f, unfilled=%d, unassigned=%d}",
                    assignments.size(), totalScore, unfilledSlots, unassignedMembers);
        }
    }

    /**
     * Main entry point for optimizing crime assignments for all factions
     */
    public static void optimizeAllFactionsCrimeAssignments() throws SQLException {
        logger.info("Starting crime assignment optimization for all factions (urgency-free algorithm)");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("Database connections established successfully");

            List<FactionInfo> factions = getFactionInfo(configConnection);
            if (factions.isEmpty()) {
                logger.warn("No OC2-enabled factions found for optimization");
                return;
            }

            logger.info("Found {} OC2-enabled factions to optimize", factions.size());

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    logger.info("Optimizing crime assignments for faction: {} ({}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    OptimizationResult result = optimizeFactionCrimeAssignments(
                            configConnection, ocDataConnection, factionInfo);

                    if (result != null) {
                        logger.info("Optimization completed for faction {} - {} assignments (score: {:.3f})",
                                factionInfo.getFactionId(), result.getAssignments().size(), result.getTotalScore());

                        // Log top assignments
                        result.getAssignments().stream()
                                .limit(5) // Show top 5 assignments
                                .forEach(assignment -> logger.info("  → {}", assignment));

                        if (result.getAssignments().size() > 5) {
                            logger.info("  → ... and {} more assignments", result.getAssignments().size() - 5);
                        }

                        successfulCount++;
                    } else {
                        logger.warn("No optimization result for faction {}", factionInfo.getFactionId());
                        failedCount++;
                    }

                    processedCount++;

                } catch (Exception e) {
                    logger.error("Error optimizing faction {}: {}", factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                }
            }

            // Final summary
            logger.info("Crime assignment optimization completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);

        } catch (SQLException e) {
            logger.error("Database error during crime assignment optimization", e);
            throw e;
        }
    }

    /**
     * Optimize crime assignments for a single faction using advanced algorithms
     */
    private static OptimizationResult optimizeFactionCrimeAssignments(Connection configConnection,
                                                                      Connection ocDataConnection,
                                                                      FactionInfo factionInfo) throws SQLException {

        // Load available members with their CPR data
        List<AvailableMember> availableMembers = loadAvailableMembers(configConnection, ocDataConnection, factionInfo);

        // Load available crime slots with expected values
        List<AvailableCrimeSlot> availableSlots = loadAvailableCrimeSlots(configConnection, ocDataConnection, factionInfo);

        if (availableMembers.isEmpty() || availableSlots.isEmpty()) {
            logger.debug("Faction {} has {} members and {} slots available",
                    factionInfo.getFactionId(), availableMembers.size(), availableSlots.size());
            return new OptimizationResult(new ArrayList<>(), 0.0, availableSlots.size(), availableMembers.size());
        }

        logger.info("Optimizing {} members to {} slots for faction {}",
                availableMembers.size(), availableSlots.size(), factionInfo.getFactionId());

        // Run the optimization algorithm
        return optimizeAssignments(availableMembers, availableSlots);
    }

    /**
     * Core optimization algorithm using weighted scoring and Hungarian-like assignment
     */
    private static OptimizationResult optimizeAssignments(List<AvailableMember> members,
                                                          List<AvailableCrimeSlot> slots) {

        // Create scoring matrix
        double[][] scoreMatrix = createScoringMatrix(members, slots);

        // Apply Hungarian algorithm variant for optimal assignment
        List<MemberSlotAssignment> assignments = hungarianAssignment(members, slots, scoreMatrix);

        // Calculate total score and statistics
        double totalScore = assignments.stream().mapToDouble(MemberSlotAssignment::getOptimizationScore).sum();
        int unfilledSlots = slots.size() - assignments.size();
        int unassignedMembers = members.size() - assignments.size();

        // Sort assignments by score (highest first)
        assignments.sort((a, b) -> Double.compare(b.getOptimizationScore(), a.getOptimizationScore()));

        return new OptimizationResult(assignments, totalScore, unfilledSlots, unassignedMembers);
    }

    /**
     * Create scoring matrix for member-slot combinations
     */
    private static double[][] createScoringMatrix(List<AvailableMember> members, List<AvailableCrimeSlot> slots) {
        double[][] matrix = new double[members.size()][slots.size()];

        for (int m = 0; m < members.size(); m++) {
            AvailableMember member = members.get(m);

            for (int s = 0; s < slots.size(); s++) {
                AvailableCrimeSlot slot = slots.get(s);

                // Calculate weighted score for this member-slot combination
                double score = calculateMemberSlotScore(member, slot);
                matrix[m][s] = score;
            }
        }

        return matrix;
    }

    /**
     * Calculate optimization score for a member-slot combination (urgency removed)
     */
    private static double calculateMemberSlotScore(AvailableMember member, AvailableCrimeSlot slot) {
        // Factor 1: Member's CPR for this crime-slot (0-100, normalized to 0-1)
        Integer memberCPR = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());
        double cprScore = (memberCPR != null ? memberCPR : 0) / 100.0;

        // Factor 2: Crime value (normalized relative to maximum expected value)
        double valueScore = Math.min(slot.getExpectedValue() / 50_000_000.0, 1.0); // Cap at 50M

        // Factor 3: Slot priority within the crime (increased importance)
        double priorityScore = Math.min(slot.getSlotPriority(), 2.0) / 2.0; // Normalize to 0-1

        // Factor 4: Member activity bonus (recent activity = higher priority)
        double activityScore = calculateActivityScore(member);

        // Weighted combination (urgency factor removed, weights redistributed)
        double totalScore = (cprScore * CPR_WEIGHT) +
                (valueScore * CRIME_VALUE_WEIGHT) +
                (priorityScore * SLOT_PRIORITY_WEIGHT) +
                (activityScore * 0.1); // Small bonus for active members

        return Math.max(0.0, Math.min(1.0, totalScore)); // Clamp to [0, 1]
    }

    /**
     * Calculate activity score based on member's last crime participation
     */
    private static double calculateActivityScore(AvailableMember member) {
        if (member.getLastJoinedCrimeDate() == null) {
            return 0.3; // Default for members with no history
        }

        long daysSinceLastCrime = (System.currentTimeMillis() - member.getLastJoinedCrimeDate().getTime())
                / (1000 * 60 * 60 * 24);

        if (daysSinceLastCrime <= 1) return 1.0;   // Very recent
        if (daysSinceLastCrime <= 7) return 0.8;   // Recent
        if (daysSinceLastCrime <= 30) return 0.6;  // Moderate
        return 0.4; // Less active
    }

    /**
     * Hungarian algorithm variant for optimal assignment
     */
    private static List<MemberSlotAssignment> hungarianAssignment(List<AvailableMember> members,
                                                                  List<AvailableCrimeSlot> slots,
                                                                  double[][] scoreMatrix) {
        List<MemberSlotAssignment> assignments = new ArrayList<>();
        boolean[] memberUsed = new boolean[members.size()];
        boolean[] slotUsed = new boolean[slots.size()];

        // Greedy approach: repeatedly find the highest scoring available combination
        while (true) {
            int bestMember = -1;
            int bestSlot = -1;
            double bestScore = -1.0;

            // Find the best available member-slot combination
            for (int m = 0; m < members.size(); m++) {
                if (memberUsed[m]) continue;

                for (int s = 0; s < slots.size(); s++) {
                    if (slotUsed[s]) continue;

                    if (scoreMatrix[m][s] > bestScore) {
                        bestScore = scoreMatrix[m][s];
                        bestMember = m;
                        bestSlot = s;
                    }
                }
            }

            // No more good assignments found
            if (bestMember == -1 || bestScore < 0.1) { // Minimum threshold
                break;
            }

            // Make the assignment
            AvailableMember member = members.get(bestMember);
            AvailableCrimeSlot slot = slots.get(bestSlot);

            String reasoning = generateAssignmentReasoning(member, slot, bestScore);
            assignments.add(new MemberSlotAssignment(member, slot, bestScore, reasoning));

            memberUsed[bestMember] = true;
            slotUsed[bestSlot] = true;
        }

        return assignments;
    }

    /**
     * Generate human-readable reasoning for an assignment (priority-focused)
     */
    private static String generateAssignmentReasoning(AvailableMember member, AvailableCrimeSlot slot, double score) {
        List<String> reasons = new ArrayList<>();

        Integer cpr = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());

        // Emphasize slot priority first (since it's now 30% of the algorithm)
        if (slot.getSlotPriority() > 2.0) {
            reasons.add("Critical priority slot (weight: " + String.format("%.1f", slot.getSlotPriority()) + ")");
        } else if (slot.getSlotPriority() > 1.5) {
            reasons.add("High priority slot (weight: " + String.format("%.1f", slot.getSlotPriority()) + ")");
        }

        if (cpr != null && cpr > 80) {
            reasons.add("High CPR (" + cpr + "%)");
        } else if (cpr != null && cpr > 60) {
            reasons.add("Good CPR (" + cpr + "%)");
        }

        if (slot.getExpectedValue() > 20_000_000) {
            reasons.add("High value crime ($" + String.format("%.1fM", slot.getExpectedValue() / 1_000_000.0) + ")");
        }

        return reasons.isEmpty() ? "Best available match" : String.join(", ", reasons);
    }

    /**
     * Load available members with their CPR data
     */
    private static List<AvailableMember> loadAvailableMembers(Connection configConnection,
                                                              Connection ocDataConnection,
                                                              FactionInfo factionInfo) throws SQLException {
        List<AvailableMember> members = new ArrayList<>();
        String availableMembersTable = Constants.TABLE_NAME_AVAILABLE_MEMBERS + factionInfo.getDbSuffix();
        String cprTable = Constants.TABLE_NAME_CPR + factionInfo.getDbSuffix();

        // First get available members
        String membersSql = "SELECT user_id, username, last_joined_crime_date FROM " + availableMembersTable +
                " WHERE is_in_oc = false ORDER BY username";

        try (PreparedStatement membersStmt = ocDataConnection.prepareStatement(membersSql);
             ResultSet membersRs = membersStmt.executeQuery()) {

            while (membersRs.next()) {
                String userId = membersRs.getString("user_id");
                String username = membersRs.getString("username");
                Timestamp lastJoinedCrimeDate = membersRs.getTimestamp("last_joined_crime_date");

                // Load CPR data for this member
                Map<String, Integer> cprData = loadMemberCPRData(ocDataConnection, cprTable, userId);

                members.add(new AvailableMember(userId, username, cprData, lastJoinedCrimeDate));
            }
        } catch (SQLException e) {
            logger.debug("Could not load available members for faction {} (table might not exist): {}",
                    factionInfo.getFactionId(), e.getMessage());
        }

        logger.debug("Loaded {} available members for faction {}", members.size(), factionInfo.getFactionId());
        return members;
    }

    /**
     * Load CPR data for a specific member
     */
    private static Map<String, Integer> loadMemberCPRData(Connection ocDataConnection, String cprTable,
                                                          String userId) throws SQLException {
        Map<String, Integer> cprData = new HashMap<>();

        // Get all columns from the CPR table for this user
        String cprSql = "SELECT * FROM " + cprTable + " WHERE user_id = ?";

        try (PreparedStatement cprStmt = ocDataConnection.prepareStatement(cprSql)) {
            cprStmt.setString(1, userId);

            try (ResultSet cprRs = cprStmt.executeQuery()) {
                if (cprRs.next()) {
                    ResultSetMetaData metaData = cprRs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);

                        // Skip non-CPR columns
                        if (columnName.equals("user_id") || columnName.equals("username") ||
                                columnName.equals("faction_id") || columnName.equals("last_updated")) {
                            continue;
                        }

                        Integer cprValue = cprRs.getObject(i, Integer.class);
                        if (cprValue != null && cprValue > 0) {
                            // Convert column name back to "CrimeName|SlotName" format
                            String crimeSlotKey = convertColumnNameToCrimeSlotKey(columnName);
                            cprData.put(crimeSlotKey, cprValue);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not load CPR data for user {} from table {}: {}",
                    userId, cprTable, e.getMessage());
        }

        return cprData;
    }

    /**
     * Convert database column name back to "CrimeName|SlotName" format
     */
    private static String convertColumnNameToCrimeSlotKey(String columnName) {
        // This reverses the sanitization done in UpdateMemberCPR
        // Example: "btb_-_robber" -> "Break the Bank|Robber"

        // This is a simplified conversion - you may need to enhance this
        // based on your actual column naming patterns
        String[] parts = columnName.split("_-_", 2);
        if (parts.length == 2) {
            String crimeAbbrev = parts[0].toUpperCase();
            String slotName = parts[1].replace("_", " ");

            // Convert abbreviation back to full crime name (you might need a lookup table)
            String crimeName = expandCrimeAbbreviation(crimeAbbrev);

            return crimeName + "|" + capitalizeWords(slotName);
        }

        return columnName; // Fallback
    }

    /**
     * Expand crime abbreviation to full name
     */
    private static String expandCrimeAbbreviation(String abbrev) {
        // Add mappings for your crime abbreviations
        Map<String, String> expansions = Map.of(
                "BTB", "Break the Bank",
                "BFTP", "Blast from the Past",
                "SG", "Smash and Grab"
                // Add more as needed
        );

        return expansions.getOrDefault(abbrev, abbrev);
    }

    /**
     * Capitalize words in a string
     */
    private static String capitalizeWords(String input) {
        if (input == null || input.isEmpty()) return input;

        String[] words = input.split("\\s+");
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < words.length; i++) {
            if (i > 0) result.append(" ");
            String word = words[i];
            if (!word.isEmpty()) {
                result.append(Character.toUpperCase(word.charAt(0)));
                if (word.length() > 1) {
                    result.append(word.substring(1).toLowerCase());
                }
            }
        }

        return result.toString();
    }

    /**
     * Load available crime slots with expected values (urgency calculations removed)
     */
    private static List<AvailableCrimeSlot> loadAvailableCrimeSlots(Connection configConnection,
                                                                    Connection ocDataConnection,
                                                                    FactionInfo factionInfo) throws SQLException {
        List<AvailableCrimeSlot> slots = new ArrayList<>();
        String availableCrimesTable = "a_crimes_" + factionInfo.getDbSuffix();

        // Removed urgency-based ordering, now focuses on value and priority
        String slotsSql = "SELECT ac.crime_id, ac.name, ac.difficulty, ac.expired_at, " +
                "ac.slot_position, ac.slot_position_id, " +
                "oc2.rewards_value_high, oc2.rewards_value_low " +
                "FROM " + availableCrimesTable + " ac " +
                "LEFT JOIN " + Constants.TABLE_NAME_OC2_CRIMES + " oc2 ON ac.name = oc2.crime_name " +
                "ORDER BY oc2.rewards_value_high DESC, ac.difficulty DESC";

        try (PreparedStatement slotsStmt = ocDataConnection.prepareStatement(slotsSql);
             ResultSet slotsRs = slotsStmt.executeQuery()) {

            while (slotsRs.next()) {
                Long crimeId = slotsRs.getLong("crime_id");
                String crimeName = slotsRs.getString("name");
                String slotPosition = slotsRs.getString("slot_position");
                String slotPositionId = slotsRs.getString("slot_position_id");
                Integer difficulty = slotsRs.getObject("difficulty", Integer.class);
                Timestamp expiredAt = slotsRs.getTimestamp("expired_at");
                Long rewardsHigh = slotsRs.getObject("rewards_value_high", Long.class);
                Long rewardsLow = slotsRs.getObject("rewards_value_low", Long.class);

                // Calculate expected value (average of high/low, fallback to difficulty-based estimate)
                Long expectedValue = calculateExpectedValue(rewardsHigh, rewardsLow, difficulty);

                // Calculate slot priority (enhanced importance in the new algorithm)
                Double slotPriority = calculateSlotPriority(slotPosition, difficulty);

                slots.add(new AvailableCrimeSlot(crimeId, crimeName, slotPosition, slotPositionId,
                        difficulty, expiredAt, expectedValue, slotPriority));
            }
        } catch (SQLException e) {
            logger.debug("Could not load available crime slots for faction {} (table might not exist): {}",
                    factionInfo.getFactionId(), e.getMessage());
        }

        logger.debug("Loaded {} available crime slots for faction {}", slots.size(), factionInfo.getFactionId());
        return slots;
    }

    /**
     * Calculate expected value for a crime
     */
    private static Long calculateExpectedValue(Long rewardsHigh, Long rewardsLow, Integer difficulty) {
        if (rewardsHigh != null && rewardsLow != null && rewardsHigh > 0 && rewardsLow > 0) {
            return (rewardsHigh + rewardsLow) / 2;
        }

        // Fallback: estimate based on difficulty
        if (difficulty != null) {
            return (long) (difficulty * 1_000_000); // Rough estimate: 1M per difficulty point
        }

        return 5_000_000L; // Default estimate
    }

    /**
     * Calculate slot priority (enhanced for increased weighting)
     */
    private static Double calculateSlotPriority(String slotPosition, Integer difficulty) {
        double basePriority = 1.0;

        // Enhanced slot priority calculation since it now has 30% weight
        if (slotPosition != null) {
            String position = slotPosition.toLowerCase();

            // Higher priority for critical roles
            if (position.contains("robber") || position.contains("hacker")) {
                basePriority = 2.0; // Critical roles
            } else if (position.contains("muscle") || position.contains("engineer")) {
                basePriority = 1.8; // Important support roles
            } else if (position.contains("bomber") || position.contains("thief")) {
                basePriority = 1.6; // Specialized roles
            } else {
                basePriority = 1.4; // Standard roles
            }
        }

        // Adjust based on difficulty (higher difficulty = higher priority)
        if (difficulty != null && difficulty > 5) {
            basePriority *= (1.0 + (difficulty - 5) * 0.1);
        }

        return Math.min(basePriority, 3.0); // Cap at 3.0 for balance
    }

    /**
     * Get faction information from the config database
     */
    private static List<FactionInfo> getFactionInfo(Connection configConnection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT " + Constants.COLUMN_NAME_FACTION_ID + ", " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE oc2_enabled = true";

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);

                if (factionId != null && dbSuffix != null && isValidDbSuffix(dbSuffix)) {
                    factions.add(new FactionInfo(factionId, dbSuffix));
                } else {
                    logger.warn("Skipping faction with invalid data: factionId={}, dbSuffix={}", factionId, dbSuffix);
                }
            }
        }

        logger.info("Found {} OC2-enabled factions for optimization", factions.size());
        return factions;
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

    /**
     * Enhanced assignment recommendation system (urgency removed)
     */
    public static class AssignmentRecommendation {
        private final List<MemberSlotAssignment> immediateAssignments;
        private final List<String> strategicRecommendations;
        private final Map<String, Double> crimeCompletionProbabilities;
        private final long totalExpectedValue;

        public AssignmentRecommendation(List<MemberSlotAssignment> immediateAssignments,
                                        List<String> strategicRecommendations,
                                        Map<String, Double> crimeCompletionProbabilities,
                                        long totalExpectedValue) {
            this.immediateAssignments = immediateAssignments;
            this.strategicRecommendations = strategicRecommendations;
            this.crimeCompletionProbabilities = crimeCompletionProbabilities;
            this.totalExpectedValue = totalExpectedValue;
        }

        public List<MemberSlotAssignment> getImmediateAssignments() { return immediateAssignments; }
        public List<String> getStrategicRecommendations() { return strategicRecommendations; }
        public Map<String, Double> getCrimeCompletionProbabilities() { return crimeCompletionProbabilities; }
        public long getTotalExpectedValue() { return totalExpectedValue; }
    }

    /**
     * Generate comprehensive assignment recommendations (urgency-free approach)
     */
    public static AssignmentRecommendation generateAssignmentRecommendations(Connection configConnection,
                                                                             Connection ocDataConnection,
                                                                             FactionInfo factionInfo) throws SQLException {

        OptimizationResult result = optimizeFactionCrimeAssignments(configConnection, ocDataConnection, factionInfo);

        logger.info("DEBUG: Optimization result - {} assignments, {} unfilled slots, {} unassigned members",
                result.getAssignments().size(),
                result.getUnfilledSlots(),
                result.getUnassignedMembers());

        if (result.getAssignments().isEmpty()) {
            return new AssignmentRecommendation(new ArrayList<>(),
                    List.of("No optimal assignments found"), new HashMap<>(), 0L);
        }

        // Calculate crime completion probabilities
        Map<String, Double> completionProbs = calculateCrimeCompletionProbabilities(result.getAssignments());

        // Generate strategic recommendations (urgency removed)
        List<String> strategicRecommendations = generateStrategicRecommendations(result, factionInfo);

        // Calculate total expected value
        long totalExpectedValue = result.getAssignments().stream()
                .mapToLong(assignment -> assignment.getSlot().getExpectedValue())
                .sum();

        return new AssignmentRecommendation(result.getAssignments(), strategicRecommendations,
                completionProbs, totalExpectedValue);
    }

    /**
     * Calculate crime completion probabilities based on assignments
     */
    private static Map<String, Double> calculateCrimeCompletionProbabilities(List<MemberSlotAssignment> assignments) {
        Map<String, List<MemberSlotAssignment>> crimeAssignments = assignments.stream()
                .collect(Collectors.groupingBy(a -> a.getSlot().getCrimeName()));

        Map<String, Double> probabilities = new HashMap<>();

        for (Map.Entry<String, List<MemberSlotAssignment>> entry : crimeAssignments.entrySet()) {
            String crimeName = entry.getKey();
            List<MemberSlotAssignment> crimeSlots = entry.getValue();

            // Simple probability model: average of all slot CPRs
            double avgCPR = crimeSlots.stream()
                    .mapToInt(assignment -> assignment.getMember().getCPRForSlot(
                            assignment.getSlot().getCrimeName(),
                            assignment.getSlot().getSlotPosition()))
                    .average()
                    .orElse(50.0);

            // Convert CPR to completion probability (with some randomness factor)
            double probability = Math.min(0.95, avgCPR / 100.0 * 0.9 + 0.1);
            probabilities.put(crimeName, probability);
        }

        return probabilities;
    }

    /**
     * Generate strategic recommendations based on optimization results (urgency removed)
     */
    private static List<String> generateStrategicRecommendations(OptimizationResult result, FactionInfo factionInfo) {
        List<String> recommendations = new ArrayList<>();

        // Analysis of assignments
        if (result.getAssignments().isEmpty()) {
            recommendations.add("No members available for assignment - consider recruiting more active members");
            return recommendations;
        }

        // High-value crime recommendations (priority increased without urgency factor)
        long highValueThreshold = 30_000_000L;
        List<MemberSlotAssignment> highValueAssignments = result.getAssignments().stream()
                .filter(a -> a.getSlot().getExpectedValue() > highValueThreshold)
                .collect(Collectors.toList());

        if (!highValueAssignments.isEmpty()) {
            recommendations.add(String.format("Focus on %d high-value crimes (>$%dM) for maximum return",
                    highValueAssignments.size(), highValueThreshold / 1_000_000));
        }

        // High-priority slot recommendations (enhanced importance)
        List<MemberSlotAssignment> highPriorityAssignments = result.getAssignments().stream()
                .filter(a -> a.getSlot().getSlotPriority() > 1.8)
                .collect(Collectors.toList());

        if (!highPriorityAssignments.isEmpty()) {
            recommendations.add(String.format("Prioritize %d critical roles (Robber, Hacker) for optimal crime success",
                    highPriorityAssignments.size()));
        }

        // CPR improvement recommendations
        double avgScore = result.getAssignments().stream()
                .mapToDouble(MemberSlotAssignment::getOptimizationScore)
                .average()
                .orElse(0.0);

        if (avgScore < 0.6) {
            recommendations.add("Consider CPR training - average assignment quality is below optimal");
        }

        // Resource efficiency
        if (result.getUnfilledSlots() > result.getUnassignedMembers()) {
            recommendations.add("More crime slots available than members - consider recruitment");
        } else if (result.getUnassignedMembers() > result.getUnfilledSlots()) {
            recommendations.add("More members than slots - consider spawning additional crimes");
        }

        // Member activity recommendations
        long inactiveMembers = result.getAssignments().stream()
                .filter(a -> a.getMember().getLastJoinedCrimeDate() == null ||
                        (System.currentTimeMillis() - a.getMember().getLastJoinedCrimeDate().getTime()) >
                                (7 * 24 * 60 * 60 * 1000L)) // 7 days
                .count();

        if (inactiveMembers > 0) {
            recommendations.add(String.format("Consider engaging %d less active members in crimes to improve participation",
                    inactiveMembers));
        }

        // Value-based strategic recommendations (replaces urgency-based logic)
        double totalValue = result.getAssignments().stream()
                .mapToLong(a -> a.getSlot().getExpectedValue())
                .sum();

        if (totalValue > 200_000_000L) {
            recommendations.add("Excellent value potential - focus on completing these high-return crimes");
        } else if (totalValue < 50_000_000L) {
            recommendations.add("Consider waiting for higher-value crimes to maximize faction returns");
        }

        return recommendations;
    }


    /**
     * Discord member mapping from the members table
     */
    public static class DiscordMemberMapping {
        private final String userId;
        private final String username;
        private final String discordId;

        public DiscordMemberMapping(String userId, String username, String discordId) {
            this.userId = userId;
            this.username = username;
            this.discordId = discordId;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getDiscordId() { return discordId; }

        public String getMention() {
            return discordId != null ? "<@" + discordId + ">" : username;
        }
    }

    /**
     * Send Discord assignment notifications for all factions
     */
    public static void sendDiscordAssignmentNotifications() throws SQLException {
        logger.info("Sending Discord crime assignment notifications to all factions");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("Database connections established successfully");

            List<FactionInfo> factions = getFactionInfo(configConnection);
            if (factions.isEmpty()) {
                logger.warn("No OC2-enabled factions found for Discord notifications");
                return;
            }

            int successfulNotifications = 0;
            int failedNotifications = 0;

            for (FactionInfo factionInfo : factions) {
                try {
                    boolean success = sendFactionAssignmentNotification(configConnection, ocDataConnection, factionInfo);
                    if (success) {
                        successfulNotifications++;
                        logger.info("✓ Sent Discord assignment notification to faction {}", factionInfo.getFactionId());
                    } else {
                        failedNotifications++;
                        logger.warn("✗ Failed to send Discord assignment notification to faction {}", factionInfo.getFactionId());
                    }

                } catch (Exception e) {
                    failedNotifications++;
                    logger.error("Error sending Discord notification to faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                }
            }

            logger.info("Discord assignment notifications completed: {} successful, {} failed",
                    successfulNotifications, failedNotifications);

        } catch (SQLException e) {
            logger.error("Database error during Discord assignment notifications", e);
            throw e;
        }
    }

    /**
     * Send Discord assignment notification for a single faction
     */
    private static boolean sendFactionAssignmentNotification(Connection configConnection,
                                                             Connection ocDataConnection,
                                                             FactionInfo factionInfo) throws SQLException {

        // Generate assignment recommendations
        AssignmentRecommendation recommendation = generateAssignmentRecommendations(
                configConnection, ocDataConnection, factionInfo);

        // TEMPORARY DEBUG
        logger.info("URGENT DEBUG - Faction {}: {} assignments found",
                factionInfo.getFactionId(),
                recommendation.getImmediateAssignments().size());

        if (recommendation.getImmediateAssignments().isEmpty()) {
            logger.debug("No assignments to notify for faction {}", factionInfo.getFactionId());
            return true; // Not an error, just nothing to send
        }

        // Load Discord member mappings
        Map<String, DiscordMemberMapping> memberMappings = loadDiscordMemberMappings(
                ocDataConnection, factionInfo);

        if (memberMappings.isEmpty()) {
            logger.warn("No Discord member mappings found for faction {}", factionInfo.getFactionId());
            return false;
        }

        // Create and send Discord message
        return DiscordMessages.sendCrimeAssignmentToAllMembers(factionInfo, recommendation, memberMappings);
    }

    /**
     * Load Discord member mappings from the members table
     */
    private static Map<String, DiscordMemberMapping> loadDiscordMemberMappings(Connection ocDataConnection,
                                                                               FactionInfo factionInfo) throws SQLException {
        Map<String, DiscordMemberMapping> mappings = new HashMap<>();
        String membersTable = "members_" + factionInfo.getDbSuffix();

        String sql = "SELECT user_id, username, discord_id FROM " + membersTable +
                " WHERE discord_id IS NOT NULL AND discord_id != ''";

        try (PreparedStatement pstmt = ocDataConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String userId = rs.getString("user_id");
                String username = rs.getString("username");
                String discordId = rs.getString("discord_id");

                if (userId != null && discordId != null && !discordId.trim().isEmpty()) {
                    mappings.put(userId, new DiscordMemberMapping(userId, username, discordId.trim()));
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not load Discord member mappings for faction {} (table might not exist): {}",
                    factionInfo.getFactionId(), e.getMessage());
        }

        logger.debug("Loaded {} Discord member mappings for faction {}", mappings.size(), factionInfo.getFactionId());
        return mappings;
    }


}