package com.Torn.FactionCrimes._Algorithms;

import com.Torn.Execute;
import com.Torn.FactionCrimes.Available.GetAvailableCrimes;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import com.Torn.Discord.Messages.DiscordMessages;

import static com.Torn.Discord.Messages.DiscordMessages.sendNeedCrimesToSpawnForAvailableMembers;

/**
 * Algorithmic optimizer for assigning available members to available crime slots
 * based on CPR, crime value, and slot priority

 For Each Faction:
 ├── Load Available Members (from OC_DATA)
 │   ├── Get members not currently in crimes
 │   ├── Load their CPR data for all crime-role combinations
 │   └── Get crime experience ranking (1-100) from CONFIG database
 ├── Load Available Crime Slots (from OC_DATA)
 │   ├── Get empty slots across all active crimes
 │   ├── Calculate expected value for each crime
 │   └── Determine slot priority (Robber > Hacker > Driver, etc.)
 └── Load Discord Mappings (from CONFIG)
 └── Match member IDs to Discord usernames for notifications

 └── Prioritise crimes with few slots remaining
 For each available slot:
 ├── Find members with CPR >= 60 for that specific crime-role
 ├── Score each member-slot combination based on:
 │   ├── 50% - Slot Priority (how critical the role is)
 │   ├── 30% - Member's CPR for that specific role
 │   ├── 20% - Crime's expected value
 │   └── Small bonuses for recent activity
 └── Assign highest-scoring combinations first

 For remaining unfilled slots:
 ├── Use members who didn't qualify for Pass 1 (CPR < 60)
 ├── Score based on:
 │   ├── 50% - Slot Priority
 │   ├── 30% - Crime Experience Ranking (1 = best, 100 = worst), only top 25% of faction members for crime exp are recommended to crime difficulty 7+
 │   ├── 10% - Crime Value
 │   └── 10% - Activity Bonus
 └── Assign the best available matches

 IF (members with CPR >= 60 available):
 └── Use high-quality CPR-based assignments

 IF (no members meet CPR threshold):
 └── Fall back to crime experience ranking
 └── Still fill slots with the best available members

 IF (some slots filled in Pass 1, some need Pass 2):
 └── Hybrid approach - quality where possible, coverage everywhere else

 For Each Assignment:
 ├── Generate human-readable reasoning:
 │   ├── CPR-based: "High CPR (85%), Critical priority slot"
 │   └── Fallback: "Fallback assignment (CPR < 60), Good crime experience (rank 15)"
 ├── Group by crime for organized display
 ├── Add strategic recommendations
 └── Send formatted message with member mentions

 */

public class CrimeAssignmentOptimizer {

    private static final Logger logger = GetAvailableCrimes.logger;

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

        @Override
        public String toString() {
            return String.format("FactionInfo{factionId='%s', dbSuffix='%s'}", factionId, dbSuffix);
        }
    }

    public static class AvailableMember {
        private final String userId;
        private final String username;
        private final Map<String, Integer> crimeSlotCPR;
        private final Timestamp lastJoinedCrimeDate;
        private final int crimeExpRank; // Added crime experience rank (1 = best, 100 = worst)

        public AvailableMember(String userId, String username, Map<String, Integer> crimeSlotCPR,
                               Timestamp lastJoinedCrimeDate, int crimeExpRank) {
            this.userId = userId;
            this.username = username;
            this.crimeSlotCPR = crimeSlotCPR != null ? crimeSlotCPR : new HashMap<>();
            this.lastJoinedCrimeDate = lastJoinedCrimeDate;
            this.crimeExpRank = crimeExpRank;
        }

        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public Map<String, Integer> getCrimeSlotCPR() { return crimeSlotCPR; }
        public Timestamp getLastJoinedCrimeDate() { return lastJoinedCrimeDate; }
        public int getCrimeExpRank() { return crimeExpRank; }

        public Integer getCPRForSlot(String crimeName, String slotName) {
            String key = crimeName + "|" + slotName;
            return crimeSlotCPR.getOrDefault(key, 0);
        }

        @Override
        public String toString() {
            return String.format("AvailableMember{userId='%s', username='%s', cprEntries=%d, crimeExpRank=%d}",
                    userId, username, crimeSlotCPR.size(), crimeExpRank);
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
        private final Integer totalSlots;        // Total slots for this crime type (from all_oc_crimes)
        private final Integer availableSlots;    // Number of available slots for this specific crime
        private final Integer filledSlots;       // Calculated: totalSlots - availableSlots
        private final Double completionPriority; // Calculated priority based on completion status

        public AvailableCrimeSlot(Long crimeId, String crimeName, String slotPosition, String slotPositionId,
                                  Integer crimeDifficulty, Timestamp expiredAt, Long expectedValue, Double slotPriority,
                                  Integer totalSlots, Integer availableSlots) {
            this.crimeId = crimeId;
            this.crimeName = crimeName;
            this.slotPosition = slotPosition;
            this.slotPositionId = slotPositionId;
            this.crimeDifficulty = crimeDifficulty;
            this.expiredAt = expiredAt;
            this.expectedValue = expectedValue != null ? expectedValue : 0L;
            this.slotPriority = slotPriority != null ? slotPriority : 1.0;
            this.totalSlots = totalSlots != null ? totalSlots : 6; // Default assumption
            this.availableSlots = availableSlots != null ? availableSlots : 1;
            this.filledSlots = this.totalSlots - this.availableSlots;
            this.completionPriority = calculateCompletionPriority(this.totalSlots, this.filledSlots);
        }

        public Long getCrimeId() { return crimeId; }
        public String getCrimeName() { return crimeName; }
        public String getSlotPosition() { return slotPosition; }
        public String getSlotPositionId() { return slotPositionId; }
        public Integer getCrimeDifficulty() { return crimeDifficulty; }
        public Timestamp getExpiredAt() { return expiredAt; }
        public Long getExpectedValue() { return expectedValue; }
        public Double getSlotPriority() { return slotPriority; }
        public Integer getTotalSlots() { return totalSlots; }
        public Integer getAvailableSlots() { return availableSlots; }
        public Integer getFilledSlots() { return filledSlots; }
        public Double getCompletionPriority() { return completionPriority; }

        public String getSlotKey() {
            return crimeId + "|" + slotPositionId;
        }

        @Override
        public String toString() {
            return String.format("AvailableCrimeSlot{crimeId=%d, crimeName='%s', slotPosition='%s', expectedValue=%d}",
                    crimeId, crimeName, slotPosition, expectedValue);
        }

        /**
         * Calculate completion priority based on how close the crime is to being full
         * Higher values = higher priority
         */
        private static Double calculateCompletionPriority(Integer totalSlots, Integer filledSlots) {
            if (totalSlots == null || totalSlots <= 0 || filledSlots == null || filledSlots < 0) {
                return 1.0; // Default priority for unknown
            }

            if (filledSlots >= totalSlots) {
                return 0.0; // Crime is already full, no priority
            }

            double completionRatio = (double) filledSlots / totalSlots;

            // Progressive priority boost as crime gets closer to completion
            if (completionRatio >= 0.75) {        // 75%+ filled
                return 3.0; // Very high priority - almost complete
            } else if (completionRatio >= 0.5) {  // 50-74% filled
                return 2.5; // High priority - halfway there
            } else if (completionRatio >= 0.25) { // 25-49% filled
                return 2.0; // Medium-high priority - getting started
            } else if (completionRatio > 0) {     // 1-24% filled
                return 1.5; // Medium priority - has some progress
            } else {                              // 0% filled
                return 1.0; // Base priority - new crime
            }
        }

        /**
         * Get human-readable completion status
         */
        public String getCompletionStatus() {
            if (totalSlots == null || filledSlots == null) {
                return "Unknown progress";
            }

            return String.format("%d/%d slots filled (%.0f%%)",
                    filledSlots, totalSlots, (filledSlots * 100.0 / totalSlots));
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

    public static class AvailableMembersResult {
        private final List<AvailableMember> members;
        private final int totalFactionMembers;

        public AvailableMembersResult(List<AvailableMember> members, int totalFactionMembers) {
            this.members = members;
            this.totalFactionMembers = totalFactionMembers;
        }

        public List<AvailableMember> getMembers() { return members; }
        public int getTotalFactionMembers() { return totalFactionMembers; }
    }

    /**
     * Main entry point for optimizing crime assignments for all factions
     * Enhanced with comprehensive debugging
     */
    public static void optimizeAllFactionsCrimeAssignments() throws SQLException {
        logger.info("==================== STARTING CRIME ASSIGNMENT OPTIMIZATION ====================");
        logger.info("Starting crime assignment optimization for all factions (urgency-free algorithm)");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            logger.error("CRITICAL: DATABASE_URL_CONFIG environment variable not set");
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            logger.error("CRITICAL: DATABASE_URL_OC_DATA environment variable not set");
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Database URLs loaded - CONFIG: {}, OC_DATA: {}",
                maskDatabaseUrl(configDatabaseUrl), maskDatabaseUrl(ocDataDatabaseUrl));

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("✓ Database connections established successfully");

            logger.info("Loading faction information...");
            List<FactionInfo> factions = getFactionInfo(configConnection);

            if (factions.isEmpty()) {
                logger.warn("WARNING: No OC2-enabled factions found for optimization");
                return;
            }

            logger.info("✓ Found {} OC2-enabled factions to optimize", factions.size());
            for (int i = 0; i < factions.size(); i++) {
                FactionInfo faction = factions.get(i);
                logger.info("  {}. Faction ID: {}, DB Suffix: {}", i + 1, faction.getFactionId(), faction.getDbSuffix());
            }

            int processedCount = 0;
            int successfulCount = 0;
            int failedCount = 0;

            for (FactionInfo factionInfo : factions) {
                logger.info("==================== PROCESSING FACTION {}/{} ====================",
                        processedCount + 1, factions.size());
                logger.info("Faction: {}", factionInfo);

                try {
                    logger.info("Starting optimization for faction: {} (Progress: {}/{})",
                            factionInfo.getFactionId(), processedCount + 1, factions.size());

                    OptimizationResult result = optimizeFactionCrimeAssignments(
                            configConnection, ocDataConnection, factionInfo);

                    if (result != null) {
                        logger.info("✓ Optimization completed for faction {} - {} assignments (score: %.3f)",
                                factionInfo.getFactionId(), result.getAssignments().size(), result.getTotalScore());

                        // Log detailed results
                        logger.info("  Result summary: {}", result);

                        // Log top assignments
                        if (!result.getAssignments().isEmpty()) {
                            logger.info("  Top assignments for faction {}:", factionInfo.getFactionId());
                            result.getAssignments().stream()
                                    .limit(5) // Show top 5 assignments
                                    .forEach(assignment -> logger.info("    → {}", assignment));

                            if (result.getAssignments().size() > 5) {
                                logger.info("    → ... and {} more assignments", result.getAssignments().size() - 5);
                            }
                        } else {
                            logger.info("  No assignments generated for faction {}", factionInfo.getFactionId());
                        }

                        successfulCount++;
                    } else {
                        logger.warn("✗ No optimization result returned for faction {}", factionInfo.getFactionId());
                        failedCount++;
                    }

                    processedCount++;

                } catch (Exception e) {
                    logger.error("✗ EXCEPTION: Error optimizing faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                    failedCount++;
                    processedCount++; // Still count as processed
                }

                logger.info("==================== COMPLETED FACTION {} ====================",
                        factionInfo.getFactionId());
            }

            // Final summary
            logger.info("==================== OPTIMIZATION SUMMARY ====================");
            logger.info("Crime assignment optimization completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful: {}", successfulCount);
            logger.info("  Failed: {}", failedCount);
            logger.info("  Success rate: %.1f%", processedCount > 0 ? (successfulCount * 100.0 / processedCount) : 0.0);

        } catch (SQLException e) {
            logger.error("CRITICAL: Database error during crime assignment optimization", e);
            throw e;
        }

        logger.info("==================== OPTIMIZATION PROCESS COMPLETE ====================");
    }

    /**
     * Load total slots for each crime type from CONFIG database
     */
    private static Map<String, Integer> loadCrimeTotalSlots(Connection configConnection) throws SQLException {
        Map<String, Integer> crimeTotalSlots = new HashMap<>();

        String sql = "SELECT crime_name, total_slots FROM all_oc_crimes";

        logger.debug("Loading crime total slots from CONFIG database: {}", sql);

        try (PreparedStatement stmt = configConnection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String crimeName = rs.getString("crime_name");
                Integer totalSlots = rs.getObject("total_slots", Integer.class);

                if (crimeName != null && totalSlots != null) {
                    crimeTotalSlots.put(crimeName, totalSlots);
                    logger.debug("Loaded total slots for {}: {}", crimeName, totalSlots);
                }
            }

        } catch (SQLException e) {
            logger.warn("Could not load crime total slots from CONFIG database: {}", e.getMessage());
            // Don't throw - we can still process with default values
        }

        logger.info("✓ Loaded total slots data for {} crime types", crimeTotalSlots.size());
        return crimeTotalSlots;
    }

    /**
     * Enhanced optimizeFactionCrimeAssignments method to handle unassigned members
     * Add this after the existing optimization logic
     */
    private static OptimizationResult optimizeFactionCrimeAssignments(Connection configConnection,
                                                                      Connection ocDataConnection,
                                                                      FactionInfo factionInfo) throws SQLException {

        logger.info("--- Starting detailed optimization for faction {} ---", factionInfo.getFactionId());

        try {
            // Step 1: Load available members with their CPR data and crime exp rank
            logger.info("STEP 1: Loading available members for faction {}", factionInfo.getFactionId());
            AvailableMembersResult membersResult = loadAvailableMembers(configConnection, ocDataConnection, factionInfo);
            List<AvailableMember> availableMembers = membersResult.getMembers();
            int totalFactionMembers = membersResult.getTotalFactionMembers();
            logger.info("STEP 1 RESULT: Found {} available members out of {} total for faction {}",
                    availableMembers.size(), totalFactionMembers, factionInfo.getFactionId());

            // Step 2: Load available crime slots with expected values
            logger.info("STEP 2: Loading available crime slots for faction {}", factionInfo.getFactionId());
            List<AvailableCrimeSlot> availableSlots = loadAvailableCrimeSlots(configConnection, ocDataConnection, factionInfo);
            logger.info("STEP 2 RESULT: Found {} available crime slots for faction {}",
                    availableSlots.size(), factionInfo.getFactionId());

            // Early exit conditions
            if (availableMembers.isEmpty() && availableSlots.isEmpty()) {
                logger.info("EARLY EXIT: Faction {} has no available members AND no available slots", factionInfo.getFactionId());
                return new OptimizationResult(new ArrayList<>(), 0.0, 0, 0);
            } else if (availableMembers.isEmpty()) {
                logger.info("EARLY EXIT: Faction {} has no available members (all busy or none exist)", factionInfo.getFactionId());
                return new OptimizationResult(new ArrayList<>(), 0.0, availableSlots.size(), 0);
            } else if (availableSlots.isEmpty()) {
                logger.info("EARLY EXIT: Faction {} has no available crime slots but has {} available members",
                        factionInfo.getFactionId(), availableMembers.size());

                // NEW: Handle case where there are available members but no crime slots
                handleUnassignedMembers(configConnection, factionInfo, availableMembers);

                return new OptimizationResult(new ArrayList<>(), 0.0, 0, availableMembers.size());
            }

            // Step 3: Run optimization
            logger.info("STEP 3: Running optimization algorithm - {} members to {} slots for faction {}",
                    availableMembers.size(), availableSlots.size(), factionInfo.getFactionId());

            // Log sample data for debugging
            if (logger.isDebugEnabled()) {
                logger.debug("Sample available members for faction {}:", factionInfo.getFactionId());
                availableMembers.stream().limit(3).forEach(member -> logger.debug("  {}", member));

                logger.debug("Sample available slots for faction {}:", factionInfo.getFactionId());
                availableSlots.stream().limit(3).forEach(slot -> logger.debug("  {}", slot));
            }

            logger.info("PRE-OPTIMIZATION DEBUG for faction {}:", factionInfo.getFactionId());
            logger.info("  Members with CPR >= 60: {}",
                    availableMembers.stream()
                            .mapToLong(member -> member.getCrimeSlotCPR().values().stream().filter(cpr -> cpr >= 60).count())
                            .sum());
            logger.info("  Total CPR entries across all members: {}",
                    availableMembers.stream().mapToInt(member -> member.getCrimeSlotCPR().size()).sum());

            OptimizationResult result = optimizeAssignments(availableMembers, availableSlots, totalFactionMembers);

            logger.info("STEP 3 RESULT: Optimization completed for faction {} - {}", factionInfo.getFactionId(), result);

            // NEW: Check for unassigned members after optimization
            if (result.getUnassignedMembers() > 0) {
                logger.info("STEP 4: Found {} unassigned members for faction {} - checking for crime spawning need",
                        result.getUnassignedMembers(), factionInfo.getFactionId());

                // Get the list of unassigned members
                List<AvailableMember> unassignedMembers = getUnassignedMembers(availableMembers, result.getAssignments());

                if (!unassignedMembers.isEmpty()) {
                    handleUnassignedMembers(configConnection, factionInfo, unassignedMembers);
                }
            }

            return result;

        } catch (SQLException e) {
            logger.error("SQL ERROR: Failed to optimize assignments for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("GENERAL ERROR: Unexpected error optimizing faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            throw new SQLException("Optimization failed for faction " + factionInfo.getFactionId(), e);
        }
    }

    /**
     * Handle unassigned members by notifying about crime spawning needs
     */
    private static void handleUnassignedMembers(Connection configConnection,
                                                FactionInfo factionInfo,
                                                List<AvailableMember> unassignedMembers) {
        try {
            logger.info("UNASSIGNED MEMBERS: Faction {} has {} members who could not be assigned to crimes",
                    factionInfo.getFactionId(), unassignedMembers.size());

            // Log details about unassigned members for debugging
            if (logger.isDebugEnabled()) {
                logger.debug("Unassigned members for faction {}:", factionInfo.getFactionId());
                unassignedMembers.forEach(member ->
                        logger.debug("  - {} (ExpRank: {}, CPR entries: {})",
                                member.getUsername(), member.getCrimeExpRank(), member.getCrimeSlotCPR().size())
                );
            }

            // Call the method to send crime spawning notification
            logger.info("NOTIFICATION: Sending 'need crimes to spawn' notification for faction {} with {} unassigned members",
                    factionInfo.getFactionId(), unassignedMembers.size());

            sendNeedCrimesToSpawnForAvailableMembers(configConnection, factionInfo, unassignedMembers);

            logger.info("✓ Successfully sent crime spawning notification for faction {}", factionInfo.getFactionId());

        } catch (Exception e) {
            logger.error("ERROR: Failed to handle unassigned members for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            // Don't throw - this is supplementary functionality, main optimization should continue
        }
    }

    /**
     * Get list of members who were not assigned in the optimization result
     */
    private static List<AvailableMember> getUnassignedMembers(List<AvailableMember> allMembers,
                                                              List<MemberSlotAssignment> assignments) {
        // Get set of assigned member user IDs
        Set<String> assignedMemberIds = assignments.stream()
                .map(assignment -> assignment.getMember().getUserId())
                .collect(Collectors.toSet());

        // Return members who are not in the assigned set
        return allMembers.stream()
                .filter(member -> !assignedMemberIds.contains(member.getUserId()))
                .collect(Collectors.toList());
    }

    /**
     * Send notification about needing to spawn more crimes for available members
     * Uses the existing sendNeedCrimesToSpawnForAvailableMembers method
     */
    private static void sendNeedCrimesToSpawnForAvailableMembers(Connection configConnection,
                                                                 FactionInfo factionInfo,
                                                                 List<AvailableMember> unassignedMembers) {
        try {
            logger.info("=== CRIME SPAWNING NOTIFICATION ===");
            logger.info("Faction {} needs more crimes spawned for {} available members",
                    factionInfo.getFactionId(), unassignedMembers.size());

            // Create summary of unassigned members by experience level for logging
            Map<String, List<AvailableMember>> membersByExperience = categorizeUnassignedMembers(unassignedMembers);

            // Log member categories for debugging
            membersByExperience.forEach((category, members) -> {
                logger.info("  {}: {} members", category, members.size());
                if (logger.isDebugEnabled()) {
                    members.forEach(member -> logger.debug("    - {} (ExpRank: {})",
                            member.getUsername(), member.getCrimeExpRank()));
                }
            });

            // Call the existing Discord notification method
            logger.info("Sending Discord crime spawning notification for faction {}", factionInfo.getFactionId());

            // Note: The existing method signature is sendNeedCrimesToSpawnForAvailableMembers(String factionId)
            // Import the class containing this method (likely DiscordMessages or similar)
            boolean success = DiscordMessages.sendNeedCrimesToSpawnForAvailableMembers(factionInfo.getFactionId());

            if (success) {
                logger.info("✓ Successfully sent crime spawning Discord notification for faction {}",
                        factionInfo.getFactionId());
            } else {
                logger.warn("✗ Failed to send crime spawning Discord notification for faction {}",
                        factionInfo.getFactionId());
            }

        } catch (Exception e) {
            logger.error("Failed to send crime spawning notification for faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            // Don't throw - this is supplementary functionality, main optimization should continue
        }
    }

    /**
     * Categorize unassigned members by their experience/capability level
     */
    private static Map<String, List<AvailableMember>> categorizeUnassignedMembers(List<AvailableMember> unassignedMembers) {
        Map<String, List<AvailableMember>> categories = new LinkedHashMap<>();

        // Initialize categories
        categories.put("Elite (Rank 1-10)", new ArrayList<>());
        categories.put("Experienced (Rank 11-30)", new ArrayList<>());
        categories.put("Capable (Rank 31-60)", new ArrayList<>());
        categories.put("Developing (Rank 61+)", new ArrayList<>());

        // Categorize members based on crime experience ranking
        for (AvailableMember member : unassignedMembers) {
            int rank = member.getCrimeExpRank();

            if (rank <= 10) {
                categories.get("Elite (Rank 1-10)").add(member);
            } else if (rank <= 30) {
                categories.get("Experienced (Rank 11-30)").add(member);
            } else if (rank <= 60) {
                categories.get("Capable (Rank 31-60)").add(member);
            } else {
                categories.get("Developing (Rank 61+)").add(member);
            }
        }

        // Remove empty categories
        categories.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        return categories;
    }


    /**
     * Core optimization algorithm using weighted scoring and Hungarian-like assignment
     */
    private static OptimizationResult optimizeAssignments(List<AvailableMember> members,
                                                          List<AvailableCrimeSlot> slots,
                                                          int totalFactionMembers) {

        logger.debug("Running optimization algorithm with {} members and {} slots", members.size(), slots.size());

        // Create scoring matrix
        double[][] scoreMatrix = createScoringMatrix(members, slots);

        // Apply Hungarian algorithm variant for optimal assignment
        List<MemberSlotAssignment> assignments = hungarianAssignment(members, slots, scoreMatrix, totalFactionMembers);

        // Calculate total score and statistics
        double totalScore = assignments.stream().mapToDouble(MemberSlotAssignment::getOptimizationScore).sum();
        int unfilledSlots = slots.size() - assignments.size();
        int unassignedMembers = members.size() - assignments.size();

        // Sort assignments by score (highest first)
        assignments.sort((a, b) -> Double.compare(b.getOptimizationScore(), a.getOptimizationScore()));

        logger.debug("Optimization algorithm completed: {} assignments, total score: %.3f",
                assignments.size(), totalScore);

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
     * Enhanced calculateMemberSlotScore method to include completion priority
     * Replace the existing method with this enhanced version
     */
    private static double calculateMemberSlotScore(AvailableMember member, AvailableCrimeSlot slot) {
        // Factor 1: Member's CPR for this crime-slot (0-100, normalized to 0-1)
        Integer memberCPR = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());
        double cprScore = (memberCPR != null ? memberCPR : 0) / 100.0;

        // Factor 2: Crime value (normalized relative to maximum expected value)
        double valueScore = Math.min(slot.getExpectedValue() / 50_000_000.0, 1.0); // Cap at 50M

        // Factor 3: Slot priority within the crime
        double priorityScore = Math.min(slot.getSlotPriority(), 2.0) / 2.0; // Normalize to 0-1

        // Factor 4: Member activity bonus
        double activityScore = calculateActivityScore(member);

        // Factor 5: Crime experience ranking
        double crimeExpScore = (101 - member.getCrimeExpRank()) / 100.0;

        // Factor 6: Completion priority (prioritize partially filled crimes)
        double completionScore = Math.min(slot.getCompletionPriority(), 3.0) / 3.0; // Normalize to 0-1

        // Enhanced weighted combination with completion priority
        double totalScore = (cprScore * CPR_WEIGHT) +                    // 30%
                (valueScore * CRIME_VALUE_WEIGHT) +           // 20%
                (priorityScore * SLOT_PRIORITY_WEIGHT) +      // 50%
                (activityScore * 0.05) +                     // 5%
                (crimeExpScore * 0.05) +                     // 5%
                (completionScore * 0.15);                    // 15% - completion priority

        // Adjust total weights to ensure they sum to reasonable values
        // Total current weights: 30% + 20% + 50% + 5% + 5% + 15% = 125%
        // Normalize back to 100% by dividing by 1.25
        totalScore = totalScore / 1.25;

        // ADD THIS DEBUG LOGGING HERE:
        if (memberCPR != null && memberCPR >= 60) { // Only log for members that should qualify
            logger.debug("SCORE DEBUG: {} -> {} - CPR:{} CompletionPriority:%.2f FinalScore:%.3f (cpr:%.3f value:%.3f priority:%.3f completion:%.3f)",
                    member.getUsername(), slot.getCrimeName(), memberCPR, slot.getCompletionPriority(),
                    totalScore, cprScore, valueScore, priorityScore, completionScore);
        }

        // Log reasoning for high-priority assignments
        if (slot.getCompletionPriority() > 2.0 && logger.isDebugEnabled()) {
            logger.debug("High completion priority assignment: {} to {} ({}) - {} - Completion Priority: %.2f",
                    member.getUsername(), slot.getCrimeName(), slot.getSlotPosition(),
                    slot.getCompletionStatus(), slot.getCompletionPriority());
        }

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
     * Hungarian algorithm variant for optimal assignment with CPR threshold and crime exp fallback
     * Now includes two-pass approach: first pass with CPR >= 60, second pass with crime exp ranking fallback
     */
    private static List<MemberSlotAssignment> hungarianAssignment(List<AvailableMember> members,
                                                                  List<AvailableCrimeSlot> slots,
                                                                  double[][] scoreMatrix,
                                                                  int totalFactionMembers) {
        List<MemberSlotAssignment> assignments = new ArrayList<>();
        boolean[] memberUsed = new boolean[members.size()];
        boolean[] slotUsed = new boolean[slots.size()];

        // Minimum CPR threshold for role assignment
        final int MIN_CPR_THRESHOLD = 60;

        logger.info("Starting assignment process: {} members, {} slots", members.size(), slots.size());

        // PASS 1: CPR-based assignments (CPR >= 60)
        logger.info("PASS 1: Assigning members with CPR >= {}", MIN_CPR_THRESHOLD);
        int pass1Assignments = performAssignmentPass(members, slots, scoreMatrix, memberUsed, slotUsed,
                assignments, MIN_CPR_THRESHOLD, true, totalFactionMembers);
        logger.info("PASS 1 COMPLETE: {} assignments made with CPR >= {}", pass1Assignments, MIN_CPR_THRESHOLD);

        // PASS 2: Crime experience ranking fallback for remaining slots
        int remainingSlots = countRemainingSlots(slotUsed);
        int remainingMembers = countRemainingMembers(memberUsed);


        // PASS 2: Crime experience ranking fallback for remaining slots
        if (remainingSlots > 0 && remainingMembers > 0) {
            logger.info("PASS 2: Using crime experience ranking fallback for {} remaining slots with {} remaining members",
                    remainingSlots, remainingMembers);
            int pass2Assignments = performAssignmentPass(members, slots, scoreMatrix, memberUsed, slotUsed,
                    assignments, MIN_CPR_THRESHOLD, false, totalFactionMembers);
            logger.info("PASS 2 COMPLETE: {} additional assignments made using crime experience ranking", pass2Assignments);
        }

        // Log final summary
        int totalSlots = slots.size();
        int assignedSlots = assignments.size();
        int unassignedSlots = totalSlots - assignedSlots;

        logger.info("ASSIGNMENT SUMMARY:");
        logger.info("  Total slots: {}", totalSlots);
        logger.info("  Assigned slots: {} (%.1f%)", assignedSlots, (assignedSlots * 100.0 / totalSlots));
        logger.info("  Pass 1 (CPR >= {}): {}", MIN_CPR_THRESHOLD, pass1Assignments);
        logger.info("  Pass 2 (Exp Rank): {}", assignedSlots - pass1Assignments);
        logger.info("  Unassigned slots: {}", unassignedSlots);

        return assignments;
    }

    /**
     * Get total member count for a faction from CONFIG database
     */
    private static int getTotalFactionMemberCount(Connection configConnection, String membersTable) {
        String sql = "SELECT COUNT(*) FROM " + membersTable;

        try (PreparedStatement stmt = configConnection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.debug("Could not get total member count from table {}: {}", membersTable, e.getMessage());
        }

        return 100; // Default fallback
    }

    /**
     * Check if a member is in the top 25% based on their crime experience ranking
     */
    private static boolean isInTop25Percent(AvailableMember member, int totalFactionMembers) {
        // Crime exp rank: 1 = best, 100 = worst
        // Top 25% means their rank should be <= 25% of total members
        int top25PercentThreshold = Math.max(1, (int) Math.ceil(totalFactionMembers * 0.25));
        return member.getCrimeExpRank() <= top25PercentThreshold;
    }

    /**
     * Perform a single assignment pass (either CPR-based or experience ranking-based)
     */
    private static int performAssignmentPass(List<AvailableMember> members,
                                             List<AvailableCrimeSlot> slots,
                                             double[][] scoreMatrix,
                                             boolean[] memberUsed,
                                             boolean[] slotUsed,
                                             List<MemberSlotAssignment> assignments,
                                             int minCprThreshold,
                                             boolean useCprFilter,
                                             int totalFactionMembers) {
        int assignmentsMade = 0;

        // Greedy approach: repeatedly find the highest scoring available combination
        while (true) {
            int bestMember = -1;
            int bestSlot = -1;
            double bestScore = -1.0;

            // Find the best available member-slot combination for this pass
            for (int m = 0; m < members.size(); m++) {
                if (memberUsed[m]) continue;

                for (int s = 0; s < slots.size(); s++) {
                    if (slotUsed[s]) continue;

                    AvailableMember member = members.get(m);
                    AvailableCrimeSlot slot = slots.get(s);
                    Integer memberCPR = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());

                    // Apply filtering based on pass type
                    if (useCprFilter) {
                        // Pass 1: Skip if CPR is below threshold
                        if (memberCPR == null || memberCPR < minCprThreshold) {
                            continue;
                        }
                        // Use normal scoring for pass 1
                        if (scoreMatrix[m][s] > bestScore) {
                            bestScore = scoreMatrix[m][s];
                            bestMember = m;
                            bestSlot = s;

                            // ADD THIS DEBUG LOGGING HERE:
                            logger.debug("NEW BEST (Pass1): {} -> {} - CPR:{} Score:%.3f CompletionPriority:%.2f",
                                    member.getUsername(), slot.getCrimeName(), memberCPR, bestScore, slot.getCompletionPriority());
                        }
                    } else {
                        // Pass 2: Skip if CPR is above threshold (already handled in pass 1)
                        if (memberCPR != null && memberCPR >= minCprThreshold) {
                            continue;
                        }

                        // NEW: For high-difficulty crimes (7+), only allow top 25% members
                        if (slot.getCrimeDifficulty() != null && slot.getCrimeDifficulty() >= 7) {
                            if (!isInTop25Percent(member, totalFactionMembers)) {
                                logger.debug("Skipping member {} for high-difficulty crime {} (difficulty {}): not in top 25% (rank {} of {})",
                                        member.getUsername(), slot.getCrimeName(), slot.getCrimeDifficulty(),
                                        member.getCrimeExpRank(), totalFactionMembers);
                                continue;
                            }
                        }

                        // For pass 2, we use crime exp ranking, so we need a different scoring approach
                        double expRankScore = calculateCrimeExpScore(member, slot);
                        if (expRankScore > bestScore) {
                            bestScore = expRankScore;
                            bestMember = m;
                            bestSlot = s;
                        }
                    }
                }
            }

            // ADD THIS DEBUG LOGGING HERE:
            if (bestMember == -1) {
                logger.info("ASSIGNMENT PASS DEBUG: No valid member found for any slot in pass {}", useCprFilter ? 1 : 2);
            } else if (bestScore < 0.1) {
                logger.info("ASSIGNMENT PASS DEBUG: Best score %.3f below threshold 0.1 in pass {}", bestScore, useCprFilter ? 1 : 2);
            } else {
                logger.debug("ASSIGNMENT PASS DEBUG: Found assignment {} -> {} with score %.3f",
                        members.get(bestMember).getUsername(), slots.get(bestSlot).getCrimeName(), bestScore);
            }

            // No more good assignments found for this pass
            if (bestMember == -1 || bestScore < 0.1) { // Minimum threshold
                break;
            }

            // Make the assignment
            AvailableMember member = members.get(bestMember);
            AvailableCrimeSlot slot = slots.get(bestSlot);
            Integer memberCPR = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());

            String reasoning = generateAssignmentReasoning(member, slot, bestScore, useCprFilter);
            assignments.add(new MemberSlotAssignment(member, slot, bestScore, reasoning));

            String assignmentType = useCprFilter ? "CPR-based" : "Exp Rank fallback";
            logger.debug("Assigned ({}): {} to {} ({}) - CPR: {}, ExpRank: {}",
                    assignmentType, member.getUsername(), slot.getCrimeName(), slot.getSlotPosition(),
                    memberCPR != null ? memberCPR : "N/A", member.getCrimeExpRank());

            memberUsed[bestMember] = true;
            slotUsed[bestSlot] = true;
            assignmentsMade++;
        }

        return assignmentsMade;
    }

    /**
     * Calculate crime experience score for fallback assignments
     */
    private static double calculateCrimeExpScore(AvailableMember member, AvailableCrimeSlot slot) {
        // Base score from crime experience ranking (1 = best, 100 = worst)
        double expRankScore = (101 - member.getCrimeExpRank()) / 100.0; // Convert to 0-1 where 1 is best

        // Factor in slot priority
        double priorityScore = Math.min(slot.getSlotPriority(), 2.0) / 2.0;

        // Factor in crime value
        double valueScore = Math.min(slot.getExpectedValue() / 50_000_000.0, 1.0);

        // Activity bonus
        double activityScore = calculateActivityScore(member);

        // Weighted combination for fallback scoring
        return (expRankScore * 0.3) + (priorityScore * 0.5) + (valueScore * 0.1) + (activityScore * 0.1);
    }

    /**
     * Count remaining unused slots
     */
    private static int countRemainingSlots(boolean[] slotUsed) {
        int count = 0;
        for (boolean used : slotUsed) {
            if (!used) count++;
        }
        return count;
    }

    /**
     * Count remaining unused members
     */
    private static int countRemainingMembers(boolean[] memberUsed) {
        int count = 0;
        for (boolean used : memberUsed) {
            if (!used) count++;
        }
        return count;
    }

    /**
     * Enhanced generateAssignmentReasoning to include completion status
     * Replace the existing method with this enhanced version
     */
    private static String generateAssignmentReasoning(AvailableMember member, AvailableCrimeSlot slot,
                                                      double score, boolean isCprBased) {
        List<String> reasons = new ArrayList<>();

        Integer cpr = member.getCPRForSlot(slot.getCrimeName(), slot.getSlotPosition());
        int expRank = member.getCrimeExpRank();

        // Add completion priority reasoning first (most important)
        if (slot.getCompletionPriority() >= 3.0) {
            reasons.add("🔥 URGENT: " + slot.getCompletionStatus() + " - nearly complete!");
        } else if (slot.getCompletionPriority() >= 2.5) {
            reasons.add("⚡ HIGH PRIORITY: " + slot.getCompletionStatus() + " - good progress");
        } else if (slot.getCompletionPriority() >= 2.0) {
            reasons.add("📈 ACTIVE CRIME: " + slot.getCompletionStatus() + " - building momentum");
        } else if (slot.getCompletionPriority() > 1.0) {
            reasons.add("🟡 STARTED: " + slot.getCompletionStatus() + " - has some progress");
        }

        if (isCprBased) {
            // Pass 1 reasoning (CPR-based)
            if (slot.getSlotPriority() > 2.0) {
                reasons.add("Critical role (weight: " + String.format("%.1f", slot.getSlotPriority()) + ")");
            } else if (slot.getSlotPriority() > 1.5) {
                reasons.add("Important role (weight: " + String.format("%.1f", slot.getSlotPriority()) + ")");
            }

            if (cpr != null && cpr > 80) {
                reasons.add("Excellent CPR (" + cpr + "%)");
            } else if (cpr != null && cpr > 60) {
                reasons.add("Good CPR (" + cpr + "%)");
            }

            if (slot.getExpectedValue() > 20_000_000) {
                reasons.add("High value ($" + String.format("%.1fM", slot.getExpectedValue() / 1_000_000.0) + ")");
            }
        } else {
            // Pass 2 reasoning (Experience ranking fallback)
            reasons.add("Fallback assignment (CPR < 60)");

            if (expRank <= 10) {
                reasons.add("Excellent crime experience (rank " + expRank + ")");
            } else if (expRank <= 30) {
                reasons.add("Good crime experience (rank " + expRank + ")");
            } else if (expRank <= 50) {
                reasons.add("Moderate crime experience (rank " + expRank + ")");
            }

            if (slot.getSlotPriority() > 1.5) {
                reasons.add("Important role needs filling");
            }
        }

        return reasons.isEmpty() ? "Best available match" : String.join(", ", reasons);
    }

    /**
     * Load available members with their CPR data and crime experience ranking
     * Enhanced with comprehensive debugging and error handling
     */
    private static AvailableMembersResult loadAvailableMembers(Connection configConnection,
                                                               Connection ocDataConnection,
                                                               FactionInfo factionInfo) throws SQLException {
        List<AvailableMember> members = new ArrayList<>();
        String availableMembersTable = Constants.TABLE_NAME_AVAILABLE_MEMBERS + factionInfo.getDbSuffix();
        String cprTable = Constants.TABLE_NAME_CPR + factionInfo.getDbSuffix();
        String membersTable = "members_" + factionInfo.getDbSuffix(); // From CONFIG database

        logger.info("DEBUG: Attempting to load members from OC_DATA table: {}", availableMembersTable);
        logger.info("DEBUG: Will also load CPR data from table: {}", cprTable);
        logger.info("DEBUG: Will load crime exp rank from CONFIG table: {}", membersTable);

        // Get total faction member count first
        int totalFactionMembers = getTotalFactionMemberCount(configConnection, membersTable);
        logger.info("DEBUG: Total faction member count: {}", totalFactionMembers);

        // Check if tables exist first
        try {
            verifyTableExists(ocDataConnection, availableMembersTable, "available members");
            logger.info("✓ Table {} exists and is accessible", availableMembersTable);
        } catch (SQLException e) {
            logger.error("✗ CRITICAL: Available members table {} does not exist for faction {}: {}",
                    availableMembersTable, factionInfo.getFactionId(), e.getMessage());
            throw new SQLException("Available members table missing for faction " + factionInfo.getFactionId(), e);
        }

        try {
            verifyTableExists(configConnection, membersTable, "members");
            logger.info("✓ Table {} exists in CONFIG database", membersTable);
        } catch (SQLException e) {
            logger.error("✗ CRITICAL: Members table {} does not exist in CONFIG database for faction {}: {}",
                    membersTable, factionInfo.getFactionId(), e.getMessage());
            throw new SQLException("Members table missing in CONFIG database for faction " + factionInfo.getFactionId(), e);
        }

        // First get available members from OC_DATA
        String membersSql = "SELECT user_id, username, last_joined_crime_date FROM " + availableMembersTable +
                " WHERE is_in_oc = false ORDER BY username";

        logger.debug("Executing SQL: {}", membersSql);

        try (PreparedStatement membersStmt = ocDataConnection.prepareStatement(membersSql);
             ResultSet membersRs = membersStmt.executeQuery()) {

            int memberCount = 0;
            while (membersRs.next()) {
                memberCount++;
                String userId = membersRs.getString("user_id");
                String username = membersRs.getString("username");
                Timestamp lastJoinedCrimeDate = membersRs.getTimestamp("last_joined_crime_date");

                logger.debug("Loading member {}: {} ({})", memberCount, username, userId);

                // Load CPR data for this member
                Map<String, Integer> cprData = loadMemberCPRData(ocDataConnection, cprTable, userId);
                logger.debug("  Loaded {} CPR entries for member {}", cprData.size(), username);

                // Load crime experience rank from CONFIG database
                int crimeExpRank = loadMemberCrimeExpRank(configConnection, membersTable, userId);
                logger.debug("  Crime experience rank for member {}: {}", username, crimeExpRank);

                members.add(new AvailableMember(userId, username, cprData, lastJoinedCrimeDate, crimeExpRank));
            }

            logger.info("✓ Successfully loaded {} members from table {} for faction {}",
                    memberCount, availableMembersTable, factionInfo.getFactionId());

            if (memberCount == 0) {
                logger.warn("WARNING: No available members found for faction {} (all may be in crimes or table empty)",
                        factionInfo.getFactionId());
            }

        } catch (SQLException e) {
            logger.error("✗ ERROR: Failed to load available members for faction {} from table {}: {}",
                    factionInfo.getFactionId(), availableMembersTable, e.getMessage());
            throw e;
        }

        return new AvailableMembersResult(members, totalFactionMembers);
    }



    /**
     * Load crime experience rank for a specific member from CONFIG database
     */
    private static int loadMemberCrimeExpRank(Connection configConnection, String membersTable, String userId) {
        String sql = "SELECT crime_exp_rank FROM " + membersTable + " WHERE user_id = ?";

        try (PreparedStatement stmt = configConnection.prepareStatement(sql)) {
            stmt.setString(1, userId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Integer rank = rs.getObject("crime_exp_rank", Integer.class);
                    return rank != null ? rank : 100; // Default to 100 if null
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not load crime exp rank for user {} from table {}: {}",
                    userId, membersTable, e.getMessage());
        }

        return 100; // Default rank if not found or error
    }

    /**
     * Load CPR data for a specific member
     * Enhanced with debugging
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
                } else {
                    logger.debug("No CPR data found for user {} in table {}", userId, cprTable);
                }
            }
        } catch (SQLException e) {
            logger.debug("Could not load CPR data for user {} from table {}: {}",
                    userId, cprTable, e.getMessage());
            // Don't throw - member can still be processed without CPR data
        }

        return cprData;
    }

    /**
     * Convert database column name back to "CrimeName|SlotName" format
     */
    private static String convertColumnNameToCrimeSlotKey(String columnName) {
        String[] parts = columnName.split("_-_", 2);
        if (parts.length == 2) {
            String crimeAbbrev = parts[0].toUpperCase();
            String slotName = parts[1].replace("_", " ");

            return crimeAbbrev + "|" + capitalizeWords(slotName);
        }

        return columnName; // Fallback
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
     * Enhanced loadAvailableCrimeSlots method to calculate completion data
     * Replace the existing method with this enhanced version
     */
    private static List<AvailableCrimeSlot> loadAvailableCrimeSlots(Connection configConnection,
                                                                    Connection ocDataConnection,
                                                                    FactionInfo factionInfo) throws SQLException {
        List<AvailableCrimeSlot> slots = new ArrayList<>();
        String availableCrimesTable = "a_crimes_" + factionInfo.getDbSuffix();

        logger.info("DEBUG: Loading crime slots with completion data from table: {}", availableCrimesTable);

        // Check if table exists
        try {
            verifyTableExists(ocDataConnection, availableCrimesTable, "available crimes");
            logger.info("✓ Table {} exists and is accessible", availableCrimesTable);
        } catch (SQLException e) {
            logger.error("✗ CRITICAL: Available crimes table {} does not exist for faction {}: {}",
                    availableCrimesTable, factionInfo.getFactionId(), e.getMessage());
            throw new SQLException("Available crimes table missing for faction " + factionInfo.getFactionId(), e);
        }

        // Load supporting data
        Map<String, CrimeRewardData> crimeRewards = loadCrimeRewardsFromConfig(configConnection);
        Map<String, Double> rolePriorities = loadRolePrioritiesFromConfig(configConnection);
        Map<String, Integer> crimeTotalSlots = loadCrimeTotalSlots(configConnection);

        // First, count available slots per crime to calculate completion status
        String countSql = "SELECT " +
                "crime_id, " +
                "name, " +
                "COUNT(*) as available_slot_count " +
                "FROM " + availableCrimesTable + " " +
                "GROUP BY crime_id, name";

        Map<Long, Integer> crimeAvailableSlotCounts = new HashMap<>();
        Map<Long, String> crimeNames = new HashMap<>();

        logger.debug("Counting available slots per crime: {}", countSql);

        try (PreparedStatement countStmt = ocDataConnection.prepareStatement(countSql);
             ResultSet countRs = countStmt.executeQuery()) {

            while (countRs.next()) {
                Long crimeId = countRs.getLong("crime_id");
                String crimeName = countRs.getString("name");
                Integer availableCount = countRs.getInt("available_slot_count");

                crimeAvailableSlotCounts.put(crimeId, availableCount);
                crimeNames.put(crimeId, crimeName);
            }
        }

        // Now load individual slots with completion data
        String slotsSql = "SELECT " +
                "crime_id, " +
                "name, " +
                "difficulty, " +
                "expired_at, " +
                "slot_position, " +
                "slot_position_id " +
                "FROM " + availableCrimesTable + " " +
                "ORDER BY crime_id, slot_position_id";


        logger.debug("Loading individual slots: {}", slotsSql);

        try (PreparedStatement slotsStmt = ocDataConnection.prepareStatement(slotsSql);
             ResultSet slotsRs = slotsStmt.executeQuery()) {

            int rowCount = 0;
            Map<String, Integer> crimeSlotCounts = new HashMap<>();
            Map<String, CompletionStats> crimeCompletionStats = new HashMap<>();

            while (slotsRs.next()) {
                rowCount++;
                Long crimeId = slotsRs.getLong("crime_id");
                String crimeName = slotsRs.getString("name");
                String slotPosition = slotsRs.getString("slot_position");
                String slotPositionId = slotsRs.getString("slot_position_id");
                Integer difficulty = slotsRs.getObject("difficulty", Integer.class);
                Timestamp expiredAt = slotsRs.getTimestamp("expired_at");

                // Get completion data
                Integer totalSlots = crimeTotalSlots.getOrDefault(crimeName, 6); // Default to 6 if not found
                Integer availableSlots = crimeAvailableSlotCounts.getOrDefault(crimeId, 1);
                Integer filledSlots = totalSlots - availableSlots;

                // Track statistics
                crimeSlotCounts.merge(crimeName, 1, Integer::sum);
                crimeCompletionStats.computeIfAbsent(crimeName, k -> new CompletionStats())
                        .addCrime(totalSlots, filledSlots, availableSlots);

                // Calculate values using existing logic
                CrimeRewardData rewardData = crimeRewards.get(crimeName);
                Long rewardsHigh = rewardData != null ? rewardData.getHighValue() : null;
                Long rewardsLow = rewardData != null ? rewardData.getLowValue() : null;
                Long expectedValue = calculateExpectedValue(rewardsHigh, rewardsLow, difficulty);
                Double slotPriority = calculateSlotPriority(crimeName, slotPosition, difficulty, rolePriorities);

                // Create enhanced slot with completion data
                AvailableCrimeSlot slot = new AvailableCrimeSlot(
                        crimeId, crimeName, slotPosition, slotPositionId,
                        difficulty, expiredAt, expectedValue, slotPriority,
                        totalSlots, availableSlots);

                slots.add(slot);

                // ADD THIS DEBUG LOGGING HERE:
                if (rowCount <= 10) { // Log first 10 entries instead of 5 for more data
                    logger.info("DEBUG SLOT {}: {} - {} ({}) - Total:{} Available:{} Filled:{} CompletionPriority:%.2f ExpectedValue:{}",
                            rowCount, slot.getCrimeName(), slot.getSlotPosition(), slot.getSlotPositionId(),
                            slot.getTotalSlots(), slot.getAvailableSlots(), slot.getFilledSlots(),
                            slot.getCompletionPriority(), slot.getExpectedValue());
                }

                if (rowCount <= 5) { // Log first few entries for debugging
                    logger.debug("  Slot {}: {} - {} ({}) - Completion: {}, Priority: %.2f",
                            rowCount, slot.getCrimeName(), slot.getSlotPosition(),
                            slot.getCompletionStatus(), slot.getCompletionPriority());
                }
            }

            // Sort slots by completion priority, then by other factors
            slots.sort((a, b) -> {
                // Primary sort: completion priority (descending)
                int priorityCompare = Double.compare(b.getCompletionPriority(), a.getCompletionPriority());
                if (priorityCompare != 0) return priorityCompare;

                // Secondary sort: difficulty (descending)
                int diffCompare = Integer.compare(
                        b.getCrimeDifficulty() != null ? b.getCrimeDifficulty() : 0,
                        a.getCrimeDifficulty() != null ? a.getCrimeDifficulty() : 0
                );
                if (diffCompare != 0) return diffCompare;

                // Tertiary sort: expected value (descending)
                return Long.compare(b.getExpectedValue(), a.getExpectedValue());
            });

            logger.info("✓ Successfully loaded {} crime slots with completion data for faction {}",
                    rowCount, factionInfo.getFactionId());

            // Enhanced logging with completion statistics
            if (!crimeCompletionStats.isEmpty()) {
                logger.info("  Crime completion breakdown for faction {}:", factionInfo.getFactionId());

                // Sort by average completion percentage for better readability
                crimeCompletionStats.entrySet().stream()
                        .sorted((a, b) -> Double.compare(
                                b.getValue().getAverageCompletionRatio(),
                                a.getValue().getAverageCompletionRatio()))
                        .forEach(entry -> {
                            String crimeName = entry.getKey();
                            CompletionStats stats = entry.getValue();
                            int availableSlots = crimeSlotCounts.getOrDefault(crimeName, 0);

                            String priorityLevel = stats.getAverageCompletionRatio() >= 0.75 ? "🔥 URGENT" :
                                    stats.getAverageCompletionRatio() >= 0.5 ? "⚡ HIGH" :
                                            stats.getAverageCompletionRatio() >= 0.25 ? "📈 MEDIUM" :
                                                    stats.getAverageCompletionRatio() > 0 ? "🟡 STARTED" : "🆕 NEW";

                            logger.info("    {} {}: avg %.0f% complete, {} available slots across {} crimes",
                                    priorityLevel, crimeName, stats.getAverageCompletionRatio() * 100,
                                    availableSlots, stats.getCrimeCount());

                            logger.info("COMPLETION SUMMARY for faction {}: {} unique crime types processed",
                                    factionInfo.getFactionId(), crimeCompletionStats.size());

                            // Log total slots by completion priority
                            long urgentSlots = slots.stream().filter(s -> s.getCompletionPriority() >= 3.0).count();
                            long highSlots = slots.stream().filter(s -> s.getCompletionPriority() >= 2.5 && s.getCompletionPriority() < 3.0).count();
                            long mediumSlots = slots.stream().filter(s -> s.getCompletionPriority() >= 2.0 && s.getCompletionPriority() < 2.5).count();
                            long lowSlots = slots.stream().filter(s -> s.getCompletionPriority() < 2.0).count();

                            logger.info("SLOT PRIORITY DISTRIBUTION: Urgent(3.0+):{} High(2.5+):{} Medium(2.0+):{} Low(<2.0):{}",
                                    urgentSlots, highSlots, mediumSlots, lowSlots);

                        });
            }

        } catch (SQLException e) {
            logger.error("✗ ERROR: Could not load available crime slots for faction {} from table {}: {}",
                    factionInfo.getFactionId(), availableCrimesTable, e.getMessage());
            throw e;
        }

        return slots;
    }

    /**
     * Helper class to track completion statistics across multiple crimes of the same type
     */
    private static class CompletionStats {
        private int crimeCount = 0;
        private int totalSlotsSum = 0;
        private int filledSlotsSum = 0;
        private int availableSlotsSum = 0;

        public void addCrime(int totalSlots, int filledSlots, int availableSlots) {
            this.crimeCount++;
            this.totalSlotsSum += totalSlots;
            this.filledSlotsSum += filledSlots;
            this.availableSlotsSum += availableSlots;
        }

        public double getAverageCompletionRatio() {
            return totalSlotsSum > 0 ? (double) filledSlotsSum / totalSlotsSum : 0.0;
        }

        public int getCrimeCount() { return crimeCount; }
        public int getTotalSlotsSum() { return totalSlotsSum; }
        public int getFilledSlotsSum() { return filledSlotsSum; }
        public int getAvailableSlotsSum() { return availableSlotsSum; }
    }


    /**
     * Load role priorities from CONFIG database
     */
    private static Map<String, Double> loadRolePrioritiesFromConfig(Connection configConnection) throws SQLException {
        Map<String, Double> priorities = new HashMap<>();

        String prioritiesSql = "SELECT role_name, weight FROM crimes_roles_priority";

        logger.debug("Loading role priorities with SQL: {}", prioritiesSql);

        try (PreparedStatement stmt = configConnection.prepareStatement(prioritiesSql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String roleName = rs.getString("role_name");
                Double weight = rs.getObject("weight", Double.class);

                if (roleName != null && weight != null) {
                    priorities.put(roleName, weight);
                    logger.debug("Loaded role priority: {} = {}", roleName, weight);
                }
            }
        } catch (SQLException e) {
            logger.warn("Could not load role priorities from CONFIG database table crimes_roles_priority: {}",
                    e.getMessage());
            logger.warn("Will use default priority values for role assignment");
            // Don't throw - we can still process with default values
        }

        return priorities;
    }

    /**
     * Load crime reward data from CONFIG database
     */
    private static Map<String, CrimeRewardData> loadCrimeRewardsFromConfig(Connection configConnection) throws SQLException {
        Map<String, CrimeRewardData> rewards = new HashMap<>();

        String rewardsSql = "SELECT crime_name, rewards_value_high, rewards_value_low FROM " + Constants.TABLE_NAME_OC2_CRIMES;

        logger.debug("Loading crime rewards with SQL: {}", rewardsSql);

        try (PreparedStatement stmt = configConnection.prepareStatement(rewardsSql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String crimeName = rs.getString("crime_name");
                Long highValue = rs.getObject("rewards_value_high", Long.class);
                Long lowValue = rs.getObject("rewards_value_low", Long.class);

                rewards.put(crimeName, new CrimeRewardData(highValue, lowValue));
            }
        } catch (SQLException e) {
            logger.warn("Could not load crime rewards from CONFIG database table {}: {}",
                    Constants.TABLE_NAME_OC2_CRIMES, e.getMessage());
            // Don't throw - we can still process without reward data
        }

        return rewards;
    }

    // Helper class for crime reward data
    private static class CrimeRewardData {
        private final Long highValue;
        private final Long lowValue;

        public CrimeRewardData(Long highValue, Long lowValue) {
            this.highValue = highValue;
            this.lowValue = lowValue;
        }

        public Long getHighValue() { return highValue; }
        public Long getLowValue() { return lowValue; }
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
     * Calculate slot priority using actual database values from crimes_roles_priority table
     * Now checks both crime name and role name for accurate priority lookup
     * Uses exact role matching including #number (e.g., "Robber #1", "Robber #2")
     */
    private static Double calculateSlotPriority(String crimeName, String slotPosition, Integer difficulty,
                                                Map<String, Double> rolePriorities) {
        double basePriority = 1.0;

        // Use actual priority from database if available (crime-specific role priority)
        if (crimeName != null && slotPosition != null && rolePriorities != null) {
            // Create key combining crime name and exact role name (including #number)
            String crimeRoleKey = crimeName + "|" + slotPosition;
            Double dbPriority = rolePriorities.get(crimeRoleKey);

            if (dbPriority != null) {
                basePriority = dbPriority;
                logger.debug("Using database priority for '{}' role '{}': {}", crimeName, slotPosition, basePriority);
            } else {
                logger.debug("No database priority found for '{}' role '{}', using default: {}",
                        crimeName, slotPosition, basePriority);
            }
        }

        // Adjust based on difficulty (higher difficulty = higher priority)
        if (difficulty != null && difficulty > 5) {
            basePriority *= (1.0 + (difficulty - 5) * 0.1);
        }

        return Math.min(basePriority, 5.0); // Increased cap to accommodate database values
    }

    /**
     * Get faction information from the config database
     * Enhanced with debugging
     */
    private static List<FactionInfo> getFactionInfo(Connection configConnection) throws SQLException {
        List<FactionInfo> factions = new ArrayList<>();

        String sql = "SELECT " + Constants.COLUMN_NAME_FACTION_ID + ", " + Constants.COLUMN_NAME_DB_SUFFIX + " " +
                "FROM " + Constants.TABLE_NAME_FACTIONS + " " +
                "WHERE oc2_enabled = true";

        logger.info("Loading faction info from CONFIG database with SQL: {}", sql);

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            int totalFactions = 0;
            int validFactions = 0;

            while (rs.next()) {
                totalFactions++;
                String factionId = rs.getString(Constants.COLUMN_NAME_FACTION_ID);
                String dbSuffix = rs.getString(Constants.COLUMN_NAME_DB_SUFFIX);

                logger.debug("Found faction record {}: ID={}, Suffix={}", totalFactions, factionId, dbSuffix);

                if (factionId != null && isValidDbSuffix(dbSuffix)) {
                    factions.add(new FactionInfo(factionId, dbSuffix));
                    validFactions++;
                    logger.info("✓ Added valid faction {} with suffix {} to processing list", factionId, dbSuffix);
                } else {
                    logger.warn("✗ Skipping invalid faction: factionId={}, dbSuffix={}", factionId, dbSuffix);
                }
            }

            logger.info("✓ Faction loading summary: {} total records, {} valid OC2-enabled factions",
                    totalFactions, validFactions);
        }

        if (factions.isEmpty()) {
            logger.warn("WARNING: No valid OC2-enabled factions found in CONFIG database");
            logger.info("  Check that table {} has records with oc2_enabled = true", Constants.TABLE_NAME_FACTIONS);
        }

        return factions;
    }

    /**
     * Validate db suffix for SQL injection prevention
     */
    private static boolean isValidDbSuffix(String dbSuffix) {
        if (dbSuffix == null || dbSuffix.isEmpty() || dbSuffix.length() > 50) {
            return false;
        }

        // Must start with letter and contain only letters, numbers, and underscores
        boolean isValid = dbSuffix.matches("^[a-zA-Z][a-zA-Z0-9_]*$");

        if (!isValid) {
            logger.warn("Invalid db_suffix format: '{}' - must start with letter and contain only alphanumeric/underscore", dbSuffix);
        }

        return isValid;
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
     * Enhanced with debugging
     */
    public static AssignmentRecommendation generateAssignmentRecommendations(Connection configConnection,
                                                                             Connection ocDataConnection,
                                                                             FactionInfo factionInfo) throws SQLException {

        logger.info("--- Generating assignment recommendations for faction {} ---", factionInfo.getFactionId());

        OptimizationResult result = optimizeFactionCrimeAssignments(configConnection, ocDataConnection, factionInfo);

        logger.info("Optimization result for faction {}: {}", factionInfo.getFactionId(), result);

        if (result.getAssignments().isEmpty()) {
            logger.info("No assignments found for faction {} - returning empty recommendation", factionInfo.getFactionId());
            return new AssignmentRecommendation(new ArrayList<>(),
                    List.of("No optimal assignments found - check member availability and crime slots"),
                    new HashMap<>(), 0L);
        }

        logger.info("Processing {} assignments for recommendations...", result.getAssignments().size());

        // Calculate crime completion probabilities
        Map<String, Double> completionProbs = calculateCrimeCompletionProbabilities(result.getAssignments());
        logger.debug("Calculated completion probabilities for {} crimes", completionProbs.size());

        // Generate strategic recommendations (urgency removed)
        List<String> strategicRecommendations = generateStrategicRecommendations(result, factionInfo);
        logger.debug("Generated {} strategic recommendations", strategicRecommendations.size());

        // Calculate total expected value
        long totalExpectedValue = result.getAssignments().stream()
                .mapToLong(assignment -> assignment.getSlot().getExpectedValue())
                .sum();

        logger.info("✓ Generated recommendations for faction {} - {} assignments, total expected value: ${}",
                factionInfo.getFactionId(), result.getAssignments().size(), formatCurrency(totalExpectedValue));

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
     * Enhanced generateStrategicRecommendations to include unassigned member info
     */
    private static List<String> generateStrategicRecommendations(OptimizationResult result, FactionInfo factionInfo) {
        List<String> recommendations = new ArrayList<>();

        if (result.getAssignments().isEmpty()) {
            recommendations.add("No members available for assignment - consider recruiting more active members");
            return recommendations;
        }

        // Completion-based recommendations
        List<MemberSlotAssignment> urgentCompletions = result.getAssignments().stream()
                .filter(a -> a.getSlot().getCompletionPriority() >= 3.0)
                .collect(Collectors.toList());

        List<MemberSlotAssignment> nearCompletions = result.getAssignments().stream()
                .filter(a -> a.getSlot().getCompletionPriority() >= 2.5 && a.getSlot().getCompletionPriority() < 3.0)
                .collect(Collectors.toList());

        if (!urgentCompletions.isEmpty()) {
            recommendations.add(String.format("🔥 URGENT: %d crimes are nearly complete (75%+ filled) - prioritize these for immediate returns",
                    urgentCompletions.size()));
        }

        if (!nearCompletions.isEmpty()) {
            recommendations.add(String.format("⚡ Focus on %d active crimes (50%+ filled) for efficient completion",
                    nearCompletions.size()));
        }

        // High-value crime recommendations
        long highValueThreshold = 30_000_000L;
        List<MemberSlotAssignment> highValueAssignments = result.getAssignments().stream()
                .filter(a -> a.getSlot().getExpectedValue() > highValueThreshold)
                .collect(Collectors.toList());

        if (!highValueAssignments.isEmpty()) {
            recommendations.add(String.format("Focus on %d high-value crimes (>$%dM) for maximum return",
                    highValueAssignments.size(), highValueThreshold / 1_000_000));
        }

        // Resource efficiency and unassigned member recommendations
        if (result.getUnfilledSlots() > result.getUnassignedMembers()) {
            recommendations.add("More crime slots available than members - consider recruitment");
        } else if (result.getUnassignedMembers() > result.getUnfilledSlots()) {
            // NEW: Enhanced recommendation for unassigned members
            recommendations.add(String.format("More members (%d) than available slots (%d) - consider spawning additional crimes",
                    result.getUnassignedMembers(), result.getUnfilledSlots()));

            if (result.getUnassignedMembers() >= 3) {
                recommendations.add("⚠️ Significant number of unassigned members - crime spawning notification has been sent");
            }
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

        @Override
        public String toString() {
            return String.format("DiscordMemberMapping{userId='%s', username='%s', discordId='%s'}",
                    userId, username, discordId);
        }
    }

    /**
     * Send Discord assignment notifications for all factions
     * Enhanced with comprehensive debugging and error tracking
     */
    public static void sendDiscordAssignmentNotifications() throws SQLException {
        logger.info("==================== STARTING DISCORD NOTIFICATIONS ====================");
        logger.info("Sending Discord crime assignment notifications to all factions");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            logger.error("CRITICAL: DATABASE_URL_CONFIG environment variable not set");
            throw new IllegalStateException("DATABASE_URL_CONFIG environment variable not set");
        }

        if (ocDataDatabaseUrl == null || ocDataDatabaseUrl.isEmpty()) {
            logger.error("CRITICAL: DATABASE_URL_OC_DATA environment variable not set");
            throw new IllegalStateException("DATABASE_URL_OC_DATA environment variable not set");
        }

        logger.info("Database URLs loaded for Discord notifications");

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            logger.info("✓ Database connections established for Discord notifications");

            List<FactionInfo> factions = getFactionInfo(configConnection);
            if (factions.isEmpty()) {
                logger.warn("WARNING: No OC2-enabled factions found for Discord notifications");
                return;
            }

            logger.info("✓ Found {} factions to send Discord notifications to", factions.size());
            for (int i = 0; i < factions.size(); i++) {
                FactionInfo faction = factions.get(i);
                logger.info("  {}. Will notify faction: {} (suffix: {})", i + 1, faction.getFactionId(), faction.getDbSuffix());
            }

            int processedCount = 0;
            int successfulNotifications = 0;
            int failedNotifications = 0;

            for (FactionInfo factionInfo : factions) {
                logger.info("==================== DISCORD NOTIFICATION {}/{} ====================",
                        processedCount + 1, factions.size());
                logger.info("Processing Discord notification for faction: {}", factionInfo);

                try {
                    boolean success = sendFactionAssignmentNotification(configConnection, ocDataConnection, factionInfo);
                    if (success) {
                        successfulNotifications++;
                        logger.info("✓ SUCCESS: Sent Discord assignment notification to faction {}",
                                factionInfo.getFactionId());
                    } else {
                        failedNotifications++;
                        logger.warn("✗ FAILED: Discord assignment notification failed for faction {}",
                                factionInfo.getFactionId());
                    }

                    processedCount++;

                } catch (Exception e) {
                    failedNotifications++;
                    processedCount++;
                    logger.error("✗ EXCEPTION: Error sending Discord notification to faction {}: {}",
                            factionInfo.getFactionId(), e.getMessage(), e);
                }

                logger.info("==================== COMPLETED NOTIFICATION {} ====================",
                        factionInfo.getFactionId());
            }

            // Final summary
            logger.info("==================== DISCORD NOTIFICATIONS SUMMARY ====================");
            logger.info("Discord assignment notifications completed:");
            logger.info("  Total factions processed: {}/{}", processedCount, factions.size());
            logger.info("  Successful notifications: {}", successfulNotifications);
            logger.info("  Failed notifications: {}", failedNotifications);
            logger.info("  Success rate: %.1f%", processedCount > 0 ? (successfulNotifications * 100.0 / processedCount) : 0.0);

        } catch (SQLException e) {
            logger.error("CRITICAL: Database error during Discord assignment notifications", e);
            throw e;
        }

        logger.info("==================== DISCORD NOTIFICATIONS COMPLETE ====================");
    }

    /**
     * Send Discord assignment notification for a single faction
     * Enhanced with detailed debugging
     */
    private static boolean sendFactionAssignmentNotification(Connection configConnection,
                                                             Connection ocDataConnection,
                                                             FactionInfo factionInfo) throws SQLException {

        logger.info("--- Processing Discord notification for faction {} ---", factionInfo.getFactionId());

        try {
            // Step 1: Generate assignment recommendations
            logger.info("STEP 1: Generating assignment recommendations for faction {}", factionInfo.getFactionId());
            AssignmentRecommendation recommendation = generateAssignmentRecommendations(
                    configConnection, ocDataConnection, factionInfo);

            logger.info("STEP 1 RESULT: Generated {} assignments for faction {}",
                    recommendation.getImmediateAssignments().size(), factionInfo.getFactionId());

            if (recommendation.getImmediateAssignments().isEmpty()) {
                logger.info("STEP 1 SKIP: No assignments found for faction {} - no Discord notification needed",
                        factionInfo.getFactionId());
                return true; // Not an error, just nothing to send
            }

            // Step 2: Load Discord member mappings from CONFIG database
            logger.info("STEP 2: Loading Discord member mappings for faction {}", factionInfo.getFactionId());
            Map<String, DiscordMemberMapping> memberMappings = loadDiscordMemberMappings(
                    configConnection, factionInfo);

            logger.info("STEP 2 RESULT: Found {} Discord mappings for faction {}",
                    memberMappings.size(), factionInfo.getFactionId());

            if (memberMappings.isEmpty()) {
                logger.error("STEP 2 FAILED: No Discord member mappings found for faction {} - cannot send notification",
                        factionInfo.getFactionId());
                return false;
            }

            // Step 3: Send Discord message
            logger.info("STEP 3: Sending Discord message for faction {} with {} assignments to {} Discord members",
                    factionInfo.getFactionId(), recommendation.getImmediateAssignments().size(), memberMappings.size());

            boolean success = DiscordMessages.sendCrimeAssignmentToAllMembers(factionInfo, recommendation, memberMappings);

            if (success) {
                logger.info("STEP 3 SUCCESS: Discord message sent successfully for faction {}", factionInfo.getFactionId());
            } else {
                logger.error("STEP 3 FAILED: Discord message send failed for faction {}", factionInfo.getFactionId());
            }

            return success;

        } catch (SQLException e) {
            logger.error("SQL ERROR: Database error processing faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("GENERAL ERROR: Unexpected error processing faction {}: {}",
                    factionInfo.getFactionId(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * Load Discord member mappings from the members table in CONFIG database
     * Enhanced with comprehensive debugging
     */
    private static Map<String, DiscordMemberMapping> loadDiscordMemberMappings(Connection configConnection,
                                                                               FactionInfo factionInfo) throws SQLException {
        Map<String, DiscordMemberMapping> mappings = new HashMap<>();

        // Fixed table name format: members_factionsuffix
        String membersTable = "members_" + factionInfo.getDbSuffix();

        logger.info("DEBUG: Loading Discord mappings from CONFIG database table: {}", membersTable);

        // Check if table exists first
        try {
            verifyTableExists(configConnection, membersTable, "members");
            logger.info("✓ Table {} exists in CONFIG database", membersTable);
        } catch (SQLException e) {
            logger.error("✗ CRITICAL: Members table {} does not exist in CONFIG database for faction {}: {}",
                    membersTable, factionInfo.getFactionId(), e.getMessage());
            throw new SQLException("Members table missing in CONFIG database for faction " + factionInfo.getFactionId(), e);
        }

        // Fixed column name: user_discord_id instead of discord_id
        String sql = "SELECT user_id, username, user_discord_id FROM " + membersTable +
                " WHERE user_discord_id IS NOT NULL AND user_discord_id != ''";

        logger.debug("Executing SQL: {}", sql);

        try (PreparedStatement pstmt = configConnection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            int totalMembers = 0;
            int mappingCount = 0;

            while (rs.next()) {
                totalMembers++;
                String userId = rs.getString("user_id");
                String username = rs.getString("username");
                String discordId = rs.getString("user_discord_id");

                logger.debug("Found member {}: {} ({}) - Discord ID: {}",
                        totalMembers, username, userId, discordId != null ? "SET" : "NULL");

                if (userId != null && discordId != null && !discordId.trim().isEmpty()) {
                    mappings.put(userId, new DiscordMemberMapping(userId, username, discordId.trim()));
                    mappingCount++;
                } else {
                    logger.debug("  Skipping member {} - missing user_id or discord_id", username);
                }
            }

            logger.info("✓ Discord mapping summary for faction {}: {} total members, {} with Discord IDs",
                    factionInfo.getFactionId(), totalMembers, mappingCount);

            if (mappingCount == 0 && totalMembers > 0) {
                logger.warn("WARNING: Faction {} has {} members but none have Discord IDs configured",
                        factionInfo.getFactionId(), totalMembers);
            } else if (mappingCount == 0) {
                logger.warn("WARNING: No members found in table {} for faction {}",
                        membersTable, factionInfo.getFactionId());
            }

        } catch (SQLException e) {
            logger.error("✗ ERROR: Could not load Discord member mappings for faction {} from table {}: {}",
                    factionInfo.getFactionId(), membersTable, e.getMessage());
            throw e;
        }

        return mappings;
    }

    /**
     * Utility method to verify a table exists and is accessible
     */
    private static void verifyTableExists(Connection connection, String tableName, String tableType) throws SQLException {
        String checkTableSql = "SELECT 1 FROM " + tableName + " LIMIT 1";

        try (PreparedStatement checkStmt = connection.prepareStatement(checkTableSql);
             ResultSet checkRs = checkStmt.executeQuery()) {
            // If we get here, table exists and is accessible
        } catch (SQLException e) {
            throw new SQLException(String.format("%s table '%s' does not exist or is not accessible", tableType, tableName), e);
        }
    }

    /**
     * Utility method to mask database URLs for safe logging
     */
    private static String maskDatabaseUrl(String url) {
        if (url == null) return "null";

        // Mask password in connection string
        return url.replaceAll(":[^:/@]*@", ":***@");
    }

    /**
     * Format currency for logging
     */
    private static String formatCurrency(long amount) {
        if (amount >= 1_000_000_000L) {
            return String.format("%.1fB", amount / 1_000_000_000.0);
        } else if (amount >= 1_000_000L) {
            return String.format("%.1fM", amount / 1_000_000.0);
        } else if (amount >= 1_000L) {
            return String.format("%.1fK", amount / 1_000.0);
        } else {
            return String.valueOf(amount);
        }
    }

    /**
     * Main method for testing - runs optimization and Discord notifications
     */
    public static void main(String[] args) {
        logger.info("Starting CrimeAssignmentOptimizer test run...");

        try {
            // First run the optimization
            logger.info("=== PHASE 1: RUNNING OPTIMIZATION ===");
            optimizeAllFactionsCrimeAssignments();

            logger.info("=== PHASE 2: SENDING DISCORD NOTIFICATIONS ===");
            sendDiscordAssignmentNotifications();

            logger.info("=== TEST RUN COMPLETED SUCCESSFULLY ===");

        } catch (Exception e) {
            logger.error("Test run failed with exception: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ==================== DEBUGGING HELPER METHODS ====================

    /**
     * Debug method to check faction table structure and data
     */
    public static void debugFactionTables() throws SQLException {
        logger.info("==================== FACTION TABLE DEBUG ====================");

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        String ocDataDatabaseUrl = System.getenv(Constants.DATABASE_URL_OC_DATA);

        try (Connection configConnection = Execute.postgres.connect(configDatabaseUrl, logger);
             Connection ocDataConnection = Execute.postgres.connect(ocDataDatabaseUrl, logger)) {

            // Check factions table
            String factionsSql = "SELECT faction_id, db_suffix, oc2_enabled FROM " + Constants.TABLE_NAME_FACTIONS +
                    " ORDER BY faction_id";

            logger.info("Checking factions table structure and data:");

            try (PreparedStatement stmt = configConnection.prepareStatement(factionsSql);
                 ResultSet rs = stmt.executeQuery()) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    String factionId = rs.getString("faction_id");
                    String dbSuffix = rs.getString("db_suffix");
                    boolean oc2Enabled = rs.getBoolean("oc2_enabled");

                    logger.info("  Faction {}: ID={}, Suffix={}, OC2Enabled={}",
                            count, factionId, dbSuffix, oc2Enabled);

                    if (oc2Enabled) {
                        // Check if required tables exist for this faction
                        checkFactionTables(configConnection, ocDataConnection, new FactionInfo(factionId, dbSuffix));
                    }
                }

                logger.info("Total factions found: {}", count);

            } catch (SQLException e) {
                logger.error("Error checking factions table: {}", e.getMessage(), e);
            }
        }

        logger.info("==================== FACTION DEBUG COMPLETE ====================");
    }

    /**
     * Check if all required tables exist for a faction
     */
    private static void checkFactionTables(Connection configConnection, Connection ocDataConnection, FactionInfo factionInfo) {
        logger.info("    Checking tables for faction {} (suffix: {})", factionInfo.getFactionId(), factionInfo.getDbSuffix());

        // Tables to check in CONFIG database
        String[] configTables = {
                "members_" + factionInfo.getDbSuffix()
        };

        // Tables to check in OC_DATA database
        String[] ocDataTables = {
                "a_crimes_" + factionInfo.getDbSuffix(),
                Constants.TABLE_NAME_AVAILABLE_MEMBERS + factionInfo.getDbSuffix(),
                Constants.TABLE_NAME_CPR + factionInfo.getDbSuffix()
        };

        // Check CONFIG tables
        for (String table : configTables) {
            try {
                verifyTableExists(configConnection, table, "CONFIG");
                logger.info("      CONFIG table exists: {}", table);
            } catch (SQLException e) {
                logger.warn("      CONFIG table missing: {} - {}", table, e.getMessage());
            }
        }

        // Check OC_DATA tables
        for (String table : ocDataTables) {
            try {
                verifyTableExists(ocDataConnection, table, "OC_DATA");
                logger.info("      OC_DATA table exists: {}", table);
            } catch (SQLException e) {
                logger.warn("      OC_DATA table missing: {} - {}", table, e.getMessage());
            }
        }
    }
}

