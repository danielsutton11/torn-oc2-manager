package com.Torn.Discord.Messages;

import com.Torn.Discord.Messages.SendDiscordMessage.DiscordEmbed;
import com.Torn.Discord.Messages.SendDiscordMessage.RoleType;
import com.Torn.Discord.Messages.SendDiscordMessage.Colors;
import com.Torn.FactionCrimes._Algorithms.CrimeAssignmentOptimizer;
import com.Torn.Helpers.Constants;
import org.apache.tomcat.util.bcel.Const;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DiscordMessages {


    /**
     * Enhanced sendPayMemberForItem method with better messaging and manual option
     */
    public static boolean sendPayMemberForItem(String factionId,
                                               String playerName,
                                               String playerId,
                                               String requestId,
                                               String itemName,
                                               long amount) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("💰 Payment Request #" + requestId)
                .setDescription(String.format(
                        "**%s [%s]** needs payment for an OC item they already had:\n\n" +
                                "💎 **Item:** %s\n" +
                                "💵 **Amount:** %s\n",
                        playerName, playerId, itemName, formatCurrency(amount)
                ))
                .setColor(Colors.BLUE)
                .addField("__Quick Actions__",
                        "\n[**Auto Fulfill**](" + createPayUrl(playerId, amount, requestId) + ") - " +
                                "Automatically opens Torn payment page\n\n" +
                                "[**Manual Fulfill**](" + createManualPayUrl(requestId) + ") - " +
                                "Claim request for manual payment\n\n" +
                                "[**Torn Payment Page**](https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user) - " +
                                "Direct link to faction controls",
                        false)
                .addField("ℹ️ Important Notes",
                        "\n" + "• Click **Auto Fulfill** to be redirected to Torn payment page\n" +
                                "• Use **Manual Fulfill** if you prefer to pay manually\n" +
                                "• Links become invalid once claimed by someone\n" +
                                "• Unclaimed requests reset after 1 hour.",
                        false)
                .setFooter("OC2 Payment System • Request expires 1 after being claimed if not paid", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to bankers with custom username
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.BANKER,
                null, // No additional text message
                embed,
                "OC2 Payment Manager" // Custom bot name
        );
    }

    /**
     * send PaymentFulfilled Message
     */
    public static boolean paymentFulfilled(String factionId,
                                               String playerName,
                                               String playerId,
                                               String requestId,
                                               String itemName,
                                               long amount) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("💰 Payment Fulfilled #" + requestId)
                .setDescription(String.format(
                        "The payment to **%s [%s]** has been fulfilled:\n\n" +
                                "💎 **Item:** %s\n" +
                                "💵 **Amount:** %s\n",
                        playerName, playerId, itemName, formatCurrency(amount)
                ))
                .addField("Status", "✅ **COMPLETED**", true)
                .setColor(Colors.GREEN)
                .setFooter("OC2 Payment System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to bankers with custom username
        return SendDiscordMessage.sendMessageNoRole(
                factionId,
                null, // No additional text message
                embed,
                "OC2 Payment Manager" // Custom bot name
        );
    }

    /**
     * Send withdraw Xanax notification
     */
    public static boolean sendLeaderWithdrawXanax(String factionId, String crimeName, String itemQuantity) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("💊 OC2 Xanax Withdrawal")
                .setDescription(String.format(
                        "The crime **%s** has just rewarded **%s Xanax**. Please withdraw as soon as possible.\n",
                        crimeName, itemQuantity
                ))
                .setColor(Colors.ORANGE)
                .addField("__Quick Actions__",
                        "\n🔫 [**Armoury**](https://www.torn.com/factions.php?step=your#/tab=armoury&start=0&sub=drugs)\n\n" +
                                "✅ **Mark as Fulfilled** (React with ✅)",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Leadership
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.LEADERSHIP,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
        );
    }


    /**
     * Send crime has completed notification
     */
    public static boolean sendCrimeComplete(String factionId, String crimeName) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("✅ OC2 Crime Complete")
                .setDescription(String.format(
                        "The crime **%s** has just completed successfully, please issue the payout as soon as possible.\n",
                        crimeName
                ))
                .setColor(Colors.GREEN)
                .addField("__Quick Actions__",
                        "\n\n" + "👮 [**Organised Crimes**](https://www.torn.com/factions.php?step=your&type=1#/tab=crimes)\n\n" +
                                "✅ **Mark as Fulfilled** (React with ✅)",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to OC Manager
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.OC_MANAGER,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
        );
    }

    /**
     * Send need more crimes spawned
     */
    public static boolean sendNeedCrimesToSpawn(String factionId) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("🚨 OC2 Crimes Needed")
                .setDescription(
                        "There are currently insufficient organised crimes available, please spawn some more!\n"
                )
                .setColor(Colors.CYAN)
                .addField("__Quick Actions__",
                        "\n\n" + "👮 [**Organised Crimes**](https://www.torn.com/factions.php?step=your&type=1#/tab=crimes)\n\n" +
                                "✅ **Mark as Fulfilled** (React with ✅)",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to OC Manager
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.OC_MANAGER,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
        );
    }

    /**
     * Send a users required OC items
     */
    public static class ItemRequest {
        private final String userId;
        private final String username;
        private final String itemId;
        private final String itemName;
        private final String requestId;

        public ItemRequest(String userId, String username, String itemId, String itemName, int amount, String requestId) {
            this.userId = userId;
            this.username = username;
            this.itemId = itemId;
            this.itemName = itemName;
            this.requestId = requestId;
        }

        // Getters
        public String getUserId() { return userId; }
        public String getUsername() { return username; }
        public String getItemId() { return itemId; }
        public String getItemName() { return itemName; }
        public String getRequestId() { return requestId; }
    }

    public static boolean sendArmourerGetItems(String factionId, List<ItemRequest> itemRequests) {

        // Build dynamic description
        StringBuilder description = new StringBuilder();
        description.append("The following users required the following items to complete their organised crimes:\n\n");

        for (ItemRequest request : itemRequests) {
            description.append(String.format("**%s [%s]** needs %s**\n",
                    request.getUsername(),
                    request.getUserId(),
                    request.getItemName()
            ));
        }

        // Build dynamic quick actions for unique items
        StringBuilder quickActions = getStringBuilder(itemRequests);

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("🔫 OC2 Items Required")
                .setDescription(description.toString())
                .setColor(Colors.RED)
                .addField("__Quick Actions__", "\n\n" + quickActions, false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Armourer
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.ARMOURER,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
        );
    }

    /**
     * Send item rewards notification to leadership when crime rewards items (excluding Xanax)
     */
    public static boolean sendLeaderItemRewards(String factionId, String crimeName, List<ItemReward> itemRewards) {

        if (itemRewards == null || itemRewards.isEmpty()) {
            return true;
        }

        // Build dynamic description with item details
        StringBuilder description = new StringBuilder();
        description.append(String.format(
                "The crime **%s** has rewarded the following items. Please withdraw from the armoury as soon as possible.\n\n",
                crimeName
        ));

        long totalValue = 0;
        for (ItemReward reward : itemRewards) {
            description.append(String.format(
                    "💎 **%s** x%d",
                    reward.getItemName(),
                    reward.getQuantity()
            ));

            if (reward.getAveragePrice() != null && reward.getAveragePrice() > 0) {
                long itemTotalValue = reward.getAveragePrice().longValue() * reward.getQuantity();
                totalValue += itemTotalValue;
                description.append(String.format(" (≈%s total)", formatCurrency(itemTotalValue)));
            }

            description.append("\n");
        }

        // Add total value if we have pricing data
        if (totalValue > 0) {
            description.append(String.format("\n**Total Estimated Value:** %s", formatCurrency(totalValue)));
        }

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("💎 OC2 Item Rewards")
                .setDescription(description.toString())
                .setColor(Colors.PURPLE)
                .addField("__Quick Actions__",
                        "\n\n" + "🔫 [**Armoury**](https://www.torn.com/factions.php?step=your#/tab=armoury)\n\n" +
                                "✅ **Mark as Fulfilled** (React with ✅)",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Leadership
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.LEADERSHIP,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
        );
    }

    /**
     * Helper class to represent item rewards
     */
    public static class ItemReward {
        private final String itemName;
        private final int quantity;
        private final Integer averagePrice;

        public ItemReward(String itemName, int quantity, Integer averagePrice) {
            this.itemName = itemName;
            this.quantity = quantity;
            this.averagePrice = averagePrice;
        }

        public String getItemName() { return itemName; }
        public int getQuantity() { return quantity; }
        public Integer getAveragePrice() { return averagePrice; }

        @Override
        public String toString() {
            return String.format("%s x%d", itemName, quantity);
        }
    }

    /**
     * Alternative method to send assignments to all members instead of just OC managers
     */
    public static boolean sendCrimeAssignmentToAllMembers(CrimeAssignmentOptimizer.FactionInfo factionInfo,
                                                          CrimeAssignmentOptimizer.AssignmentRecommendation recommendation,
                                                          Map<String, CrimeAssignmentOptimizer.DiscordMemberMapping> memberMappings) {

        // Same message creation logic as above...
        List<CrimeAssignmentOptimizer.MemberSlotAssignment> assignments = recommendation.getImmediateAssignments();
        Map<String, List<CrimeAssignmentOptimizer.MemberSlotAssignment>> assignmentsByCrime = assignments.stream()
                .collect(Collectors.groupingBy(a -> a.getSlot().getCrimeName()));

        StringBuilder description = new StringBuilder();
        description.append("**Your Optimal Crime Assignments**\n\n");

        Set<String> mentionedUsers = new HashSet<>();

        for (Map.Entry<String, List<CrimeAssignmentOptimizer.MemberSlotAssignment>> crimeEntry : assignmentsByCrime.entrySet()) {
            String crimeName = crimeEntry.getKey();
            List<CrimeAssignmentOptimizer.MemberSlotAssignment> crimeAssignments = crimeEntry.getValue();

            description.append(String.format("**🎯 %s**\n", crimeName));

            for (CrimeAssignmentOptimizer.MemberSlotAssignment assignment : crimeAssignments) {
                String userId = assignment.getMember().getUserId();
                CrimeAssignmentOptimizer.DiscordMemberMapping memberMapping = memberMappings.get(userId);

                if (memberMapping != null) {
                    String mention = memberMapping.getMention();
                    mentionedUsers.add(mention);

                    description.append(String.format("• **%s** → %s\n",
                            assignment.getSlot().getSlotPosition(),
                            mention));
                }
            }
            description.append("\n");
        }

        SendDiscordMessage.DiscordEmbed embed = new SendDiscordMessage.DiscordEmbed()
                .setTitle("🎯 OC2 Crime & Role Assignments")
                .setDescription(description.toString())
                .setColor(Colors.GOLD)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        String allMentions = String.join(" ", mentionedUsers);

        // Send without role restrictions - goes to general webhook
        return SendDiscordMessage.sendMessageNoRole(
                factionInfo.getFactionId(),
                allMentions,
                embed,
                "OC2 MAnager"
        );
    }

    @NotNull
    private static StringBuilder getStringBuilder(List<ItemRequest> itemRequests) {
        Set<String> uniqueItems = new LinkedHashSet<>(); // Preserve order, avoid duplicates
        Map<String, String> itemIdMap = new HashMap<>();

        for (ItemRequest request : itemRequests) {
            uniqueItems.add(request.getItemName());
            itemIdMap.put(request.getItemName(), request.getItemId());
        }

        StringBuilder quickActions = new StringBuilder();

        // Add market links for each unique item
        for (String itemName : uniqueItems) {
            String itemId = itemIdMap.get(itemName);
            String marketUrl = "https://www.torn.com/page.php?sid=ItemMarket#/market/view=search&itemID=" + itemId;
            quickActions.append(String.format("🛒 [**%s**](%s)\n", itemName, marketUrl));
        }

        // Add fulfilled action
        quickActions.append("✅ **Mark as Fulfilled** (React with ✅)");
        return quickActions;
    }

    public static String createPayUrl(String userId, long amount, String requestId){
        String baseUrl = System.getenv(Constants.PAYMENT_SERVICE_BASE_URL);
        if (baseUrl == null) {
            baseUrl = "https://oc2-payment-service-dev.up.railway.app";
        }
        return baseUrl + "/payment/claim/" + requestId + "?userId=" + userId;
    }

    /**
     * Create manual payment URL for cases where auto-fulfill doesn't work
     */
    public static String createManualPayUrl(String requestId) {
        String baseUrl = System.getenv(Constants.PAYMENT_SERVICE_BASE_URL);
        if (baseUrl == null) {
            baseUrl = "https://oc2-payment-service-dev.up.railway.app";
        }
        return baseUrl + "/payment/manual/" + requestId;
    }


    public static String formatCurrency(long number) {
        DecimalFormat formatter = new DecimalFormat("$#,###");
        return formatter.format(number);
    }
}