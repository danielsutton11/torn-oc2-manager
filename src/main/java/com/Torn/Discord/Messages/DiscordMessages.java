package com.Torn.Discord.Messages;

import com.Torn.Discord.Messages.SendDiscordMessage.DiscordEmbed;
import com.Torn.Discord.Messages.SendDiscordMessage.RoleType;
import com.Torn.Discord.Messages.SendDiscordMessage.Colors;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.util.*;

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
                .setTitle("💰 Payment Request #" + requestId.substring(0, 8) + "...")
                .setDescription(String.format(
                        "**%s [%s]** needs payment for an item they already have:\n\n" +
                                "💎 **Item:** %s\n" +
                                "💵 **Amount:** %s\n" +
                                "⏰ **Expires:** 15 minutes after claimed\n",
                        playerName, playerId, itemName, formatCurrency(amount)
                ))
                .setColor(Colors.BLUE)
                .addField("🔧 Quick Actions",
                        "[**Auto Fulfill**](" + createPayUrl(playerId, amount, requestId) + ") - " +
                                "Automatically opens Torn payment page\n\n" +
                                "[**Manual Fulfill**](" + createManualPayUrl(requestId) + ") - " +
                                "Claim request for manual payment\n\n" +
                                "[**Torn Payment Page**](https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user) - " +
                                "Direct link to faction controls",
                        false)
                .addField("📋 Request Details",
                        String.format(
                                "**Request ID:** %s\n" +
                                        "**Player:** %s [%s]\n" +
                                        "**Amount:** %s\n" +
                                        "**Item:** %s\n" +
                                        "**Status:** ⏳ Pending\n" +
                                        "**Crime ID:** %s",
                                requestId.substring(0, 8) + "...",
                                playerName,
                                playerId,
                                formatCurrency(amount),
                                itemName,
                                "N/A" // Could add crime ID if needed
                        ),
                        true)
                .addField("ℹ️ Important Notes",
                        "• Click **Auto Fulfill** to be redirected to Torn payment page\n" +
                                "• Use **Manual Fulfill** if you prefer to pay manually\n" +
                                "• Links become invalid once claimed by someone\n" +
                                "• Unclaimed requests reset after 15 minutes",
                        false)
                .setFooter("OC2 Payment System • Request expires 15 minutes after being claimed", null)
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
     * Send withdraw Xanax notification
     */
    public static boolean sendLeaderWithdrawXanax(String factionId, String crimeName, String itemQuantity) {

        DiscordEmbed embed = new DiscordEmbed()
                .setTitle("💊 OC2 Xanax Withdrawal")
                .setDescription(String.format(
                        "The crime **%s has just rewarded **%s Xanax. Please withdraw as soon as possible.\n",
                        crimeName, itemQuantity
                ))
                .setColor(Colors.ORANGE)
                .addField("📋 Quick Actions",
                        "🔫 [**Armoury**](https://www.torn.com/factions.php?step=your#/tab=armoury&start=0&sub=drugs) | ",
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
                        "The crime **%s has just completed successfully, please issue the payout as soon as possible.\n",
                        crimeName
                ))
                .setColor(Colors.GREEN)
                .addField("📋 Quick Actions",
                        "👮 [**Organised Crimes**](https://www.torn.com/factions.php?step=your&type=1#/tab=crimes) | ",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Leadership
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
                .setTitle("✅ OC2 Crime Complete")
                .setDescription(
                        "There are currently insufficient organised crimes available, please spawn some more!\n"
                )
                .setColor(Colors.CYAN)
                .addField("📋 Quick Actions",
                        "👮 [**Organised Crimes**](https://www.torn.com/factions.php?step=your&type=1#/tab=crimes) | ",
                        false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Leadership
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
                .addField("📋 Quick Actions", quickActions.toString(), false)
                .setFooter("OC2 Management System", null)
                .setTimestamp(java.time.Instant.now().toString());

        // Send to Leadership
        return SendDiscordMessage.sendToRole(
                factionId,
                RoleType.ARMOURER,
                null, // No additional text message
                embed,
                "OC2 Manager" // Custom bot name
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
        // TODO: Replace with your actual web service domain
        // For now, this is a placeholder URL structure
        return "https://your-service-domain.com/payment/claim/" + requestId + "?userId=" + userId;

        // Alternative: For development/testing, you could temporarily use the direct Torn URL
        // return "https://www.torn.com/factions.php?step=your&type=1#/tab=controls&option=give-to-user&addMoneyTo=" +
        //         userId + "&money=" + amount + "&ref=" + requestId;
    }

    /**
     * Create manual payment URL for cases where auto-fulfill doesn't work
     */
    public static String createManualPayUrl(String requestId) {
        // TODO: Replace with your actual web service domain
        return "https://your-service-domain.com/payment/manual/" + requestId;
    }


    public static String formatCurrency(long number) {
        DecimalFormat formatter = new DecimalFormat("$#,###");
        return formatter.format(number);
    }
}