package com.Torn.PaymentRequests;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;

@RestController
@RequestMapping("/payment")
public class PaymentController {

    /**
     * ├── PaymentController.java
     * │   ├── Web endpoints for payment claim links
     * │   ├── Auto-redirect to Torn payment page
     * │   ├── Manual payment instruction pages
     * │   └── Request status tracking
     */

    private static final Logger logger = LoggerFactory.getLogger(PaymentController.class);

    /**
     * Handle auto-fulfill link clicks - shows HTML page with redirect to Torn
     */
    @GetMapping("/claim/{requestId}")
    public ResponseEntity<String> claimPayment(@PathVariable("requestId") String requestId,
                                               @RequestParam("userId") String userId) {

        logger.info("Payment claim request: {} by user: {}", requestId, userId);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            // Try to claim the request (atomic operation)
            boolean claimed = PaymentRequestDAO.claimPaymentRequest(connection, requestId, userId);

            if (!claimed) {
                logger.warn("Failed to claim payment request {} - may already be claimed", requestId);
                return ResponseEntity.status(409)
                        .header("Content-Type", "text/html")
                        .body(createAlreadyClaimedPage(requestId));
            }

            // Get the request details for redirect
            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);
            if (request == null) {
                logger.error("Request {} not found after claiming", requestId);
                return ResponseEntity.status(404)
                        .header("Content-Type", "text/html")
                        .body(createErrorPage("Request not found"));
            }

            // Build Torn payment URL
            String tornPaymentUrl = String.format(
                    "https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user&addMoneyTo=%s&money=%d",
                    request.getUserId(),
                    request.getItemValue()
            );

            logger.info("Redirecting to Torn payment page for request {} (${} to {})",
                    requestId, request.getItemValue(), request.getUsername());

            // Return success page with auto-redirect
            return ResponseEntity.ok()
                    .header("Content-Type", "text/html")
                    .body(createSuccessRedirectPage(request, tornPaymentUrl));

        } catch (Exception e) {
            logger.error("Error processing payment claim for request {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Internal server error"));
        }
    }

    /**
     * Handle manual fulfill link clicks - shows confirmation page
     */
    @GetMapping("/manual/{requestId}")
    public ResponseEntity<String> claimManualPayment(@PathVariable("requestId") String requestId,
                                                     @RequestParam(value = "userId", required = false) String userId) {

        logger.info("Manual payment claim request: {} by user: {}", requestId, userId);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            // Use "MANUAL" as userId if not provided
            String claimUserId = userId != null ? userId : "MANUAL";

            // Try to claim the request (atomic operation)
            boolean claimed = PaymentRequestDAO.claimPaymentRequest(connection, requestId, claimUserId);

            if (!claimed) {
                logger.warn("Failed to claim manual payment request {} - may already be claimed", requestId);
                return ResponseEntity.status(409)
                        .header("Content-Type", "text/html")
                        .body(createAlreadyClaimedPage(requestId));
            }

            // Get the request details
            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);
            if (request == null) {
                logger.error("Request {} not found after claiming", requestId);
                return ResponseEntity.status(404)
                        .header("Content-Type", "text/html")
                        .body(createErrorPage("Request not found"));
            }

            logger.info("Manual payment claimed for request {} (${} to {})",
                    requestId, request.getItemValue(), request.getUsername());

            // Return manual payment instructions page
            return ResponseEntity.ok()
                    .header("Content-Type", "text/html")
                    .body(createManualPaymentPage(request));

        } catch (Exception e) {
            logger.error("Error processing manual payment claim for request {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Internal server error"));
        }
    }

    /**
     * Get request status page
     */
    @GetMapping("/status/{requestId}")
    public ResponseEntity<String> getRequestStatus(@PathVariable("requestId") String requestId) {

        logger.info("Getting status for request: {}", requestId);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            PaymentRequestDAO.createTableIfNotExists(connection);
            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);

            if (request == null) {
                return ResponseEntity.status(404)
                        .header("Content-Type", "text/html")
                        .body(createErrorPage("Request not found"));
            }

            return ResponseEntity.ok()
                    .header("Content-Type", "text/html")
                    .body(createStatusPage(request));

        } catch (Exception e) {
            logger.error("Error getting request status for {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500)
                    .header("Content-Type", "text/html")
                    .body(createErrorPage("Internal server error"));
        }
    }

    /**
     * Create success page with auto-redirect to Torn
     */
    private String createSuccessRedirectPage(PaymentRequest request, String tornPaymentUrl) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Payment Request Claimed - OC2 System</title>\n" +
                "    <meta http-equiv=\"refresh\" content=\"1;url=" + tornPaymentUrl + "\">\n" +
                "    <style>\n" +
                "        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f5f5f5; }\n" +
                "        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }\n" +
                "        .success { color: #28a745; font-size: 48px; margin-bottom: 20px; }\n" +
                "        .amount { font-size: 24px; font-weight: bold; color: #007bff; margin: 15px 0; }\n" +
                "        .details { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }\n" +
                "        .countdown { font-size: 18px; color: #6c757d; margin: 20px 0; }\n" +
                "        .manual-link { margin-top: 20px; }\n" +
                "        .manual-link a { color: #28a745; text-decoration: none; font-weight: bold; }\n" +
                "        .manual-link a:hover { text-decoration: underline; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"success\">✅</div>\n" +
                "        <h1>Payment Request Claimed!</h1>\n" +
                "        <div class=\"amount\">$" + String.format("%,d", request.getItemValue()) + "</div>\n" +
                "        <div class=\"details\">\n" +
                "            <strong>Recipient:</strong> " + request.getUsername() + " [" + request.getUserId() + "]<br>\n" +
                "            <strong>Item:</strong> " + request.getItemRequired() + "<br>\n" +
                "            <strong>Request ID:</strong> " + request.getRequestId() + "\n" + // Show full ID
                "        </div>\n" +
                "        <div class=\"countdown\">Redirecting to Torn in <span id=\"countdown\">1</span> second...</div>\n" +
                "        <div class=\"manual-link\">\n" +
                "            <a href=\"" + tornPaymentUrl + "\">Click here if not redirected automatically</a>\n" +
                "        </div>\n" +
                "        <script>\n" +
                "            let seconds = 1;\n" +
                "            const countdown = document.getElementById('countdown');\n" +
                "            const timer = setInterval(() => {\n" +
                "                seconds--;\n" +
                "                countdown.textContent = seconds;\n" +
                "                if (seconds <= 0) {\n" +
                "                    clearInterval(timer);\n" +
                "                    countdown.textContent = 'now';\n" +
                "                }\n" +
                "            }, 1000);\n" +
                "        </script>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

    /**
     * Create manual payment instructions page
     */
    private String createManualPaymentPage(PaymentRequest request) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Manual Payment Instructions - OC2 System</title>\n" +
                "    <style>\n" +
                "        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f5f5f5; }\n" +
                "        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }\n" +
                "        .manual { color: #ffc107; font-size: 48px; text-align: center; margin-bottom: 20px; }\n" +
                "        .amount { font-size: 24px; font-weight: bold; color: #007bff; margin: 15px 0; text-align: center; }\n" +
                "        .details { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }\n" +
                "        .instructions { background: #e9ecef; padding: 15px; border-radius: 5px; margin: 20px 0; }\n" +
                "        .steps { list-style: none; padding: 0; }\n" +
                "        .steps li { margin: 10px 0; padding: 10px; background: white; border-radius: 5px; border-left: 4px solid #007bff; }\n" +
                "        .torn-link { text-align: center; margin: 20px 0; }\n" +
                "        .torn-link a { background: #dc3545; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold; }\n" +
                "        .torn-link a:hover { background: #c82333; }\n" +
                "        .warning { color: #dc3545; font-weight: bold; text-align: center; margin: 20px 0; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"manual\">📋</div>\n" +
                "        <h1 style=\"text-align: center;\">Manual Payment Claimed</h1>\n" +
                "        <div class=\"amount\">$" + String.format("%,d", request.getItemValue()) + "</div>\n" +
                "        <div class=\"details\">\n" +
                "            <strong>Recipient:</strong> " + request.getUsername() + " [" + request.getUserId() + "]<br>\n" +
                "            <strong>Item:</strong> " + request.getItemRequired() + "<br>\n" +
                "            <strong>Request ID:</strong> " + request.getRequestId() + "\n" + // Show full ID
                "        </div>\n" +
                "        <div class=\"instructions\">\n" +
                "            <h3>Payment Instructions:</h3>\n" +
                "            <ol class=\"steps\">\n" +
                "                <li><strong>Step 1:</strong> Click the button below to go to Torn faction controls</li>\n" +
                "                <li><strong>Step 2:</strong> Navigate to 'Give to User' section</li>\n" +
                "                <li><strong>Step 3:</strong> Enter user ID: <strong>" + request.getUserId() + "</strong></li>\n" +
                "                <li><strong>Step 4:</strong> Enter amount: <strong>$" + String.format("%,d", request.getItemValue()) + "</strong></li>\n" +
                "                <li><strong>Step 5:</strong> Add note: <strong>OC2 Item Payment - " + request.getItemRequired() + "</strong></li>\n" +
                "                <li><strong>Step 6:</strong> Send the payment</li>\n" +
                "            </ol>\n" +
                "        </div>\n" +
                "        <div class=\"torn-link\">\n" +
                "            <a href=\"https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user\" target=\"_blank\">Go to Torn Faction Controls</a>\n" +
                "        </div>\n" +
                "        <div class=\"warning\">\n" +
                "            ⚠️ This request expires in 1 hour\n" +
                "        </div>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

    /**
     * Create already claimed page
     */
    private String createAlreadyClaimedPage(String requestId) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Request Already Claimed - OC2 System</title>\n" +
                "    <style>\n" +
                "        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f5f5f5; }\n" +
                "        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }\n" +
                "        .warning { color: #ffc107; font-size: 48px; margin-bottom: 20px; }\n" +
                "        .message { font-size: 18px; color: #6c757d; margin: 20px 0; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"warning\">⚠️</div>\n" +
                "        <h1>Request Already Claimed</h1>\n" +
                "        <div class=\"message\">\n" +
                "            This payment request has already been claimed by someone else.<br>\n" +
                "            Request ID: " + requestId + "\n" + // Show full ID
                "        </div>\n" +
                "        <p>If you believe this is an error, please contact your faction leadership.</p>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

    /**
     * Create status page
     */
    private String createStatusPage(PaymentRequest request) {
        String statusColor = getStatusColor(request.getStatus());
        String statusText = getStatusText(request.getStatus());

        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Payment Request Status - OC2 System</title>\n" +
                "    <style>\n" +
                "        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f5f5f5; }\n" +
                "        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }\n" +
                "        .status { color: " + statusColor + "; font-size: 24px; font-weight: bold; text-align: center; margin: 20px 0; }\n" +
                "        .details { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }\n" +
                "        .detail-row { margin: 8px 0; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <h1 style=\"text-align: center;\">Payment Request Status</h1>\n" +
                "        <div class=\"status\">" + statusText + "</div>\n" +
                "        <div class=\"details\">\n" +
                "            <div class=\"detail-row\"><strong>Request ID:</strong> " + request.getRequestId() + "</div>\n" + // Show full ID
                "            <div class=\"detail-row\"><strong>Recipient:</strong> " + request.getUsername() + " [" + request.getUserId() + "]</div>\n" +
                "            <div class=\"detail-row\"><strong>Item:</strong> " + request.getItemRequired() + "</div>\n" +
                "            <div class=\"detail-row\"><strong>Amount:</strong> $" + String.format("%,d", request.getItemValue()) + "</div>\n" +
                "            <div class=\"detail-row\"><strong>Created:</strong> " + request.getCreatedAt() + "</div>\n" +
                (request.getClaimedAt() != null ?
                        "            <div class=\"detail-row\"><strong>Claimed:</strong> " + request.getClaimedAt() + "</div>\n" : "") +
                (request.getFulfilledAt() != null ?
                        "            <div class=\"detail-row\"><strong>Fulfilled:</strong> " + request.getFulfilledAt() + "</div>\n" : "") +
                "        </div>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }


    /**
     * Create error page
     */
    private String createErrorPage(String error) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Error - OC2 System</title>\n" +
                "    <style>\n" +
                "        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f5f5f5; }\n" +
                "        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }\n" +
                "        .error { color: #dc3545; font-size: 48px; margin-bottom: 20px; }\n" +
                "        .message { font-size: 18px; color: #6c757d; margin: 20px 0; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"error\">❌</div>\n" +
                "        <h1>Error</h1>\n" +
                "        <div class=\"message\">" + error + "</div>\n" +
                "        <p>Please try again or contact your faction leadership if the problem persists.</p>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

    private String getStatusColor(PaymentRequest.RequestStatus status) {
        switch (status) {
            case PENDING: return "#ffc107";
            case CLAIMED: return "#007bff";
            case FULFILLED: return "#28a745";
            case EXPIRED: return "#6c757d";
            case CANCELLED: return "#dc3545";
            default: return "#6c757d";
        }
    }

    private String getStatusText(PaymentRequest.RequestStatus status) {
        switch (status) {
            case PENDING: return "⏳ Pending";
            case CLAIMED: return "🔄 Claimed";
            case FULFILLED: return "✅ Fulfilled";
            case EXPIRED: return "⏰ Expired";
            case CANCELLED: return "❌ Cancelled";
            default: return "❓ Unknown";
        }
    }

    @GetMapping("/test")
    public ResponseEntity<String> healthCheck() {
        return ResponseEntity.ok()
                .header("Content-Type", "text/html")
                .body("<!DOCTYPE html><html><head><title>OC2 Payment Service</title></head>" +
                        "<body><h1>✅ OC2 Payment Service is running!</h1>" +
                        "<p>Service is healthy and ready to process payment requests.</p></body></html>");
    }
}