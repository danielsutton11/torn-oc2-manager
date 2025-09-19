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
                "    <meta http-equiv=\"refresh\" content=\"3;url=" + tornPaymentUrl + "\">\n" +
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --primary-color: #2563eb;\n" +
                "            --success-color: #059669;\n" +
                "            --gray-50: #f9fafb;\n" +
                "            --gray-100: #f3f4f6;\n" +
                "            --gray-600: #4b5563;\n" +
                "            --gray-800: #1f2937;\n" +
                "            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);\n" +
                "            --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" +
                "            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);\n" +
                "            min-height: 100vh;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: white;\n" +
                "            max-width: 500px;\n" +
                "            width: 100%;\n" +
                "            padding: 40px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: var(--shadow-xl);\n" +
                "            text-align: center;\n" +
                "            position: relative;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        .container::before {\n" +
                "            content: '';\n" +
                "            position: absolute;\n" +
                "            top: 0;\n" +
                "            left: 0;\n" +
                "            right: 0;\n" +
                "            height: 4px;\n" +
                "            background: linear-gradient(90deg, var(--success-color), var(--primary-color));\n" +
                "        }\n" +
                "        .success-icon {\n" +
                "            width: 80px;\n" +
                "            height: 80px;\n" +
                "            background: var(--success-color);\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            margin: 0 auto 24px;\n" +
                "            font-size: 36px;\n" +
                "            color: white;\n" +
                "            animation: pulse 2s infinite;\n" +
                "        }\n" +
                "        @keyframes pulse {\n" +
                "            0% { transform: scale(1); }\n" +
                "            50% { transform: scale(1.05); }\n" +
                "            100% { transform: scale(1); }\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 28px;\n" +
                "            font-weight: 600;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .amount {\n" +
                "            font-size: 32px;\n" +
                "            font-weight: 700;\n" +
                "            color: var(--success-color);\n" +
                "            margin: 20px 0;\n" +
                "        }\n" +
                "        .details {\n" +
                "            background: var(--gray-50);\n" +
                "            padding: 24px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "            text-align: left;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row {\n" +
                "            display: flex;\n" +
                "            justify-content: space-between;\n" +
                "            align-items: center;\n" +
                "            margin: 12px 0;\n" +
                "            padding: 8px 0;\n" +
                "            border-bottom: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row:last-child { border-bottom: none; }\n" +
                "        .detail-label {\n" +
                "            font-weight: 500;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "        .detail-value {\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-800);\n" +
                "        }\n" +
                "        .countdown-container {\n" +
                "            background: linear-gradient(135deg, var(--primary-color), #3b82f6);\n" +
                "            color: white;\n" +
                "            padding: 20px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "        }\n" +
                "        .countdown-text {\n" +
                "            font-size: 18px;\n" +
                "            font-weight: 500;\n" +
                "        }\n" +
                "        .countdown-number {\n" +
                "            font-size: 24px;\n" +
                "            font-weight: 700;\n" +
                "            margin: 8px 0;\n" +
                "        }\n" +
                "        .manual-link {\n" +
                "            margin-top: 24px;\n" +
                "        }\n" +
                "        .manual-link a {\n" +
                "            display: inline-flex;\n" +
                "            align-items: center;\n" +
                "            gap: 8px;\n" +
                "            color: var(--primary-color);\n" +
                "            text-decoration: none;\n" +
                "            font-weight: 500;\n" +
                "            padding: 12px 24px;\n" +
                "            border: 2px solid var(--primary-color);\n" +
                "            border-radius: 8px;\n" +
                "            transition: all 0.2s ease;\n" +
                "        }\n" +
                "        .manual-link a:hover {\n" +
                "            background: var(--primary-color);\n" +
                "            color: white;\n" +
                "            transform: translateY(-2px);\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "        }\n" +
                "        .logo {\n" +
                "            position: absolute;\n" +
                "            top: 16px;\n" +
                "            right: 16px;\n" +
                "            background: var(--gray-100);\n" +
                "            padding: 8px 12px;\n" +
                "            border-radius: 6px;\n" +
                "            font-size: 12px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"logo\">OC2 SYSTEM</div>\n" +
                "        <div class=\"success-icon\">✓</div>\n" +
                "        <h1>Payment Request Claimed!</h1>\n" +
                "        <div class=\"amount\">$" + String.format("%,d", request.getItemValue()) + "</div>\n" +
                "        \n" +
                "        <div class=\"details\">\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Recipient</span>\n" +
                "                <span class=\"detail-value\">" + request.getUsername() + " [" + request.getUserId() + "]</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Item</span>\n" +
                "                <span class=\"detail-value\">" + request.getItemRequired() + "</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Request ID</span>\n" +
                "                <span class=\"detail-value\">" + request.getRequestId() + "</span>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"countdown-container\">\n" +
                "            <div class=\"countdown-text\">Redirecting to Torn in</div>\n" +
                "            <div class=\"countdown-number\"><span id=\"countdown\">3</span> seconds</div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"manual-link\">\n" +
                "            <a href=\"" + tornPaymentUrl + "\">\n" +
                "                <span>→</span>\n" +
                "                Continue to Torn Manually\n" +
                "            </a>\n" +
                "        </div>\n" +
                "        \n" +
                "        <script>\n" +
                "            let seconds = 3;\n" +
                "            const countdown = document.getElementById('countdown');\n" +
                "            const timer = setInterval(() => {\n" +
                "                seconds--;\n" +
                "                countdown.textContent = seconds;\n" +
                "                if (seconds <= 0) {\n" +
                "                    clearInterval(timer);\n" +
                "                    countdown.textContent = '0';\n" +
                "                    document.querySelector('.countdown-text').textContent = 'Redirecting now...';\n" +
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
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --primary-color: #2563eb;\n" +
                "            --warning-color: #d97706;\n" +
                "            --gray-50: #f9fafb;\n" +
                "            --gray-100: #f3f4f6;\n" +
                "            --gray-600: #4b5563;\n" +
                "            --gray-800: #1f2937;\n" +
                "            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" +
                "            background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);\n" +
                "            min-height: 100vh;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: white;\n" +
                "            max-width: 700px;\n" +
                "            margin: 20px auto;\n" +
                "            padding: 40px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "            position: relative;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        .container::before {\n" +
                "            content: '';\n" +
                "            position: absolute;\n" +
                "            top: 0;\n" +
                "            left: 0;\n" +
                "            right: 0;\n" +
                "            height: 4px;\n" +
                "            background: linear-gradient(90deg, var(--warning-color), #f59e0b);\n" +
                "        }\n" +
                "        .manual-icon {\n" +
                "            width: 80px;\n" +
                "            height: 80px;\n" +
                "            background: var(--warning-color);\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            margin: 0 auto 24px;\n" +
                "            font-size: 36px;\n" +
                "            color: white;\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 28px;\n" +
                "            font-weight: 600;\n" +
                "            text-align: center;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .amount {\n" +
                "            font-size: 32px;\n" +
                "            font-weight: 700;\n" +
                "            color: var(--warning-color);\n" +
                "            text-align: center;\n" +
                "            margin: 20px 0;\n" +
                "        }\n" +
                "        .details {\n" +
                "            background: var(--gray-50);\n" +
                "            padding: 24px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row {\n" +
                "            display: flex;\n" +
                "            justify-content: space-between;\n" +
                "            align-items: center;\n" +
                "            margin: 12px 0;\n" +
                "            padding: 8px 0;\n" +
                "            border-bottom: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row:last-child { border-bottom: none; }\n" +
                "        .detail-label {\n" +
                "            font-weight: 500;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "        .detail-value {\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-800);\n" +
                "        }\n" +
                "        .instructions {\n" +
                "            margin: 32px 0;\n" +
                "        }\n" +
                "        .instructions h3 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 20px;\n" +
                "            font-weight: 600;\n" +
                "            margin-bottom: 20px;\n" +
                "        }\n" +
                "        .steps {\n" +
                "            list-style: none;\n" +
                "            padding: 0;\n" +
                "        }\n" +
                "        .step {\n" +
                "            background: white;\n" +
                "            margin: 12px 0;\n" +
                "            padding: 20px;\n" +
                "            border-radius: 12px;\n" +
                "            border-left: 4px solid var(--primary-color);\n" +
                "            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);\n" +
                "            display: flex;\n" +
                "            align-items: flex-start;\n" +
                "            gap: 16px;\n" +
                "        }\n" +
                "        .step-number {\n" +
                "            background: var(--primary-color);\n" +
                "            color: white;\n" +
                "            width: 32px;\n" +
                "            height: 32px;\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            font-weight: 600;\n" +
                "            font-size: 14px;\n" +
                "            flex-shrink: 0;\n" +
                "        }\n" +
                "        .step-content {\n" +
                "            flex: 1;\n" +
                "        }\n" +
                "        .step-title {\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-800);\n" +
                "            margin-bottom: 4px;\n" +
                "        }\n" +
                "        .step-description {\n" +
                "            color: var(--gray-600);\n" +
                "            line-height: 1.5;\n" +
                "        }\n" +
                "        .highlight {\n" +
                "            background: #fef3c7;\n" +
                "            padding: 2px 6px;\n" +
                "            border-radius: 4px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-800);\n" +
                "        }\n" +
                "        .torn-link {\n" +
                "            text-align: center;\n" +
                "            margin: 32px 0;\n" +
                "        }\n" +
                "        .torn-link a {\n" +
                "            display: inline-flex;\n" +
                "            align-items: center;\n" +
                "            gap: 12px;\n" +
                "            background: linear-gradient(135deg, #dc2626, #b91c1c);\n" +
                "            color: white;\n" +
                "            padding: 16px 32px;\n" +
                "            text-decoration: none;\n" +
                "            border-radius: 12px;\n" +
                "            font-weight: 600;\n" +
                "            font-size: 18px;\n" +
                "            transition: all 0.2s ease;\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "        }\n" +
                "        .torn-link a:hover {\n" +
                "            transform: translateY(-2px);\n" +
                "            box-shadow: 0 20px 25px -5px rgba(220, 38, 38, 0.3);\n" +
                "        }\n" +
                "        .warning-banner {\n" +
                "            background: linear-gradient(135deg, #fef3c7, #fde68a);\n" +
                "            border: 1px solid #f59e0b;\n" +
                "            color: var(--gray-800);\n" +
                "            padding: 20px;\n" +
                "            border-radius: 12px;\n" +
                "            text-align: center;\n" +
                "            font-weight: 600;\n" +
                "            margin: 32px 0;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            gap: 12px;\n" +
                "        }\n" +
                "        .logo {\n" +
                "            position: absolute;\n" +
                "            top: 16px;\n" +
                "            right: 16px;\n" +
                "            background: var(--gray-100);\n" +
                "            padding: 8px 12px;\n" +
                "            border-radius: 6px;\n" +
                "            font-size: 12px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"logo\">OC2 SYSTEM</div>\n" +
                "        <div class=\"manual-icon\">📋</div>\n" +
                "        <h1>Manual Payment Instructions</h1>\n" +
                "        <div class=\"amount\">$" + String.format("%,d", request.getItemValue()) + "</div>\n" +
                "        \n" +
                "        <div class=\"details\">\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Recipient</span>\n" +
                "                <span class=\"detail-value\">" + request.getUsername() + " [" + request.getUserId() + "]</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Item</span>\n" +
                "                <span class=\"detail-value\">" + request.getItemRequired() + "</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Request ID</span>\n" +
                "                <span class=\"detail-value\">" + request.getRequestId() + "</span>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"instructions\">\n" +
                "            <h3>Follow these steps to complete the payment:</h3>\n" +
                "            <div class=\"steps\">\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">1</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Access Torn Faction Controls</div>\n" +
                "                        <div class=\"step-description\">Click the button below to open Torn faction controls in a new tab</div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">2</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Navigate to Give to User</div>\n" +
                "                        <div class=\"step-description\">Find and click on the 'Give to User' section in the faction controls</div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">3</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Enter User ID</div>\n" +
                "                        <div class=\"step-description\">Enter the recipient's user ID: <span class=\"highlight\">" + request.getUserId() + "</span></div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">4</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Enter Payment Amount</div>\n" +
                "                        <div class=\"step-description\">Enter the exact amount: <span class=\"highlight\">$" + String.format("%,d", request.getItemValue()) + "</span></div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">5</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Add Payment Note</div>\n" +
                "                        <div class=\"step-description\">Add note: <span class=\"highlight\">OC2 Item Payment - " + request.getItemRequired() + "</span></div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "                <div class=\"step\">\n" +
                "                    <div class=\"step-number\">6</div>\n" +
                "                    <div class=\"step-content\">\n" +
                "                        <div class=\"step-title\">Complete Payment</div>\n" +
                "                        <div class=\"step-description\">Review all details and click send to complete the payment</div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"torn-link\">\n" +
                "            <a href=\"https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user\" target=\"_blank\">\n" +
                "                <span>🎯</span>\n" +
                "                Open Torn Faction Controls\n" +
                "            </a>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"warning-banner\">\n" +
                "            <span>⏰</span>\n" +
                "            This request expires in 1 hour\n" +
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
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --warning-color: #d97706;\n" +
                "            --gray-50: #f9fafb;\n" +
                "            --gray-100: #f3f4f6;\n" +
                "            --gray-600: #4b5563;\n" +
                "            --gray-800: #1f2937;\n" +
                "            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" +
                "            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);\n" +
                "            min-height: 100vh;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: white;\n" +
                "            max-width: 500px;\n" +
                "            width: 100%;\n" +
                "            padding: 40px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "            text-align: center;\n" +
                "            position: relative;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        .container::before {\n" +
                "            content: '';\n" +
                "            position: absolute;\n" +
                "            top: 0;\n" +
                "            left: 0;\n" +
                "            right: 0;\n" +
                "            height: 4px;\n" +
                "            background: linear-gradient(90deg, var(--warning-color), #f59e0b);\n" +
                "        }\n" +
                "        .warning-icon {\n" +
                "            width: 80px;\n" +
                "            height: 80px;\n" +
                "            background: var(--warning-color);\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            margin: 0 auto 24px;\n" +
                "            font-size: 36px;\n" +
                "            color: white;\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 28px;\n" +
                "            font-weight: 600;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .message {\n" +
                "            background: var(--gray-50);\n" +
                "            padding: 24px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .message-text {\n" +
                "            font-size: 16px;\n" +
                "            color: var(--gray-600);\n" +
                "            line-height: 1.6;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .request-id {\n" +
                "            background: white;\n" +
                "            padding: 12px 16px;\n" +
                "            border-radius: 8px;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "            font-family: 'Monaco', 'Menlo', monospace;\n" +
                "            font-size: 14px;\n" +
                "            color: var(--gray-800);\n" +
                "            display: flex;\n" +
                "            justify-content: space-between;\n" +
                "            align-items: center;\n" +
                "        }\n" +
                "        .help-text {\n" +
                "            color: var(--gray-600);\n" +
                "            font-size: 14px;\n" +
                "            margin-top: 24px;\n" +
                "            line-height: 1.5;\n" +
                "        }\n" +
                "        .logo {\n" +
                "            position: absolute;\n" +
                "            top: 16px;\n" +
                "            right: 16px;\n" +
                "            background: var(--gray-100);\n" +
                "            padding: 8px 12px;\n" +
                "            border-radius: 6px;\n" +
                "            font-size: 12px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"logo\">OC2 SYSTEM</div>\n" +
                "        <div class=\"warning-icon\">⚠️</div>\n" +
                "        <h1>Request Already Claimed</h1>\n" +
                "        \n" +
                "        <div class=\"message\">\n" +
                "            <div class=\"message-text\">\n" +
                "                This payment request has already been claimed by someone else.\n" +
                "            </div>\n" +
                "            <div class=\"request-id\">\n" +
                "                <span>Request ID:</span>\n" +
                "                <span>" + requestId + "</span>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"help-text\">\n" +
                "            If you believe this is an error, please contact your faction leadership for assistance.\n" +
                "        </div>\n" +
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
        String statusIcon = getStatusIcon(request.getStatus());

        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Payment Request Status - OC2 System</title>\n" +
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --primary-color: #2563eb;\n" +
                "            --success-color: #059669;\n" +
                "            --gray-50: #f9fafb;\n" +
                "            --gray-100: #f3f4f6;\n" +
                "            --gray-600: #4b5563;\n" +
                "            --gray-800: #1f2937;\n" +
                "            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" +
                "            background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);\n" +
                "            min-height: 100vh;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: white;\n" +
                "            max-width: 600px;\n" +
                "            width: 100%;\n" +
                "            padding: 40px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "            position: relative;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        .container::before {\n" +
                "            content: '';\n" +
                "            position: absolute;\n" +
                "            top: 0;\n" +
                "            left: 0;\n" +
                "            right: 0;\n" +
                "            height: 4px;\n" +
                "            background: linear-gradient(90deg, var(--primary-color), #6366f1);\n" +
                "        }\n" +
                "        .status-header {\n" +
                "            text-align: center;\n" +
                "            margin-bottom: 32px;\n" +
                "        }\n" +
                "        .status-icon {\n" +
                "            width: 80px;\n" +
                "            height: 80px;\n" +
                "            background: " + statusColor + ";\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            margin: 0 auto 24px;\n" +
                "            font-size: 36px;\n" +
                "            color: white;\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 28px;\n" +
                "            font-weight: 600;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .status-badge {\n" +
                "            display: inline-flex;\n" +
                "            align-items: center;\n" +
                "            gap: 8px;\n" +
                "            background: " + statusColor + ";\n" +
                "            color: white;\n" +
                "            padding: 12px 24px;\n" +
                "            border-radius: 50px;\n" +
                "            font-weight: 600;\n" +
                "            font-size: 16px;\n" +
                "        }\n" +
                "        .details {\n" +
                "            background: var(--gray-50);\n" +
                "            padding: 24px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row {\n" +
                "            display: flex;\n" +
                "            justify-content: space-between;\n" +
                "            align-items: center;\n" +
                "            margin: 16px 0;\n" +
                "            padding: 12px 0;\n" +
                "            border-bottom: 1px solid var(--gray-100);\n" +
                "        }\n" +
                "        .detail-row:last-child { border-bottom: none; }\n" +
                "        .detail-label {\n" +
                "            font-weight: 500;\n" +
                "            color: var(--gray-600);\n" +
                "            flex: 1;\n" +
                "        }\n" +
                "        .detail-value {\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-800);\n" +
                "            text-align: right;\n" +
                "            flex: 2;\n" +
                "        }\n" +
                "        .amount {\n" +
                "            font-size: 24px !important;\n" +
                "            color: var(--success-color) !important;\n" +
                "        }\n" +
                "        .logo {\n" +
                "            position: absolute;\n" +
                "            top: 16px;\n" +
                "            right: 16px;\n" +
                "            background: var(--gray-100);\n" +
                "            padding: 8px 12px;\n" +
                "            border-radius: 6px;\n" +
                "            font-size: 12px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"logo\">OC2 SYSTEM</div>\n" +
                "        \n" +
                "        <div class=\"status-header\">\n" +
                "            <div class=\"status-icon\">" + statusIcon + "</div>\n" +
                "            <h1>Payment Request Status</h1>\n" +
                "            <div class=\"status-badge\">" + statusText + "</div>\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"details\">\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Request ID</span>\n" +
                "                <span class=\"detail-value\">" + request.getRequestId() + "</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Recipient</span>\n" +
                "                <span class=\"detail-value\">" + request.getUsername() + " [" + request.getUserId() + "]</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Item</span>\n" +
                "                <span class=\"detail-value\">" + request.getItemRequired() + "</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Amount</span>\n" +
                "                <span class=\"detail-value amount\">$" + String.format("%,d", request.getItemValue()) + "</span>\n" +
                "            </div>\n" +
                "            <div class=\"detail-row\">\n" +
                "                <span class=\"detail-label\">Created</span>\n" +
                "                <span class=\"detail-value\">" + request.getCreatedAt() + "</span>\n" +
                "            </div>\n" +
                (request.getClaimedAt() != null ?
                        "            <div class=\"detail-row\">\n" +
                                "                <span class=\"detail-label\">Claimed</span>\n" +
                                "                <span class=\"detail-value\">" + request.getClaimedAt() + "</span>\n" +
                                "            </div>\n" : "") +
                (request.getFulfilledAt() != null ?
                        "            <div class=\"detail-row\">\n" +
                                "                <span class=\"detail-label\">Fulfilled</span>\n" +
                                "                <span class=\"detail-value\">" + request.getFulfilledAt() + "</span>\n" +
                                "            </div>\n" : "") +
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
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --error-color: #dc2626;\n" +
                "            --gray-50: #f9fafb;\n" +
                "            --gray-100: #f3f4f6;\n" +
                "            --gray-600: #4b5563;\n" +
                "            --gray-800: #1f2937;\n" +
                "            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" +
                "            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);\n" +
                "            min-height: 100vh;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: white;\n" +
                "            max-width: 500px;\n" +
                "            width: 100%;\n" +
                "            padding: 40px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: var(--shadow-lg);\n" +
                "            text-align: center;\n" +
                "            position: relative;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        .container::before {\n" +
                "            content: '';\n" +
                "            position: absolute;\n" +
                "            top: 0;\n" +
                "            left: 0;\n" +
                "            right: 0;\n" +
                "            height: 4px;\n" +
                "            background: linear-gradient(90deg, var(--error-color), #ef4444);\n" +
                "        }\n" +
                "        .error-icon {\n" +
                "            width: 80px;\n" +
                "            height: 80px;\n" +
                "            background: var(--error-color);\n" +
                "            border-radius: 50%;\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            justify-content: center;\n" +
                "            margin: 0 auto 24px;\n" +
                "            font-size: 36px;\n" +
                "            color: white;\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: var(--gray-800);\n" +
                "            font-size: 28px;\n" +
                "            font-weight: 600;\n" +
                "            margin-bottom: 16px;\n" +
                "        }\n" +
                "        .error-message {\n" +
                "            background: var(--gray-50);\n" +
                "            padding: 24px;\n" +
                "            border-radius: 12px;\n" +
                "            margin: 24px 0;\n" +
                "            border: 1px solid var(--gray-100);\n" +
                "            font-size: 16px;\n" +
                "            color: var(--gray-600);\n" +
                "            line-height: 1.6;\n" +
                "        }\n" +
                "        .help-text {\n" +
                "            color: var(--gray-600);\n" +
                "            font-size: 14px;\n" +
                "            margin-top: 24px;\n" +
                "            line-height: 1.5;\n" +
                "        }\n" +
                "        .logo {\n" +
                "            position: absolute;\n" +
                "            top: 16px;\n" +
                "            right: 16px;\n" +
                "            background: var(--gray-100);\n" +
                "            padding: 8px 12px;\n" +
                "            border-radius: 6px;\n" +
                "            font-size: 12px;\n" +
                "            font-weight: 600;\n" +
                "            color: var(--gray-600);\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <div class=\"logo\">OC2 SYSTEM</div>\n" +
                "        <div class=\"error-icon\">✕</div>\n" +
                "        <h1>Error</h1>\n" +
                "        \n" +
                "        <div class=\"error-message\">\n" +
                "            " + error + "\n" +
                "        </div>\n" +
                "        \n" +
                "        <div class=\"help-text\">\n" +
                "            Please try again or contact your faction leadership if the problem persists.\n" +
                "        </div>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

    /**
     * Get status icon for display
     */
    private String getStatusIcon(PaymentRequest.RequestStatus status) {
        switch (status) {
            case PENDING: return "⏳";
            case CLAIMED: return "🔄";
            case FULFILLED: return "✓";
            case EXPIRED: return "⏰";
            case CANCELLED: return "✕";
            default: return "❓";
        }
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