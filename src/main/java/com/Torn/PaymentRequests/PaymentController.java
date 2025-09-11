package com.Torn.PaymentRequests;

import com.Torn.Execute;
import com.Torn.Helpers.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/payment")
public class PaymentController {

    private static final Logger logger = LoggerFactory.getLogger(PaymentController.class);

    /**
     * Handle auto-fulfill link clicks - redirects to Torn payment page
     */
    @GetMapping("/claim/{requestId}")
    public ResponseEntity<?> claimPayment(@PathVariable String requestId,
                                          @RequestParam String userId) {

        logger.info("Payment claim request: {} by user: {}", requestId, userId);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500).body(createErrorResponse("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            PaymentRequestDAO.createTableIfNotExists(connection);

            // Try to claim the request (atomic operation)
            boolean claimed = PaymentRequestDAO.claimPaymentRequest(connection, requestId, userId);

            if (!claimed) {
                logger.warn("Failed to claim payment request {} - may already be claimed", requestId);
                return ResponseEntity.status(409).body(createErrorResponse("Request already claimed or not found"));
            }

            // Get the request details for redirect
            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);
            if (request == null) {
                logger.error("Request {} not found after claiming", requestId);
                return ResponseEntity.status(404).body(createErrorResponse("Request not found"));
            }

            // Build Torn payment URL
            String tornPaymentUrl = String.format(
                    "https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user&addMoneyTo=%s&money=%d",
                    request.getUserId(),
                    request.getItemValue()
            );

            logger.info("Redirecting to Torn payment page for request {} (${} to {})",
                    requestId, request.getItemValue(), request.getUsername());

            // Return redirect response
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("redirectUrl", tornPaymentUrl);
            response.put("message", String.format("Paying $%,d to %s for %s",
                    request.getItemValue(), request.getUsername(), request.getItemRequired()));
            response.put("requestId", requestId);
            response.put("expiresIn", "15 minutes");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing payment claim for request {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500).body(createErrorResponse("Internal server error"));
        }
    }

    /**
     * Handle manual fulfill link clicks - just claims the request
     */
    @GetMapping("/manual/{requestId}")
    public ResponseEntity<?> claimManualPayment(@PathVariable String requestId,
                                                @RequestParam(required = false) String userId) {

        logger.info("Manual payment claim request: {} by user: {}", requestId, userId);

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500).body(createErrorResponse("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            PaymentRequestDAO.createTableIfNotExists(connection);

            // Use "MANUAL" as userId if not provided
            String claimUserId = userId != null ? userId : "MANUAL";

            // Try to claim the request (atomic operation)
            boolean claimed = PaymentRequestDAO.claimPaymentRequest(connection, requestId, claimUserId);

            if (!claimed) {
                logger.warn("Failed to claim manual payment request {} - may already be claimed", requestId);
                return ResponseEntity.status(409).body(createErrorResponse("Request already claimed or not found"));
            }

            // Get the request details
            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);
            if (request == null) {
                logger.error("Request {} not found after claiming", requestId);
                return ResponseEntity.status(404).body(createErrorResponse("Request not found"));
            }

            logger.info("Manual payment claimed for request {} (${} to {})",
                    requestId, request.getItemValue(), request.getUsername());

            // Return success response with payment details
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", String.format("Request claimed! Please pay $%,d to %s for %s",
                    request.getItemValue(), request.getUsername(), request.getItemRequired()));
            response.put("paymentDetails", Map.of(
                    "recipient", request.getUsername(),
                    "amount", request.getItemValue(),
                    "item", request.getItemRequired(),
                    "userId", request.getUserId()
            ));
            response.put("tornPaymentUrl", "https://www.torn.com/factions.php?step=your#/tab=controls&option=give-to-user");
            response.put("requestId", requestId);
            response.put("expiresIn", "15 minutes");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing manual payment claim for request {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500).body(createErrorResponse("Internal server error"));
        }
    }

    /**
     * Get request status (optional - for debugging)
     */
    @GetMapping("/status/{requestId}")
    public ResponseEntity<?> getRequestStatus(@PathVariable String requestId) {

        String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
        if (configDatabaseUrl == null || configDatabaseUrl.isEmpty()) {
            return ResponseEntity.status(500).body(createErrorResponse("Service configuration error"));
        }

        try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

            PaymentRequestDAO.createTableIfNotExists(connection);

            PaymentRequest request = PaymentRequestDAO.getPaymentRequest(connection, requestId);
            if (request == null) {
                return ResponseEntity.status(404).body(createErrorResponse("Request not found"));
            }

            Map<String, Object> response = new HashMap<>();
            response.put("requestId", request.getRequestId());
            response.put("status", request.getStatus().name());
            response.put("username", request.getUsername());
            response.put("itemRequired", request.getItemRequired());
            response.put("itemValue", request.getItemValue());
            response.put("createdAt", request.getCreatedAt());
            response.put("claimedAt", request.getClaimedAt());
            response.put("fulfilledAt", request.getFulfilledAt());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting request status for {}: {}", requestId, e.getMessage(), e);
            return ResponseEntity.status(500).body(createErrorResponse("Internal server error"));
        }
    }

    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("success", false);
        error.put("error", message);
        return error;
    }

    @GetMapping("/payment/test")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "healthy");
        response.put("service", "OC2 Payment Service");
        response.put("timestamp", System.currentTimeMillis());
        response.put("version", "1.0.0");

        // Test database connectivity
        try {
            String configDatabaseUrl = System.getenv(Constants.DATABASE_URL_CONFIG);
            if (configDatabaseUrl != null) {
                try (Connection connection = Execute.postgres.connect(configDatabaseUrl, logger)) {

                    PaymentRequestDAO.createTableIfNotExists(connection);
                    response.put("database", "connected");
                }
            } else {
                response.put("database", "config_missing");
            }
        } catch (Exception e) {
            response.put("database", "error: " + e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/")
    public ResponseEntity<String> root() {
        return ResponseEntity.ok("OC2 Payment Service is running!");
    }

    @GetMapping("/health")
    public ResponseEntity<String> simpleHealth() {
        return ResponseEntity.ok("OK");
    }
}