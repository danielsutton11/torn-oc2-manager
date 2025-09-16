package com.Torn.Api;

import com.Torn.Helpers.Constants;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * Robust API client for Torn API with retry logic, circuit breaker pattern,
 * and graceful handling of maintenance windows and outages.
 */
public class TornApiHandler {

    private static final Logger logger = LoggerFactory.getLogger(TornApiHandler.class);

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 2000; // 2 seconds
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_RETRY_DELAY_MS = 30000; // 30 seconds

    // Circuit breaker configuration
    private static final int CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_BREAKER_TIMEOUT_MS = 300000; // 5 minutes

    // API rate limiting
    private static final long RATE_LIMIT_DELAY_MS = 2000; // 2 seconds between calls
    private static long lastApiCall = 0;

    // Circuit breaker state
    private static int consecutiveFailures = 0;
    private static LocalDateTime circuitBreakerOpenTime = null;
    private static boolean circuitOpen = false;

    // HTTP client with appropriate timeouts
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(45, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(false) // We'll handle retries ourselves
            .build();

    /**
     * Execute an API request with full retry logic and error handling
     */
    public static ApiResponse executeRequest(String url, String apiKey) {
        return executeRequest(url, apiKey, MAX_RETRIES);
    }

    /**
     * Execute an API request with custom retry count
     */
    public static ApiResponse executeRequest(String url, String apiKey, int maxRetries) {
        // Check circuit breaker
        if (isCircuitOpen()) {
            logger.warn("Circuit breaker is OPEN - skipping API call to prevent further failures");
            return ApiResponse.circuitBreakerOpen();
        }

        // Enforce rate limiting
        enforceRateLimit();

        String maskedUrl = maskApiKey(url);
        logger.debug("Executing API request to: {}", maskedUrl);

        ApiResponse response = null;
        int attempt = 0;
        long retryDelay = INITIAL_RETRY_DELAY_MS;

        while (attempt <= maxRetries) {
            attempt++;

            try {
                Request request = new Request.Builder()
                        .url(url)
                        .addHeader(Constants.HEADER_ACCEPT, Constants.HEADER_ACCEPT_VALUE)
                        .addHeader(Constants.HEADER_AUTHORIZATION, Constants.HEADER_TORN_AUTHORIZATION_VALUE + apiKey)
                        .build();

                try (Response httpResponse = client.newCall(request).execute()) {
                    response = handleHttpResponse(httpResponse, maskedUrl, attempt, maxRetries);

                    if (response.isSuccess()) {
                        onApiSuccess();
                        return response;
                    } else if (!response.shouldRetry()) {
                        // Don't retry for certain errors (401, 403, etc.)
                        onApiFailure();
                        return response;
                    }

                } catch (IOException e) {
                    response = ApiResponse.networkError(e.getMessage());
                    logger.warn("Network error on attempt {} for {}: {}", attempt, maskedUrl, e.getMessage());
                }

            } catch (Exception e) {
                response = ApiResponse.unexpectedError(e.getMessage());
                logger.error("Unexpected error on attempt {} for {}: {}", attempt, maskedUrl, e.getMessage(), e);
            }

            // If we have more retries left, wait and try again
            if (attempt <= maxRetries && response.shouldRetry()) {
                logger.info("Retrying API call to {} in {}ms (attempt {}/{})",
                        maskedUrl, retryDelay, attempt, maxRetries + 1);

                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Retry interrupted for {}", maskedUrl);
                    return ApiResponse.interrupted();
                }

                retryDelay = Math.min((long)(retryDelay * BACKOFF_MULTIPLIER), MAX_RETRY_DELAY_MS);
            }
        }

        // All retries exhausted
        onApiFailure();
        logger.error("All {} retry attempts failed for {}", maxRetries + 1, maskedUrl);
        return response != null ? response : ApiResponse.maxRetriesExceeded();
    }

    /**
     * Handle HTTP response and categorize the result
     */
    private static ApiResponse handleHttpResponse(Response httpResponse, String maskedUrl, int attempt, int maxRetries) throws IOException {
        int statusCode = httpResponse.code();
        String responseBody = null;

        if (httpResponse.body() != null) {
            responseBody = httpResponse.body().string();
        }

        logger.debug("API response for {} (attempt {}): HTTP {}", maskedUrl, attempt, statusCode);

        // Success cases
        if (statusCode >= 200 && statusCode < 300) {
            return ApiResponse.success(responseBody);
        }

        // Client errors (don't retry)
        if (statusCode == 401) {
            logger.error("API key invalid or expired for {}", maskedUrl);
            return ApiResponse.authenticationError("Invalid or expired API key");
        }

        if (statusCode == 403) {
            logger.error("API key lacks required permissions for {}", maskedUrl);
            return ApiResponse.authorizationError("Insufficient API key permissions");
        }

        if (statusCode == 404) {
            logger.error("API endpoint not found: {}", maskedUrl);
            return ApiResponse.notFound("API endpoint not found");
        }

        // Rate limiting (retry with longer delay)
        if (statusCode == 429) {
            logger.warn("Rate limited by Torn API for {} (attempt {})", maskedUrl, attempt);
            return ApiResponse.rateLimited("API rate limit exceeded");
        }

        // Server errors and maintenance (retry)
        if (statusCode >= 500) {
            logger.warn("Torn API server error {} for {} (attempt {})", statusCode, maskedUrl, attempt);
            return ApiResponse.serverError("Torn API server error: " + statusCode);
        }

        // Other client errors (don't retry)
        if (statusCode >= 400) {
            logger.error("Client error {} for {}: {}", statusCode, maskedUrl, responseBody);
            return ApiResponse.clientError("Client error: " + statusCode);
        }

        // Unknown status code
        logger.error("Unknown status code {} for {}", statusCode, maskedUrl);
        return ApiResponse.unknownError("Unknown status code: " + statusCode);
    }

    /**
     * Enforce rate limiting between API calls
     */
    private static void enforceRateLimit() {
        long now = System.currentTimeMillis();
        long timeSinceLastCall = now - lastApiCall;

        if (timeSinceLastCall < RATE_LIMIT_DELAY_MS) {
            long sleepTime = RATE_LIMIT_DELAY_MS - timeSinceLastCall;
            logger.debug("Rate limiting: waiting {}ms before API call", sleepTime);

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Rate limit sleep interrupted");
            }
        }

        lastApiCall = System.currentTimeMillis();
    }

    /**
     * Check if circuit breaker is open
     */
    private static boolean isCircuitOpen() {
        if (!circuitOpen) {
            return false;
        }

        // Check if timeout has elapsed
        if (circuitBreakerOpenTime != null &&
                Duration.between(circuitBreakerOpenTime, LocalDateTime.now()).toMillis() > CIRCUIT_BREAKER_TIMEOUT_MS) {

            logger.info("Circuit breaker timeout elapsed - attempting to close circuit");
            circuitOpen = false;
            circuitBreakerOpenTime = null;
            consecutiveFailures = 0;
            return false;
        }

        return true;
    }

    /**
     * Handle successful API call
     */
    private static void onApiSuccess() {
        if (consecutiveFailures > 0) {
            logger.info("API call succeeded - resetting failure count from {}", consecutiveFailures);
            consecutiveFailures = 0;
        }

        if (circuitOpen) {
            logger.info("API call succeeded - CLOSING circuit breaker");
            circuitOpen = false;
            circuitBreakerOpenTime = null;
        }
    }

    /**
     * Handle failed API call
     */
    private static void onApiFailure() {
        consecutiveFailures++;
        logger.warn("API failure count: {}/{}", consecutiveFailures, CIRCUIT_BREAKER_FAILURE_THRESHOLD);

        if (consecutiveFailures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD && !circuitOpen) {
            logger.error("OPENING circuit breaker after {} consecutive failures", consecutiveFailures);
            circuitOpen = true;
            circuitBreakerOpenTime = LocalDateTime.now();
        }
    }

    /**
     * Get current circuit breaker status for monitoring
     */
    public static CircuitBreakerStatus getCircuitBreakerStatus() {
        return new CircuitBreakerStatus(
                circuitOpen,
                consecutiveFailures,
                circuitBreakerOpenTime,
                CIRCUIT_BREAKER_FAILURE_THRESHOLD
        );
    }

    /**
     * Manually reset circuit breaker (for testing or emergency)
     */
    public static void resetCircuitBreaker() {
        logger.info("Manually resetting circuit breaker");
        circuitOpen = false;
        circuitBreakerOpenTime = null;
        consecutiveFailures = 0;
    }

    /**
     * Mask API key in URLs for safe logging
     */
    private static String maskApiKey(String url) {
        if (url == null) return "null";
        return url.replaceAll("key=[^&]+", "key=***")
                .replaceAll("ApiKey [^\\s]+", "ApiKey ***");
    }

    /**
     * Circuit breaker status for monitoring
     */
    public static class CircuitBreakerStatus {
        private final boolean isOpen;
        private final int consecutiveFailures;
        private final LocalDateTime openTime;
        private final int failureThreshold;

        public CircuitBreakerStatus(boolean isOpen, int consecutiveFailures, LocalDateTime openTime, int failureThreshold) {
            this.isOpen = isOpen;
            this.consecutiveFailures = consecutiveFailures;
            this.openTime = openTime;
            this.failureThreshold = failureThreshold;
        }

        public boolean isOpen() { return isOpen; }
        public int getConsecutiveFailures() { return consecutiveFailures; }
        public LocalDateTime getOpenTime() { return openTime; }
        public int getFailureThreshold() { return failureThreshold; }

        @Override
        public String toString() {
            return String.format("CircuitBreaker{open=%s, failures=%d/%d, openTime=%s}",
                    isOpen, consecutiveFailures, failureThreshold, openTime);
        }
    }
}