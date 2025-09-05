package com.Torn.Api;

/**
 * Wrapper class for API responses with detailed error categorization
 * and retry logic indicators.
 */
public class ApiResponse {

    public enum ResponseType {
        SUCCESS,
        AUTHENTICATION_ERROR,    // 401 - don't retry
        AUTHORIZATION_ERROR,     // 403 - don't retry
        NOT_FOUND,              // 404 - don't retry
        RATE_LIMITED,           // 429 - retry with longer delay
        CLIENT_ERROR,           // 4xx - don't retry
        SERVER_ERROR,           // 5xx - retry
        NETWORK_ERROR,          // IOException - retry
        CIRCUIT_BREAKER_OPEN,   // Circuit breaker triggered
        INTERRUPTED,            // Thread interrupted
        MAX_RETRIES_EXCEEDED,   // All retries failed
        UNEXPECTED_ERROR,       // Other exceptions
        UNKNOWN_ERROR          // Unknown status code
    }

    private final ResponseType type;
    private final boolean success;
    private final boolean shouldRetry;
    private final String body;
    private final String errorMessage;

    private ApiResponse(ResponseType type, boolean success, boolean shouldRetry, String body, String errorMessage) {
        this.type = type;
        this.success = success;
        this.shouldRetry = shouldRetry;
        this.body = body;
        this.errorMessage = errorMessage;
    }

    // Factory methods for different response types
    public static ApiResponse success(String body) {
        // Allow null body for successful responses with no content
        return new ApiResponse(ResponseType.SUCCESS, true, false, body, null);
    }

    public static ApiResponse authenticationError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Authentication failed";
        return new ApiResponse(ResponseType.AUTHENTICATION_ERROR, false, false, null, errorMsg);
    }

    public static ApiResponse authorizationError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Authorization failed";
        return new ApiResponse(ResponseType.AUTHORIZATION_ERROR, false, false, null, errorMsg);
    }

    public static ApiResponse notFound(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Resource not found";
        return new ApiResponse(ResponseType.NOT_FOUND, false, false, null, errorMsg);
    }

    public static ApiResponse rateLimited(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Rate limit exceeded";
        return new ApiResponse(ResponseType.RATE_LIMITED, false, true, null, errorMsg);
    }

    public static ApiResponse clientError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Client error occurred";
        return new ApiResponse(ResponseType.CLIENT_ERROR, false, false, null, errorMsg);
    }

    public static ApiResponse serverError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Server error occurred";
        return new ApiResponse(ResponseType.SERVER_ERROR, false, true, null, errorMsg);
    }

    public static ApiResponse networkError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Network error occurred";
        return new ApiResponse(ResponseType.NETWORK_ERROR, false, true, null, errorMsg);
    }

    public static ApiResponse circuitBreakerOpen() {
        return new ApiResponse(ResponseType.CIRCUIT_BREAKER_OPEN, false, false, null,
                "Circuit breaker is open - API calls suspended");
    }

    public static ApiResponse interrupted() {
        return new ApiResponse(ResponseType.INTERRUPTED, false, false, null,
                "Operation was interrupted");
    }

    public static ApiResponse maxRetriesExceeded() {
        return new ApiResponse(ResponseType.MAX_RETRIES_EXCEEDED, false, false, null,
                "Maximum retry attempts exceeded");
    }

    public static ApiResponse unexpectedError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Unexpected error occurred";
        return new ApiResponse(ResponseType.UNEXPECTED_ERROR, false, true, null, errorMsg);
    }

    public static ApiResponse unknownError(String message) {
        String errorMsg = (message != null && !message.trim().isEmpty()) ? message : "Unknown error occurred";
        return new ApiResponse(ResponseType.UNKNOWN_ERROR, false, true, null, errorMsg);
    }

    // Getters
    public ResponseType getType() { return type; }
    public boolean isSuccess() { return success; }
    public boolean shouldRetry() { return shouldRetry; }
    public String getBody() { return body; }
    public String getErrorMessage() { return errorMessage; }

    // Helper methods
    public boolean isRetryableError() {
        return shouldRetry && !success;
    }

    public boolean isFatalError() {
        return !success && !shouldRetry && type != ResponseType.CIRCUIT_BREAKER_OPEN;
    }

    public boolean isTemporaryError() {
        return type == ResponseType.RATE_LIMITED ||
                type == ResponseType.SERVER_ERROR ||
                type == ResponseType.NETWORK_ERROR ||
                type == ResponseType.CIRCUIT_BREAKER_OPEN;
    }

    public boolean isAuthenticationIssue() {
        return type == ResponseType.AUTHENTICATION_ERROR ||
                type == ResponseType.AUTHORIZATION_ERROR;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ApiResponse{type=").append(type);
        sb.append(", success=").append(success);
        sb.append(", shouldRetry=").append(shouldRetry);

        if (errorMessage != null) {
            sb.append(", error='").append(errorMessage).append("'");
        }

        if (body != null && !body.isEmpty()) {
            int previewLength = Math.min(body.length(), 100);
            sb.append(", bodyPreview='").append(body, 0, previewLength);
            if (body.length() > 100) {
                sb.append("...");
            }
            sb.append("'");
        }

        sb.append("}");
        return sb.toString();
    }
}