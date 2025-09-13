package com.Torn.PaymentRequests;

import java.sql.Timestamp;

/**
 * Model class representing a payment request for OC2 items
 */
public class PaymentRequest {

    public enum RequestStatus {
        PENDING,     // Request created, waiting to be claimed
        CLAIMED,     // Someone clicked the fulfill link
        FULFILLED,   // Payment verified in faction news
        EXPIRED,     // Request timed out or was cancelled
        CANCELLED    // Manually cancelled
    }

    private String requestId;
    private String factionId;
    private String userId;
    private String username;
    private Long crimeId;
    private String role;
    private String itemRequired;
    private Long itemValue;
    private RequestStatus status;
    private Timestamp createdAt;
    private Timestamp claimedAt;
    private String claimedByUserId;
    private Timestamp fulfilledAt;
    private String fulfilmentMethod;
    private String paymentNewsId;

    // Default constructor
    public PaymentRequest() {}

    // Constructor for creating new requests
    public PaymentRequest(String requestId, String factionId, String userId, String username,
                          Long crimeId, String role, String itemRequired, Long itemValue) {
        this.requestId = requestId;
        this.factionId = factionId;
        this.userId = userId;
        this.username = username;
        this.crimeId = crimeId;
        this.role = role;
        this.itemRequired = itemRequired;
        this.itemValue = itemValue;
        this.status = RequestStatus.PENDING;
        this.createdAt = new Timestamp(System.currentTimeMillis());
    }

    // Getters
    public String getRequestId() { return requestId; }
    public String getFactionId() { return factionId; }
    public String getUserId() { return userId; }
    public String getUsername() { return username; }
    public Long getCrimeId() { return crimeId; }
    public String getRole() { return role; }
    public String getItemRequired() { return itemRequired; }
    public Long getItemValue() { return itemValue; }
    public RequestStatus getStatus() { return status; }
    public Timestamp getCreatedAt() { return createdAt; }
    public Timestamp getClaimedAt() { return claimedAt; }
    public String getClaimedByUserId() { return claimedByUserId; }
    public Timestamp getFulfilledAt() { return fulfilledAt; }
    public String getFulfilmentMethod() { return fulfilmentMethod; }
    public String getPaymentNewsId() { return paymentNewsId; }

    // Setters
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public void setFactionId(String factionId) { this.factionId = factionId; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setUsername(String username) { this.username = username; }
    public void setCrimeId(Long crimeId) { this.crimeId = crimeId; }
    public void setRole(String role) { this.role = role; }
    public void setItemRequired(String itemRequired) { this.itemRequired = itemRequired; }
    public void setItemValue(Long itemValue) { this.itemValue = itemValue; }
    public void setStatus(RequestStatus status) { this.status = status; }
    public void setCreatedAt(Timestamp createdAt) { this.createdAt = createdAt; }
    public void setClaimedAt(Timestamp claimedAt) { this.claimedAt = claimedAt; }
    public void setClaimedByUserId(String claimedByUserId) { this.claimedByUserId = claimedByUserId; }
    public void setFulfilledAt(Timestamp fulfilledAt) { this.fulfilledAt = fulfilledAt; }
    public void setFulfilmentMethod(String fulfilmentMethod) { this.fulfilmentMethod = fulfilmentMethod; }
    public void setPaymentNewsId(String paymentNewsId) { this.paymentNewsId = paymentNewsId; }

    // Utility methods
    public boolean isExpired() {
        if (claimedAt == null) return false;
        long timeSinceClaimed = System.currentTimeMillis() - claimedAt.getTime();
        return timeSinceClaimed > (60 * 60 * 1000); // 60 minutes in milliseconds (changed from 15)
    }

    public void markAsClaimed(String claimedByUserId) {
        this.status = RequestStatus.CLAIMED;
        this.claimedAt = new Timestamp(System.currentTimeMillis());
        this.claimedByUserId = claimedByUserId;
    }

    public void markAsFulfilled(String method, String newsId) {
        this.status = RequestStatus.FULFILLED;
        this.fulfilledAt = new Timestamp(System.currentTimeMillis());
        this.fulfilmentMethod = method;
        this.paymentNewsId = newsId;
    }

    @Override
    public String toString() {
        return "PaymentRequest{" +
                "requestId='" + requestId + '\'' +
                ", factionId='" + factionId + '\'' +
                ", username='" + username + '\'' +
                ", itemRequired='" + itemRequired + '\'' +
                ", itemValue=" + itemValue +
                ", status=" + status +
                ", createdAt=" + createdAt +
                '}';
    }
}