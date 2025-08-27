package com.Torn.FactionCrimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SlotUser {
    @JsonProperty("outcome")
    private String outcome;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("joined_at")
    private Long joinedAt;

    @JsonProperty("progress")
    private Double progress;

    @JsonProperty("item_outcome")
    private String itemOutcome;

    // Getters and setters
    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJoinedAt() {
        return joinedAt;
    }

    public void setJoinedAt(Long joinedAt) {
        this.joinedAt = joinedAt;
    }

    public Double getProgress() {
        return progress;
    }

    public void setProgress(Double progress) {
        this.progress = progress;
    }

    public String getItemOutcome() {
        return itemOutcome;
    }

    public void setItemOutcome(String itemOutcome) {
        this.itemOutcome = itemOutcome;
    }
}