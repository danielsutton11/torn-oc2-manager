package com.Torn.FactionCrimes.CrimesModel;

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

    public Long getId() {
        return id;
    }

    public Long getJoinedAt() {
        return joinedAt;
    }

    public Double getProgress() {
        return progress;
    }

    public String getItemOutcome() {
        return itemOutcome;
    }

}