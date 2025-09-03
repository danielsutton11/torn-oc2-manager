package com.Torn.FactionCrimes.Models.CrimesModel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
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
    private Object itemOutcome; // Changed from String to Object to handle both strings and objects

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

    public Object getItemOutcome() {
        return itemOutcome;
    }

    // Helper method to get item outcome as string if you need it
    public String getItemOutcomeAsString() {
        if (itemOutcome == null) {
            return null;
        }
        if (itemOutcome instanceof String) {
            return (String) itemOutcome;
        }
        // If it's an object, convert to string representation
        return itemOutcome.toString();
    }
}