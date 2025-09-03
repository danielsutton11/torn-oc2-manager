package com.Torn.FactionCrimes.Models.CrimesModel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Crime {
    @JsonProperty("id")
    private Long id;

    @JsonProperty("previous_crime_id")
    private Long previousCrimeId;

    @JsonProperty("name")
    private String name;

    @JsonProperty("difficulty")
    private Integer difficulty;

    @JsonProperty("status")
    private String status;

    @JsonProperty("created_at")
    private Long createdAt;

    @JsonProperty("planning_at")
    private Long planningAt;

    @JsonProperty("executed_at")
    private Long executedAt;

    @JsonProperty("ready_at")
    private Long readyAt;

    @JsonProperty("expired_at")
    private Long expiredAt;

    @JsonProperty("slots")
    private List<Slot> slots;

    @JsonProperty("rewards")
    private Object rewards; // Can be null or complex object

    // Getters and setters
    public Long getId() {
        return id;
    }

    public Long getPreviousCrimeId() {
        return previousCrimeId;
    }

    public String getName() {
        return name;
    }

    public Integer getDifficulty() {
        return difficulty;
    }

    public String getStatus() {
        return status;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public Long getPlanningAt() {
        return planningAt;
    }

    public Long getExecutedAt() {
        return executedAt;
    }

    public Long getReadyAt() {
        return readyAt;
    }

    public Long getExpiredAt() {
        return expiredAt;
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public Object getRewards() {
        return rewards;
    }

}