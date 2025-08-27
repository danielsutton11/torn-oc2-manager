package com.Torn.FactionCrimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

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

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPreviousCrimeId() {
        return previousCrimeId;
    }

    public void setPreviousCrimeId(Long previousCrimeId) {
        this.previousCrimeId = previousCrimeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getDifficulty() {
        return difficulty;
    }

    public void setDifficulty(Integer difficulty) {
        this.difficulty = difficulty;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getPlanningAt() {
        return planningAt;
    }

    public void setPlanningAt(Long planningAt) {
        this.planningAt = planningAt;
    }

    public Long getExecutedAt() {
        return executedAt;
    }

    public void setExecutedAt(Long executedAt) {
        this.executedAt = executedAt;
    }

    public Long getReadyAt() {
        return readyAt;
    }

    public void setReadyAt(Long readyAt) {
        this.readyAt = readyAt;
    }

    public Long getExpiredAt() {
        return expiredAt;
    }

    public void setExpiredAt(Long expiredAt) {
        this.expiredAt = expiredAt;
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public void setSlots(List<Slot> slots) {
        this.slots = slots;
    }

    public Object getRewards() {
        return rewards;
    }

    public void setRewards(Object rewards) {
        this.rewards = rewards;
    }
}