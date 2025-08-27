package com.Torn.FactionCrimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Slot {
    @JsonProperty("position")
    private String position;

    @JsonProperty("position_id")
    private String positionId;

    @JsonProperty("position_number")
    private Integer positionNumber;

    @JsonProperty("item_requirement")
    private ItemRequirement itemRequirement;

    @JsonProperty("user")
    private SlotUser user;

    @JsonProperty("checkpoint_pass_rate")
    private Integer checkpointPassRate;

    // Getters and setters
    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public Integer getPositionNumber() {
        return positionNumber;
    }

    public void setPositionNumber(Integer positionNumber) {
        this.positionNumber = positionNumber;
    }

    public ItemRequirement getItemRequirement() {
        return itemRequirement;
    }

    public void setItemRequirement(ItemRequirement itemRequirement) {
        this.itemRequirement = itemRequirement;
    }

    public SlotUser getUser() {
        return user;
    }

    public void setUser(SlotUser user) {
        this.user = user;
    }

    public Integer getCheckpointPassRate() {
        return checkpointPassRate;
    }

    public void setCheckpointPassRate(Integer checkpointPassRate) {
        this.checkpointPassRate = checkpointPassRate;
    }
}