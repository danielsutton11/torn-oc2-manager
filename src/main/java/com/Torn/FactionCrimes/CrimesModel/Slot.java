package com.Torn.FactionCrimes.CrimesModel;

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

    public Integer getPositionNumber() {
        return positionNumber;
    }

    public ItemRequirement getItemRequirement() {
        return itemRequirement;
    }

    public SlotUser getUser() {
        return user;
    }

    public Integer getCheckpointPassRate() {
        return checkpointPassRate;
    }

}