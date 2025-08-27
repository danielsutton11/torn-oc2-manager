package com.Torn.FactionCrimes.CrimesModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemRequirement {
    @JsonProperty("id")
    private Long id;

    @JsonProperty("is_reusable")
    private Boolean isReusable;

    @JsonProperty("is_available")
    private Boolean isAvailable;

    // Getters and setters
    public Long getId() {
        return id;
    }

    public Boolean getIsReusable() {
        return isReusable;
    }

    public Boolean getIsAvailable() {
        return isAvailable;
    }

}