package com.Torn.FactionCrimes.model;

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

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean getIsReusable() {
        return isReusable;
    }

    public void setIsReusable(Boolean isReusable) {
        this.isReusable = isReusable;
    }

    public Boolean getIsAvailable() {
        return isAvailable;
    }

    public void setIsAvailable(Boolean isAvailable) {
        this.isAvailable = isAvailable;
    }
}