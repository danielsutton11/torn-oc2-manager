package com.Torn.FactionCrimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class TornCrimesResponse {
    @JsonProperty("crimes")
    private List<Crime> crimes;

    @JsonProperty("_metadata")
    private Metadata metadata;

    // Getters and setters
    public List<Crime> getCrimes() {
        return crimes;
    }

    public void setCrimes(List<Crime> crimes) {
        this.crimes = crimes;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}