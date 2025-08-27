package com.Torn.FactionCrimes.CrimesModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Metadata {
    @JsonProperty("links")
    private Links links;

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }
}