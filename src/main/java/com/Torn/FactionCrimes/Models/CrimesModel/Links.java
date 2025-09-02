package com.Torn.FactionCrimes.Models.CrimesModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Links {
    @JsonProperty("prev")
    private String prev;

    @JsonProperty("next")
    private String next;

    public String getPrev() {
        return prev;
    }

    public void setPrev(String prev) {
        this.prev = prev;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }
}