package com.Torn.FactionCrimes.Models.ItemMarketModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemListing {
    @JsonProperty("price")
    private Integer price;

    @JsonProperty("amount")
    private Integer amount;

    public Integer getPrice() {
        return price;
    }

    public Integer getAmount() {
        return amount;
    }
}
