package com.Torn.FactionCrimes.Models.ItemMarketModel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
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
