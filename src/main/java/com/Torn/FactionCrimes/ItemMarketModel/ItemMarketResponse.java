package com.Torn.FactionCrimes.ItemMarketModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemMarketResponse {

    @JsonProperty("itemmarket")
    private ItemMarket itemMarket;

    public ItemMarket getItemMarket() {
        return itemMarket;
    }
}
