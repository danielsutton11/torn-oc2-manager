package com.Torn.FactionCrimes.ItemMarketModel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


public class ItemMarketResponse {

    @JsonProperty("itemmarket")
    private ItemWrapper itemWrapper;

    public Item getItemMarket() {
        return itemWrapper != null ? itemWrapper.item : null;
    }

    // Inner wrapper for the nested structure
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ItemWrapper {
        @JsonProperty("item")
        private Item item;
    }
}
