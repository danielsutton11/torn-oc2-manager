package com.Torn.FactionCrimes.ItemMarketModel;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemMarket {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private String type;

    @JsonProperty("average_price")
    private Integer averagePrice;

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Integer getAveragePrice() {
        return averagePrice;
    }
}
