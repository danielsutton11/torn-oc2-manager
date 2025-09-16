package com.Torn.Helpers;

import java.util.ArrayList;
import java.util.List;

public class FactionInfo {

    private final String factionId;
    private final String dbSuffix;
    private final List<String> apiKeys;

    public FactionInfo(String factionId, String dbPrefix, List<String> apiKeys) {
        this.factionId = factionId;
        this.dbSuffix = dbPrefix;
        this.apiKeys = new ArrayList<>(apiKeys);
    }

    // Getters
    public String getFactionId() { return factionId; }
    public String getDbSuffix() { return dbSuffix; }
    public List<String> getApiKeys() { return apiKeys; }

}
