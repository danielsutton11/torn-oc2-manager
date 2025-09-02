package com.Torn.Helpers;

public class Constants {


    //RAILWAY VARIABLE NAMES

    public static final String TORN_LIMITED_API_KEY = "TORN_LIMITED_API_KEY";
    public static final String DATABASE_URL = "DATABASE_URL";

    //POSTGRES
    public static final String POSTGRES_URL = "postgresql://";
    public static final String POSTGRES_JDBC_URL = "jdbc:postgresql://";

    //TORN API ENDPOINTS

    public static final String BASE_URL = "https://api.torn.com/v2/";
    public static final String API_URL_AVAILABLE_FACTION_CRIMES = BASE_URL + "faction/crimes?cat=available&offset=0&sort=DESC";
    public static final String API_URL_ITEM_MARKET = BASE_URL + "market/";
    public static final String API_URL_ITEM_MARKET_JOIN = "/itemmarket?offset=0";


    //API VARIABLES
    public static final String ACCEPT_HEADER = "accept";
    public static final String ACCEPT_HEADER_VALUE = "application/json";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String AUTHORIZATION_HEADER_VALUE = "ApiKey ";

    //CRIME STATUSES
    public static final String PLANNING = "Planning";
    public static final String RECRUITING = "Recruiting";


}
