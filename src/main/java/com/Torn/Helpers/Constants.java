package com.Torn.Helpers;

public class Constants {


    //RAILWAY VARIABLE NAMES

    public static final String JOB_VALIDATE_API_KEYS = "VALIDATE_KEYS";
    public static final String JOB_GET_FACTION_MEMBERS = "FACTION_MEMBERS";
    public static final String JOB_UPDATE_OVERVIEW_DATA = "OVERVIEW_UPDATE";
    public static final String JOB_UPDATE_COMPLETED_DATA = "COMPLETED_CRIMES";
    public static final String JOB_CHECK_USER_ITEMS = "CHECK_ITEMS";
    public static final String JOB_CHECK_AVAILABLE_CRIMES_MEMBERS = "AVAILABLE_CRIMES_MEMBERS";

    public static final String DATABASE_URL_CONFIG = "DATABASE_URL_CONFIG";
    public static final String DATABASE_URL_OC_DATA = "DATABASE_URL_OC_DATA";


    //POSTGRES
    public static final String POSTGRES_URL = "postgresql://";
    public static final String POSTGRES_JDBC_URL = "jdbc:postgresql://";

    public static final String TABLE_NAME_API_KEYS = "api_keys";
    public static final String COLUMN_NAME_API_KEY = "value";
    public static final String COLUMN_NAME_ACTIVE = "active";

    public static final String TABLE_NAME_FACTIONS = "factions";
    public static final String COLUMN_NAME_FACTION_ID = "faction_id";
    public static final String COLUMN_NAME_DB_SUFFIX = "db_suffix";
    public static final String COLUMN_NAME_OWNER_NAME = "owner_name";
    public static final String FACTION_MEMBERS_TABLE_PREFIX = "members_";

    //TORN API ENDPOINTS
    public static final String API_URL_TORN_BASE_URL = "https://api.torn.com/v2/";
    public static final String API_URL_VALIDATE_KEY = API_URL_TORN_BASE_URL + "faction/crimes";
    public static final String API_URL_FACTION = API_URL_TORN_BASE_URL + "faction/";
    public static final String API_URL_FACTION_MEMBERS = "/members?key=";
    public static final String API_URL_AVAILABLE_FACTION_CRIMES = API_URL_TORN_BASE_URL + "faction/crimes?cat=available&offset=0&sort=DESC";
    public static final String API_URL_ITEM_MARKET = API_URL_TORN_BASE_URL + "market/";
    public static final String API_URL_ITEM_MARKET_JOIN = "/itemmarket?offset=0";

    //TORN API PARAMETERS
    public static final String HEADER_ACCEPT = "accept";
    public static final String HEADER_ACCEPT_VALUE = "application/json";
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_TORN_AUTHORIZATION_VALUE = "ApiKey ";

    //TORN FACTION MEMBERS API
    public static final String MEMBERS = "members";
    public static final String ID = "id";
    public static final String NAME = "name";

    //DISCORD
    public static final String DISCORD_BOT_TOKEN = "DISCORD_BOT_TOKEN";
    public static final String DISCORD_GUILD_ID = "DISCORD_GUILD_ID";

    //CRIME STATUSES
    public static final String PLANNING = "Planning";
    public static final String RECRUITING = "Recruiting";


}
