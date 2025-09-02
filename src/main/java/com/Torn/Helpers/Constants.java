package com.Torn.Helpers;

public class Constants {


    //RAILWAY VARIABLE NAMES

    public static final String JOB_VALIDATE_API_KEYS = "VALIDATE_KEYS";
    public static final String JOB_GET_FACTION_MEMBERS = "FACTION_MEMBERS";
    public static final String JOB_UPDATE_OVERVIEW_DATA = "OVERVIEW_UPDATE";
    public static final String JOB_UPDATE_COMPLETED_DATA = "COMPLETED_CRIMES";
    public static final String JOB_CHECK_USER_ITEMS = "CHECK_ITEMS";
    public static final String JOB_CHECK_AVAILABLE_CRIMES = "AVAILABLE_CRIMES";

    public static final String DATABASE_URL = "DATABASE_URL";

    public static final String DISCORD_BOT_TOKEN = "DISCORD_BOT_TOKEN";
    public static final String DISCORD_GUILD_ID = "DISCORD_GUILD_ID";


    //POSTGRES
    public static final String POSTGRES_URL = "postgresql://";
    public static final String POSTGRES_JDBC_URL = "jdbc:postgresql://";

    public static final String TABLE_NAME_API_KEYS = "api_keys";
    public static final String COLUMN_NAME_API_KEY = "value";
    public static final String COLUMN_NAME_ACTIVE = "active";

    public static final String TABLE_NAME_FACTIONS = "factions";
    public static final String COLUMN_NAME_FACTION_ID = "faction_id";
    public static final String COLUMN_NAME_DB_PREFIX = "db_prefix";
    public static final String FACTION_MEMBERS_TABLE_SUFFIX = "_faction_members";

    //TORN API ENDPOINTS
    public static final String API_URL_TORN_BASE_URL = "https://api.torn.com/v2/";
    public static final String API_URL_VALIDATE_KEY = API_URL_TORN_BASE_URL + "faction/basic";
    public static final String API_URL_FACTION_MEMBERS = "/members?key=";
    public static final String API_URL_AVAILABLE_FACTION_CRIMES = API_URL_TORN_BASE_URL + "faction/crimes?cat=available&offset=0&sort=DESC";
    public static final String API_URL_ITEM_MARKET = API_URL_TORN_BASE_URL + "market/";
    public static final String API_URL_ITEM_MARKET_JOIN = "/itemmarket?offset=0";

    //TORN API PARAMETERS
    public static final String HEADER_ACCEPT = "accept";
    public static final String HEADER_ACCEPT_VALUE = "application/json";
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_TORN_AUTHORIZATION_VALUE = "ApiKey ";

    //DISCORD API ENDPOINTS
    public static final String API_URL_DISCORD_BASE_URL = "https://discord.com/api/v10/guilds/";
    public static final String API_URL_DISCORD_MEMBERS = "/members?limit=1000";

    //DISCORD API PARAMETERS
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_CONTENT_TYPE_VALUE = "application/json";
    public static final String HEADER_DISCORD_AUTHORIZATION_VALUE = "Bot ";

    //DISCORD VALUES
    public static final String MEMBERS = "members";
    public static final String USER = "user";
    public static final String ID = "id";
    public static final String USERNAME = "username";
    public static final String NICKNAME = "nick";
    public static final String META = "meta";
    public static final String AFTER = "after";
    public static final String AFTER_JOIN = "&after=";

    //CRIME STATUSES
    public static final String PLANNING = "Planning";
    public static final String RECRUITING = "Recruiting";


}
