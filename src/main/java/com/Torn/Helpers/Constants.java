package com.Torn.Helpers;

public class Constants {

    public static final String EXECUTE_JOB = "Execute_Job";
    public static final String EXECUTE = "Execute";

    //RAILWAY VARIABLE NAMES
    public static final String JOB_RUN_ALL_SETUP_JOBS = "RUN_ALL_SETUP_JOBS";
    public static final String JOB_VALIDATE_API_KEYS = "VALIDATE_KEYS";
    public static final String JOB_GET_FACTION_MEMBERS = "FACTION_MEMBERS";
    public static final String JOB_GET_ALL_OC_CRIMES = "GET_ALL_OC2_CRIMES";
    public static final String JOB_UPDATE_OVERVIEW_DATA = "OVERVIEW_UPDATE";
    public static final String JOB_UPDATE_COMPLETED_DATA = "COMPLETED_CRIMES";
    public static final String JOB_CHECK_USER_ITEMS = "CHECK_ITEMS";
    public static final String JOB_CHECK_AVAILABLE_CRIMES_MEMBERS = "AVAILABLE_CRIMES_MEMBERS";
    public static final String JOB_UPDATE_CRIMES_PAID_DATA = "CRIMES_PAID";
    public static final String JOB_UPDATE_UPDATE_CPR_DATA = "UPDATE_MEMBER_CPR";

    // FIXED: Use consistent naming for all environment variables
    public static final String OVERRIDE_COMPLETED_CRIMES_FROM_TIMESTAMP = "OVERRIDE_COMPLETED_CRIMES_FROM_TIMESTAMP";
    public static final String OVERRIDE_COMPLETED_CRIMES_INCREMENTAL_MINUTES = "OVERRIDE_COMPLETED_CRIMES_INCREMENTAL_MINUTES";
    public static final String OVERRIDE_PAYOUT_CRIMES_FROM_TIMESTAMP = "OVERRIDE_PAYOUT_CRIMES_FROM_TIMESTAMP";
    public static final String OVERRIDE_PAYOUT_CRIMES_LOOKBACK_HOURS = "OVERRIDE_PAYOUT_CRIMES_LOOKBACK_HOURS";

    public static final String DATABASE_URL_CONFIG = "DATABASE_URL_CONFIG";
    public static final String DATABASE_URL_OC_DATA = "DATABASE_URL_OC_DATA";

    //POSTGRES
    public static final String POSTGRES_URL = "postgresql://";
    public static final String POSTGRES_JDBC_URL = "jdbc:postgresql://";

    public static final String ROW_COUNT = "row_count";
    public static final String COLUMN_NAME_LAST_UPDATE = "last_update";

    public static final String TABLE_NAME_API_KEYS = "api_keys";
    public static final String COLUMN_NAME_API_KEY = "value";
    public static final String COLUMN_NAME_ACTIVE = "active";

    public static final String TABLE_NAME_FACTIONS = "factions";
    public static final String COLUMN_NAME_FACTION_ID = "faction_id";
    public static final String COLUMN_NAME_DB_SUFFIX = "db_suffix";
    public static final String COLUMN_NAME_OWNER_NAME = "owner_name";

    public static final String TABLE_NAME_OC2_ITEMS = "all_oc2_items";
    // Global threshold for item transfer (you can adjust this value)
    public static final int ITEM_TRANSFER_THRESHOLD = 1000000;

    public static final String TABLE_NAME_FACTION_MEMBERS = "members_";
    public static final String TABLE_NAME_OC2_CRIMES = "all_oc2_crimes";
    public static final String TABLE_NAME_OC2_CRIMES_SLOTS = "all_oc2_crimes_slots";

    public static final String TABLE_NAME_AVAILABLE_CRIMES = "a_crimes_";
    public static final String TABLE_NAME_AVAILABLE_MEMBERS = "a_members_";
    public static final String TABLE_NAME_COMPLETED_CRIMES = "c_crimes_";
    public static final String TABLE_NAME_REWARDS_CRIMES = "r_crimes_";
    public static final String TABLE_NAME_CPR = "cpr_";
    public static final String TABLE_NAME_OVERVIEW = "overview_";

    public static final String COLUMN_NAME_USER_ID = "user_id";
    public static final String COLUMN_NAME_USER_NAME = "username";
    public static final String COLUMN_NAME_CRIME_ID = "crime_id";
    public static final String COLUMN_NAME_CRIME_NAME = "crime_name";
    public static final String COLUMN_NAME_CRIME_VALUE = "crime_value";
    public static final String COLUMN_NAME_USER_IN_DISCORD = "user_in_discord";
    public static final String COLUMN_NAME_IN_ORGANISED_CRIME = "in_organised_crime";
    public static final String COLUMN_NAME_CRIME_STATUS = "crime_status";
    public static final String COLUMN_NAME_CRIME_DIFFICULTY = "crime_difficulty";
    public static final String COLUMN_NAME_ROLE = "role";
    public static final String COLUMN_NAME_CHECKPOINT_PASS_RATE = "checkpoint_pass_rate";
    public static final String COLUMN_NAME_ITEM_REQUIRED = "item_required";
    public static final String COLUMN_NAME_ITEM_IS_REUSABLE = "item_is_reusable";
    public static final String COLUMN_NAME_USER_HAS_ITEM = "user_has_item";
    public static final String COLUMN_NAME_CRIME_HAS_ALL_MEMBERS = "crime_has_all_members";
    public static final String COLUMN_NAME_CRIME_COMPLETION_DATE = "crime_completion_date";
    public static final String COLUMN_NAME_ITEM_AVERAGE_PRICE = "item_average_price";

    //CRIME STATUSES
    public static final String AVAILABLE = "available";
    public static final String PLANNING = "planning";
    public static final String RECRUITING = "recruiting";
    public static final String COMPLETED = "completed";

    //TORN API ENDPOINTS & Parameters
    public static final String API_URL_TORN_PARAMETER_JOIN_QUERY = "?";
    public static final String API_URL_TORN_PARAMETER_JOIN_AND = "&";
    public static final String API_URL_TORN_PARAMETER_OFFSET = "offset=";
    public static final String API_URL_TORN_PARAMETER_SORT = "sort=";
    public static final String API_URL_TORN_PARAMETER_CAT = "cat=";
    public static final String API_URL_TORN_PARAMETER_FROM = "from=";

    public static final String API_URL_TORN_PARAMETER_ASC = "ASC";
    public static final String API_URL_TORN_PARAMETER_DESC = "DESC";

    public static final String API_URL_TORN_BASE_URL = "https://api.torn.com/v2";

    public static final String API_URL_FACTION = API_URL_TORN_BASE_URL + "/faction";
    public static final String API_URL_FACTION_CRIMES = API_URL_FACTION + "/crimes";
    public static final String API_URL_FACTION_MEMBERS = "/members";

    public static final String API_URL_MARKET = API_URL_TORN_BASE_URL + "/market";
    public static final String API_URL_ITEM_MARKET = "/itemmarket";

    public static final String API_URL_TORN = API_URL_TORN_BASE_URL +  "/torn";
    public static final String API_URL_TORN_ORGANISED_CRIMES = API_URL_TORN + "/organizedcrimes";

    public static final String API_URL_AVAILABLE_FACTION_CRIMES = API_URL_FACTION_CRIMES +
            API_URL_TORN_PARAMETER_JOIN_QUERY + API_URL_TORN_PARAMETER_CAT + AVAILABLE +
            API_URL_TORN_PARAMETER_JOIN_AND + API_URL_TORN_PARAMETER_SORT + API_URL_TORN_PARAMETER_DESC;

    public static final String API_URL_COMPLETED_FACTION_CRIMES = API_URL_FACTION_CRIMES +
            API_URL_TORN_PARAMETER_JOIN_QUERY + API_URL_TORN_PARAMETER_CAT + COMPLETED +
            API_URL_TORN_PARAMETER_JOIN_AND + API_URL_TORN_PARAMETER_OFFSET;

    //TORN API HEADER PARAMETERS
    public static final String HEADER_ACCEPT = "accept";
    public static final String HEADER_ACCEPT_VALUE = "application/json";
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_TORN_AUTHORIZATION_VALUE = "ApiKey ";

    //TORN FACTION MEMBERS API RESPONSE NODES
    public static final String NODE_MEMBERS = "members";
    public static final String NODE_ID = "id";
    public static final String NODE_NAME = "name";
    public static final String NODE_ORGANIZED_CRIMES = "organizedcrimes";
    public static final String NODE_DIFFICULTY = "difficulty";
    public static final String NODE_SCOPE = "scope";
    public static final String NODE_COST = "cost";
    public static final String NODE_RETURN = "return";
    public static final String NODE_SLOTS = "slots";
    public static final String NODE_REQUIRED_ITEMS = "required_item";
    public static final String NODE_IS_USED = "is_used";
    public static final String NODE_MONEY = "money";
    public static final String NODE_RESPECT = "respect";
    public static final String NODE_ITEMS = "items";
    public static final String NODE_QUANTITY = "quantity";
    public static final String NODE_PERCENTAGE = "percentage";
    public static final String NODE_PAID_AT = "paid_at";

    public static final String NODE_IS_IN_OC = "is_in_oc";
    public static final String NODE_STATUS = "status";
    public static final String NODE_LEVEL = "level";
    public static final String NODE_lAST_ACTION = "last_action";

    //DISCORD
    public static final String DISCORD_BOT_TOKEN = "DISCORD_BOT_TOKEN";
    public static final String DISCORD_GUILD_ID = "DISCORD_GUILD_ID";

    //OTHER STATICS
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
}