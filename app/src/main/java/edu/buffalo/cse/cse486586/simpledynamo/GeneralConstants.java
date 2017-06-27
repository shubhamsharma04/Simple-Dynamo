package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by opensam on 4/28/17.
 */

public final class GeneralConstants {

    public static final String ALL_PORTS[] = new String[]{"11108", "11112", "11116", "11120", "11124"};
    public static final String ACK_MSG = "HOUSTEN_I_GOT_YOU";
    public static final int SERVER_PORT = 10000;
    public static final int N = 3;
    public static final int R = 2;
    public static final int W = 2;
    public static final int LOCAL_ID = 1;
    public static final int GLOBAL_ID = 2;
    public static final int DEFAULT_ID = 3;
    public static final String LOCAL = "@";
    public static final String GLOBAL = "*";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String VERSION = "version";
    public static final String[] COLUMNS = new String[]{KEY, VALUE, VERSION};
    public static final String[] RESPONSE_COLUMNS = new String[]{KEY, VALUE};
    public static final String ACTION = "Action";
    public static final String CLIENT_ID = "Client_Port";
    public static final String NON_INIT_QUERY = "NON_LAME_QUERY";
    public static final String INIT_QUERY = "LAME_QUERY";
    public static final int SOCKET_TIMEOUT = 1000;
    public static final int LOCK_TIMEOUT = 100;
    public static final int ACTION_INSERT = 1;
    public static final int ACTION_QUERY = 2;
    public static final int ACTION_DELETE = 3;
    public static final int ACTION_PING_LIVE = 4;
    public static final int ACTION_INIT = 5;
}
