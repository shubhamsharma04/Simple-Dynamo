package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = SimpleDynamoProvider.class.getName();
    // This would be populated by MainActivity
    public static String emulatorId = null;
    // This would be populated by MainActivity
    public static String myPort = null;
    // This would be populated by this class
    public static String myID = null;
    public static List<String> hashedNodesList;
    public static Map<String, String> nodeToHashMap;
    private static SimpleDynamoContentHelper simpleDynamoContentHelper;
    //public static StringBuilder response;

    public static final Integer forSem = 0;

    public static Set<String> initAliveNodes;

    public static Integer myIndex = 0;
    public static boolean[] isAlive;
    public static List<Integer> succList;

    private static Uri mUri;

    private static Context context;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "Delete for key : " + selection);
        String key = selection;
        int action = 0;
        if (GeneralConstants.GLOBAL.equals(key)) {
            action = GeneralConstants.GLOBAL_ID;
        } else if (GeneralConstants.LOCAL.equals(key)) {
            action = GeneralConstants.LOCAL_ID;
        } else {
            action = GeneralConstants.DEFAULT_ID;
        }
        switch (action) {
            case GeneralConstants.DEFAULT_ID:
                String hashedKey = null;
                try {
                    hashedKey = SimpleUtils.genHash(key);
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "", e);
                    return -1;
                }
                List<Integer> destinationIndices = SimpleUtils.getDestinationIndices(hashedKey);
                for (int destinationPort : destinationIndices) {
                    contactNodesToProcessDelete(key, nodeToHashMap.get(hashedNodesList.get(destinationPort)), GeneralConstants.ACTION_DELETE);
                }
            case GeneralConstants.LOCAL_ID:
                contactNodesToProcessDelete(key, SimpleDynamoProvider.myPort, GeneralConstants.ACTION_DELETE);
                break;
            case GeneralConstants.GLOBAL_ID:
                for (String destinationPort : GeneralConstants.ALL_PORTS) {
                    contactNodesToProcessDelete(GeneralConstants.LOCAL, destinationPort, GeneralConstants.ACTION_DELETE);
                }
                break;
        }
        return 0;
    }

    private void contactNodesToProcessDelete(String key, String destinationPort, int action) {
        new ClientTask().doInBackground(String.valueOf(action), destinationPort, key);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        synchronized (forSem) {
            String key = values.getAsString(GeneralConstants.KEY);
            Log.i(TAG, "Received insert request for key : " + key);
            String hashedKey = null;
            String value = values.getAsString(GeneralConstants.VALUE);
            try {
                hashedKey = SimpleUtils.genHash(key);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "", e);
                return null;
            }

            List<Integer> destinationIndices = SimpleUtils.getDestinationIndices(hashedKey);
            for (int destinationPort : destinationIndices) {
                contactNodesToProcessAction(key, value, nodeToHashMap.get(hashedNodesList.get(destinationPort)), GeneralConstants.ACTION_INSERT);
            }
        }
        return null;
    }

    private void contactNodesToProcessAction(String key, String value, String destinationPort, int action) {
        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(action), destinationPort, key, value);
        new ClientTask().doInBackgroundWithRet(String.valueOf(action), destinationPort, key, value);
    }

    @Override
    public boolean onCreate() {
        boolean result = true;
        initAliveNodes = new HashSet<String>();
        context = getContext();
        simpleDynamoContentHelper = new SimpleDynamoContentHelper();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        emulatorId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(emulatorId) * 2));
        try {
            myID = SimpleUtils.genHash(emulatorId);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
        }
        populateHashedNodes();
        SimpleUtils.populateSuccessors(myIndex);
        try {
            ServerSocket serverSocket = new ServerSocket(GeneralConstants.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            result = false;
        }
        int length = GeneralConstants.ALL_PORTS.length - 1;
        //while (true) {
        boolean canBreak = false;
        for (String port : GeneralConstants.ALL_PORTS) {
            if (!port.equals(myPort)) {
                pingOfLife(GeneralConstants.ACTION_PING_LIVE, port, myIndex);
            }
        }
           /* synchronized (forSem) {
                if (initAliveNodes.size()==length){
                    canBreak = true;
                } *//*else {
                    Log.v(TAG,"Everyone not up");
                }*//*
            }*/
           /* if (canBreak) {
                Log.i(TAG, "Everyone up");
                break;
            }*/

        //}
        // Ping Everyone I am alive.. Hurray
        // Ask Predecessors Ask Successors
        megaInit(GeneralConstants.ACTION_INIT, myPort);
        return result;
    }

    private void megaInit(int action, String destinationPort) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(action), destinationPort, "");
    }

    private void pingOfLife(int action, String destinationPort, int myIndex) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(action), destinationPort, String.valueOf(myIndex)); // Empty key for consistency in method call
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.v(TAG, "Query for key : " + selection);
        MatrixCursor cursor = new MatrixCursor(GeneralConstants.RESPONSE_COLUMNS);
        synchronized (forSem) {
            String key = selection;
            int action = 0;
            if (GeneralConstants.GLOBAL.equals(key)) {
                action = GeneralConstants.GLOBAL_ID;
            } else if (GeneralConstants.LOCAL.equals(key)) {
                action = GeneralConstants.LOCAL_ID;
            } else {
                action = GeneralConstants.DEFAULT_ID;
            }
            MatrixCursor currCursor = new MatrixCursor(GeneralConstants.COLUMNS);
            switch (action) {
                case GeneralConstants.DEFAULT_ID:
                    String hashedKey = null;
                    try {
                        hashedKey = SimpleUtils.genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "", e);
                        return null;
                    }
                    List<Integer> destinationIndices = SimpleUtils.getDestinationIndices(hashedKey);
                    for (int destinationPort : destinationIndices) {
                        MatrixCursor currCursorG = (MatrixCursor) contactNodesToProcessQuery(key, nodeToHashMap.get(hashedNodesList.get(destinationPort)), GeneralConstants.ACTION_QUERY, GeneralConstants.NON_INIT_QUERY);
                        Log.v(TAG, "Default - Number of rows returned : " + currCursor.getCount() + " for key : " + key);
                        for (currCursorG.moveToFirst(); !currCursorG.isAfterLast(); currCursorG.moveToNext()) {
                            String k = currCursorG.getString(0);
                            String v = currCursorG.getString(1);
                            String ver = currCursorG.getString(2);
                            if (null != v) {
                                Object[] row = new Object[]{k, v, ver};
                                currCursor.addRow(row);
                            }
                        }
                    }
                    break;
                case GeneralConstants.LOCAL_ID:
                    currCursor = (MatrixCursor) contactNodesToProcessQuery(key, SimpleDynamoProvider.myPort, GeneralConstants.ACTION_QUERY, GeneralConstants.NON_INIT_QUERY);
                    Log.v(TAG, "Local - Number of rows returned : " + currCursor.getCount() + " for key : " + key);
                    break;
                case GeneralConstants.GLOBAL_ID:
                    for (String destinationPort : GeneralConstants.ALL_PORTS) {
                        MatrixCursor currCursorG = (MatrixCursor) contactNodesToProcessQuery(GeneralConstants.LOCAL, destinationPort, GeneralConstants.ACTION_QUERY, GeneralConstants.NON_INIT_QUERY);
                        Log.v(TAG, "Global - Number of rows returned : " + currCursorG.getCount() + " for key : " + key);
                        for (currCursorG.moveToFirst(); !currCursorG.isAfterLast(); currCursorG.moveToNext()) {
                            String k = currCursorG.getString(0);
                            String v = currCursorG.getString(1);
                            String ver = currCursorG.getString(2);
                            if (null != v) {
                                Object[] row = new Object[]{k, v, ver};
                                currCursor.addRow(row);
                            }
                        }
                    }
                    break;
            }

            cursor = getOnlyHighestVersionMsgs(cursor, currCursor);
        }
        return cursor;
    }

    private MatrixCursor getOnlyHighestVersionMsgs(MatrixCursor cursor, MatrixCursor currCursor) {
        Map<String, PriorityQueue<MsgVO>> valTestMap = new HashMap<String, PriorityQueue<MsgVO>>();
        for (currCursor.moveToFirst(); !currCursor.isAfterLast(); currCursor.moveToNext()) {
            String k = currCursor.getString(0);
            String val = currCursor.getString(1);
            int version = Integer.parseInt(currCursor.getString(2));
            MsgVO msgVO = new MsgVO(val, version);
            PriorityQueue<MsgVO> pq = null;
            if (valTestMap.containsKey(k)) {
                pq = valTestMap.get(k);

            } else {
                pq = new PriorityQueue<MsgVO>();
            }
            pq.add(msgVO);
            valTestMap.put(k, pq);
        }
        for (Map.Entry<String, PriorityQueue<MsgVO>> entry : valTestMap.entrySet()) {
            Object[] row = new Object[]{entry.getKey(), entry.getValue().peek().toString()};
            cursor.addRow(row);
        }
        return cursor;
    }

    private Cursor contactNodesToProcessQuery(String key, String destinationPort, int action, String value) {
        String str = new ClientTask().doInBackgroundWithRet(String.valueOf(action), destinationPort, key, value);
        MatrixCursor cursor = new MatrixCursor(GeneralConstants.COLUMNS);
        String[] response = str.split(",");
        int length = response.length;
        //Log.v(TAG, "Length of response fields : " + length);
        if (length > 2) {
            for (int i = 0; i < length; i = i + 3) {
                //Log.v(TAG, "In contactNodesToProcessQuery key : " + key + " response[i+1] : " + response[i+1] + " response[i + 2] : " + response[i + 2]);
                Object[] row = new Object[]{response[i], response[i + 1], response[i + 2]};
                cursor.addRow(row);
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void populateHashedNodes() {
        hashedNodesList = new ArrayList<String>();
        nodeToHashMap = new HashMap<String, String>();
        int length = GeneralConstants.ALL_PORTS.length;
        isAlive = new boolean[length];
        succList = new ArrayList<Integer>();
        for (int i = 0; i < length; i++) {
            isAlive[i] = true;
            try {
                String hashedValue = SimpleUtils.genHash(String.valueOf(Integer.valueOf(GeneralConstants.ALL_PORTS[i]) / 2));
                hashedNodesList.add(hashedValue);
                nodeToHashMap.put(hashedValue, GeneralConstants.ALL_PORTS[i]);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "", e);
            }
        }
        Collections.sort(hashedNodesList);
        for (int i = 0; i < length; i++) {
            Log.v(TAG, i + "th node : " + hashedNodesList.get(i));
            if (hashedNodesList.get(i).equals(myID)) {
                SimpleDynamoProvider.myIndex = i;
                Log.v(TAG, "My index is : " + i);
            }
        }
        for (Map.Entry<String, String> entry : nodeToHashMap.entrySet()) {
            Log.v(TAG, "key : " + entry.getKey() + "  Value : " + entry.getValue());
        }
    }

    public String insertSingleMsg(String key, String value) {
        simpleDynamoContentHelper.writeContent(key, value);
        return "";
    }

    public String getResponseForQuery(String key) {
        Log.i(TAG, "Query for key : " + key);
        Map<String, MsgVO> cursor = simpleDynamoContentHelper.getContentForKey(key);
        StringBuilder response = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, MsgVO> entry : cursor.entrySet()) {
            if(entry.getValue()!=null) {
                count++;
                response.append(entry.getKey());
                response.append(",");
                response.append(entry.getValue().getMsg());
                response.append(",");
                response.append(entry.getValue().getVersion());
                response.append(",");
            }
        }
        if (response.length() > 0 && response.charAt(response.length() - 1) == ',') {
            response = response.deleteCharAt(response.length() - 1);
        }
        Log.i(TAG, "Finished with cursor iteration. Returning : " + count + " rows" + " with value : " + response.toString());
        return response.toString();
    }

    public void deleteKey(String key) {
        simpleDynamoContentHelper.deleteContentForKey(key);
    }

    public String initiateInit() {
        MatrixCursor cursor = new MatrixCursor(GeneralConstants.COLUMNS);
        MatrixCursor currCursor = new MatrixCursor(GeneralConstants.COLUMNS);
        List<Integer> allRelPredecessors = SimpleUtils.getAllRelPredecessors(myIndex);
        for (int pred : allRelPredecessors) {
            Log.i(TAG,"Currently obtaining from pred : "+pred);
            MatrixCursor currCursorG = (MatrixCursor) contactNodesToProcessQuery(GeneralConstants.LOCAL, nodeToHashMap.get(hashedNodesList.get(pred)), GeneralConstants.ACTION_QUERY, GeneralConstants.INIT_QUERY);
            Log.v(TAG, "initiateInit - Number of rows returned : " + currCursorG.getCount());
            for (currCursorG.moveToFirst(); !currCursorG.isAfterLast(); currCursorG.moveToNext()) {
                String k = currCursorG.getString(0);
                String v = currCursorG.getString(1);
                String ver = currCursorG.getString(2);
                if (null != v) {
                    if (SimpleUtils.doesBelongFromPred(k, new ArrayList<Integer>(allRelPredecessors))) {
                        Object[] row = new Object[]{k, v, ver};
                        currCursor.addRow(row);
                    }
                }
            }
        }

        for(int succ : succList){
            Log.v(TAG,"Currently obtaining from succ : "+succ);
            MatrixCursor currCursorG = (MatrixCursor) contactNodesToProcessQuery(GeneralConstants.LOCAL, nodeToHashMap.get(hashedNodesList.get(succ)), GeneralConstants.ACTION_QUERY, GeneralConstants.INIT_QUERY);
            Log.v(TAG, "initiateInit - Number of rows returned : " + currCursor.getCount());
            for (currCursorG.moveToFirst(); !currCursorG.isAfterLast(); currCursorG.moveToNext()) {
                String k = currCursorG.getString(0);
                String v = currCursorG.getString(1);
                String ver = currCursorG.getString(2);
                if (null != v) {
                    // TODO Remember it is all relevant preds
                    if (SimpleUtils.doesBelongFromSucc(k,myID,myIndex)) {
                        Object[] row = new Object[]{k, v, ver};
                        currCursor.addRow(row);
                    }
                }
            }
        }


        cursor = getOnlyHighestVersionMsgsWithVersion(cursor, currCursor);
        Log.i(TAG, "Eventually obtained : " + cursor.getCount() + " rows from predecessors");
        simpleDynamoContentHelper.initialStorage(cursor);
        return "";
    }

    // TODO : Change this
    private MatrixCursor getOnlyHighestVersionMsgsWithVersion(MatrixCursor cursor, MatrixCursor currCursor) {
        Map<String, PriorityQueue<MsgVO>> valTestMap = new HashMap<String, PriorityQueue<MsgVO>>();
        for (currCursor.moveToFirst(); !currCursor.isAfterLast(); currCursor.moveToNext()) {
            String k = currCursor.getString(0);
            String val = currCursor.getString(1);
            int version = Integer.parseInt(currCursor.getString(2));
            MsgVO msgVO = new MsgVO(val, version);
            PriorityQueue<MsgVO> pq = null;
            if (valTestMap.containsKey(k)) {
                pq = valTestMap.get(k);

            } else {
                pq = new PriorityQueue<MsgVO>();
            }
            pq.add(msgVO);
            valTestMap.put(k, pq);
        }
        for (Map.Entry<String, PriorityQueue<MsgVO>> entry : valTestMap.entrySet()) {
            MsgVO msg = entry.getValue().peek();
            Object[] row = new Object[]{entry.getKey(), msg.getMsg(), msg.getVersion()};
            cursor.addRow(row);
        }
        return cursor;
    }
}
