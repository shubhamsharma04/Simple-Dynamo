package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by opensam on 4/28/17.
 */

public final class SimpleUtils {

    private static final String TAG = SimpleUtils.class.getName();

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static void populateSuccessors(Integer index) {
        int succCount = 0;
        int length = SimpleDynamoProvider.isAlive.length;
        index++;
        while (succCount != 2) {
            int succ = index % length;
            SimpleDynamoProvider.succList.add(succ);
            Log.v(TAG, "succ found is: " + succ);
            succCount++;
            index++;
        }
    }

    public static List<Integer> getDestinationIndices(String key) {
        Log.v(TAG, "Finding destinations for key : " + key);
        List<Integer> destinationIndices = new ArrayList<Integer>();
        int coordIndex = getCoordIndexForKey(key);
        int length = GeneralConstants.ALL_PORTS.length;
        int succCount = 0;
        while (succCount != 3) {
            int succ = coordIndex % length;
           // if (SimpleDynamoProvider.isAlive[succ]) {
                destinationIndices.add(succ);
                Log.v(TAG, "Adding destination : " + succ + " for key : " + key);

            //}
            succCount++;
            coordIndex++;
        }
        return destinationIndices;
    }

    private static int getCoordIndexForKey(String key) {
        int length = SimpleDynamoProvider.hashedNodesList.size();
        int index = length - 1;
        if (key.compareTo(SimpleDynamoProvider.hashedNodesList.get(length - 1)) <= 0) {
            for (int i = 0; i < length; i++) {
                if (key.compareTo(SimpleDynamoProvider.hashedNodesList.get(i)) <= 0) {
                    index = i;
                    Log.v(TAG, "CoordIndex for key : " + key + " is : " + i);
                    break;
                }
            }
        } else {
            index = 0;
        }
        return index;
    }

    public static int getIndexForPort(String rmtPort) {
        int index = 0;
        String hashedKey = "";
        try {
            hashedKey = genHash(rmtPort);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
        }
        int length = SimpleDynamoProvider.hashedNodesList.size();
        for (int i = 0; i < length; i++) {
            if (SimpleDynamoProvider.hashedNodesList.get(i).equals(hashedKey)) {
                Log.v(TAG, "Found index : " + i + " for port : " + rmtPort);
                index = i;
                break;
            }
        }
        return index;
    }

    public static List<Integer> getAllRelPredecessors(int index) {
        Log.v(TAG, "Finding Predecessors for index : " + index);
        int length = SimpleDynamoProvider.hashedNodesList.size();
        List<Integer> predecessorIndices = new ArrayList<Integer>();
        int succCount = 0;
        for (int i = index - 1; succCount < 2; i--) {
            if (i < 0) {
                i = length - 1;
            }
           // if (SimpleDynamoProvider.isAlive[i]) {
                Log.v(TAG, "Adding : " + i + " as pred to : " + index);
                predecessorIndices.add(i);
            //}
            succCount++;
        }
        return predecessorIndices;
    }

    public static boolean doesBelongFromPred(String key, ArrayList<Integer> preds) {
        boolean doesBelong = false;
        int length = SimpleDynamoProvider.hashedNodesList.size();
        String hashedKey = "";
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (Integer index : preds) {
            String node = SimpleDynamoProvider.hashedNodesList.get(index);
            String pred = SimpleDynamoProvider.hashedNodesList.get(index - 1 >= 0 ? index - 1 : length - 1);
            if (index == 0) {
                if (hashedKey.compareTo(pred) > 0 || hashedKey.compareTo(node) <= 0) {
                    doesBelong = true;
                    break;
                }
            } else {
                if (hashedKey.compareTo(pred) > 0 && hashedKey.compareTo(node) <= 0) {
                    doesBelong = true;
                    break;
                }
            }
        }

        Log.v(TAG, "Key from pred : " + key + " belongs : " + doesBelong);
        return doesBelong;
    }

    public static void commitInit(String rmtPort) {
        synchronized (SimpleDynamoProvider.forSem) {
            SimpleDynamoProvider.initAliveNodes.add(rmtPort);
        }
    }

    public static boolean doesBelongFromSucc(String key, String myID, Integer myIndex) {
        boolean doesBelong = false;
        int length = SimpleDynamoProvider.hashedNodesList.size();
        String hashedKey = "";
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (myIndex == 0) {
            if (hashedKey.compareTo(SimpleDynamoProvider.hashedNodesList.get(length - 1)) > 0 || hashedKey.compareTo(myID) <= 0) {
                doesBelong = true;
            }
        } else {
            if (hashedKey.compareTo(SimpleDynamoProvider.hashedNodesList.get(myIndex - 1)) > 0 && hashedKey.compareTo(myID) <= 0) {
                doesBelong = true;
            }
        }
        Log.v(TAG, "Key from succ : " + key + " belongs : "+doesBelong);
        return doesBelong;
    }
}
