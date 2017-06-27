package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Application;
import android.database.MatrixCursor;
import android.util.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by opensam on 4/28/17.
 */

public class SimpleDynamoContentHelper extends Application {
    private static final String TAG = SimpleDynamoContentHelper.class.getName();


    // Credit : How to get context in non-activity class in a good way http://stackoverflow.com/questions/22371124/getting-activity-context-into-a-non-activity-class-android
    private static Application instance;

    // This would be populated by this class
    public static String myID = null;

    private static Map<String, PriorityQueue<MsgVO>> map;

    private static Semaphore binaryLock;

    @Override
    public void onCreate() {
        Log.i(TAG, "SimpleDhtContentHelper created");
        super.onCreate();
        instance = this;
        map = new HashMap<String, PriorityQueue<MsgVO>>();
        binaryLock = new Semaphore(1);
        Log.i(TAG, "instance created : " + instance);
    }

    public boolean writeContent(String key, String value) {
        boolean result = true;
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                Log.i(TAG, "Lock acquired for writing");
                MsgVO msgVO = new MsgVO(value);
                PriorityQueue<MsgVO> pq = null;
                if (map.containsKey(key)) {
                    pq = map.get(key);
                    msgVO.setVersion(pq.peek().getVersion() + 1);
                } else {
                    pq = new PriorityQueue<MsgVO>();
                    msgVO.setVersion(1);
                }
                pq.add(msgVO);
                map.put(key, pq);
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key + " with value : ");
        return result;
    }

    public Map<String, MsgVO> getContentForKey(String key) {
        Map<String, MsgVO> result = new HashMap<String, MsgVO>();
        //Log.i(TAG,"key : "+key+" action : "+action);
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                //Log.i(TAG,"Lock acquired for reading");
                if (GeneralConstants.LOCAL.equals(key)) {
                    for (Map.Entry<String, PriorityQueue<MsgVO>> entry : map.entrySet()) {
                        result.put(entry.getKey(), entry.getValue().peek());
                    }
                } else {
                    PriorityQueue<MsgVO> p = map.get(key);
                    if(p==null){
                        p = new PriorityQueue<MsgVO>();
                    }
                    MsgVO value = p.peek();
                    if(value!=null) {
                        Log.i(TAG, "Returning value : " + value + " for key : " + key + " and version : " + value.getVersion());
                        result.put(key, value);
                    } else {
                        Log.w(TAG, "Returning null for key : " + key);
                    }
                }
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key );
        return result;
    }

    public String deleteContentForKey(String key) {
        String result = "S";
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                //Log.i(TAG,"Lock acquired for reading");
                if (GeneralConstants.LOCAL.equals(key)) {
                    map = new HashMap<String, PriorityQueue<MsgVO>>();
                } else {
                    map.remove(key);
                }
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
            result = "F";
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key);
        return result;
    }

    public void initialStorage(MatrixCursor currCursor) {
        boolean result = true;
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                Log.i(TAG, "Lock acquired for writing Big Chunk ");
                for (currCursor.moveToFirst(); !currCursor.isAfterLast(); currCursor.moveToNext()) {
                    String key = currCursor.getString(0);
                    String value = currCursor.getString(1);
                    String version = currCursor.getString(2);
                    MsgVO msgVO = new MsgVO(value,Integer.parseInt(version));
                    PriorityQueue<MsgVO> pq = null;
                    if (map.containsKey(key)) {
                        pq = map.get(key);
                    } else {
                        pq = new PriorityQueue<MsgVO>();
                    }
                    pq.add(msgVO);
                    map.put(key, pq);
                }
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
            result = false;
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result);
    }
}
