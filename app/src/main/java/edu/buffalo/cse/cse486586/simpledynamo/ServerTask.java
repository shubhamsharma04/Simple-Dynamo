package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/***
 * ServerTask is an AsyncTask that should handle incoming messages. It is created by
 * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
 * <p>
 * Please make sure you understand how AsyncTask works by reading
 * http://developer.android.com/reference/android/os/AsyncTask.html
 *
 * @author and Credit stevko
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    private static final Integer sem = 0;

    private static final String TAG = ServerTask.class.getName();

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        Log.i(TAG, "Inside receive");
        while (true) {
            try {
                Socket client = serverSocket.accept();
                Log.v(TAG, "Server started");
                client.setSoTimeout(GeneralConstants.SOCKET_TIMEOUT);
                // Credit : Socket programming Based on the input from TA Sharath during recitatio
                DataInputStream inputStream = new DataInputStream(client.getInputStream());
                StringBuilder str = new StringBuilder(inputStream.readUTF());
                Log.i(TAG, "Msg Received : " + str.toString());
                StringBuilder messageToSend = new StringBuilder(GeneralConstants.ACK_MSG);

                JSONObject jsonObject = new JSONObject(str.toString());
                int action = Integer.parseInt(jsonObject.getString(GeneralConstants.ACTION));
                String clientPort = jsonObject.getString(GeneralConstants.CLIENT_ID);
                String key = "";
                switch (action) {
                    case GeneralConstants.ACTION_INSERT:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        String value = jsonObject.getString(GeneralConstants.VALUE);
                        String insertMsgs = insertMsgs(key, value);
                        Log.i(TAG, "Insert single msg for Key : " + key + " Value : " + value);
                        break;
                    case GeneralConstants.ACTION_QUERY:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        String response = getResponseForQuery(key);
                        messageToSend.append(",").append(response);
                        Log.i(TAG, "Query for Key : " + key + " got response : " + response);
                        break;
                    case GeneralConstants.ACTION_DELETE:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        deleteMsgs(key);
                        Log.i(TAG, "Delete for Key : " + key + " processed @ :  " + SimpleDynamoProvider.myPort);
                        break;
                    case GeneralConstants.ACTION_PING_LIVE:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        setNodeAlive(key);
                        Log.i(TAG, "Action set alive for Key : " + key + " processed @ :  " + SimpleDynamoProvider.myPort);
                        break;
                    case GeneralConstants.ACTION_INIT:
                        initiateInit();
                        break;
                }
                DataOutputStream dataOutputStream = new DataOutputStream(client.getOutputStream());
                dataOutputStream.writeUTF(messageToSend.toString());
                dataOutputStream.close();
                inputStream.close();
                client.close();
            } catch (IOException e) {
                Log.e(TAG, "Can't connect ", e);
            } catch (JSONException e) {
                Log.e(TAG, "", e);
            }
        }
    }

    private void initiateInit() {
            String result = "";
            result = new SimpleDynamoProvider().initiateInit();
    }

    private void setNodeAlive(String key) {
        SimpleDynamoProvider.isAlive[Integer.parseInt(key)] = true;
    }

    private void deleteMsgs(String key) {
        new SimpleDynamoProvider().deleteKey(key);
    }

    private String getResponseForQuery(String key) {
        String result = "";
        result = new SimpleDynamoProvider().getResponseForQuery(key);
        return result;
    }

    private String insertMsgs(String key, String value) {
        String result = "";
        result = new SimpleDynamoProvider().insertSingleMsg(key, value);
        return result;
    }


}
