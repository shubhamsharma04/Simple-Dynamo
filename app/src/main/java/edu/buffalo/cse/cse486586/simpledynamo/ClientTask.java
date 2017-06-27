package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;

/***
 * ClientTask is an AsyncTask that should send a string over the network.
 * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
 * an enter key press event.
 *
 * @author and Credit stevko
 */
public class ClientTask extends AsyncTask<String, Void, Void> {

    private static final String TAG = ClientTask.class.getName();

    @Override
    protected Void doInBackground(String... msgs) {
        int action = Integer.parseInt(msgs[0]);
        String destinationPort = msgs[1];
        String key = msgs[2];
        Log.i(TAG, "Processing avd : " + destinationPort + " for action : " + action);
        String value = "";
        String response = "";
        switch (action) {
            case GeneralConstants.ACTION_INSERT:
                value = msgs[3];
                Log.i(TAG, "Insert key : " + key + " value : " + value + " at port : " + destinationPort);
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, value);
                break;
            case GeneralConstants.ACTION_DELETE:
                Log.i(TAG, "Sending key : " + key + " to be deleted");
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, "");
                break;
            case GeneralConstants.ACTION_PING_LIVE:
                Log.i(TAG, "Making index : " + key + " alive in node : " + destinationPort);
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, "");
                response = new String(response.replaceAll(GeneralConstants.ACK_MSG + ",", ""));
                break;
            case GeneralConstants.ACTION_INIT:
                Log.i(TAG, "Starting init process for port : " + destinationPort);
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, "");
                break;
            default:
                Log.e(TAG, "Unrecognised action : " + action);
                break;
        }
        Log.i(TAG, "Received response for action : " + action + " : " + response);
        return null;
    }

    protected String doInBackgroundWithRet(String... msgs) {
        int action = Integer.parseInt(msgs[0]);
        String destinationPort = msgs[1];
        String key = msgs[2];
        Log.i(TAG, "Processing avd : " + destinationPort + " for action : " + action);
        String value = "";
        String response = "";
        switch (action) {
            case GeneralConstants.ACTION_INSERT:
                value = msgs[3];
                Log.i(TAG, "Insert key : " + key + " value : " + value + " at port : " + destinationPort);
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, value);
                break;
            case GeneralConstants.ACTION_QUERY:
                value = msgs[3];
                Log.i(TAG, "Querying key : " + key + " with port : " + destinationPort);
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, value);
                response = new String(response.replaceAll(GeneralConstants.ACK_MSG + ",", ""));
                break;
            case GeneralConstants.ACTION_DELETE:
                Log.i(TAG, "Sending key : " + key + " to be deleted");
                response = communicateWithServer(action, SimpleDynamoProvider.myPort, destinationPort, key, "");
                break;
            default:
                Log.e(TAG, "Unrecognised action : " + action);
                break;
        }
        Log.i(TAG, "Received response for action : " + action + " : " + response);
        return response;
    }


    private String communicateWithServer(int action, String myPort, String rmtPort, String key, String value) {
        Log.i(TAG, "Inside communicateWithServer with action : " + action);
        boolean isSuccessFul = true;
        String result = "";
        String remotePort = rmtPort;
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(GeneralConstants.ACTION, String.valueOf(action));
            jsonObject.put(GeneralConstants.CLIENT_ID, myPort);
            jsonObject.put(GeneralConstants.KEY, key);
            jsonObject.put(GeneralConstants.VALUE, value);
            String msg = jsonObject.toString();
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
// Credit : Socket programming Based on the input from TA Sharath during recitation
            socket.setSoTimeout(GeneralConstants.SOCKET_TIMEOUT);
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            Log.i(TAG, "Sending message : " + msg + " to server @ : " + remotePort);
            outputStream.writeUTF(msg);
            StringBuilder str = null;
            InputStream inputStream = socket.getInputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            do {
                str = new StringBuilder(dataInputStream.readUTF());
            } while (!str.toString().startsWith(GeneralConstants.ACK_MSG));
            Log.i(TAG, "Received msg : " + str.toString());
            result = str.toString();
            outputStream.close();
            inputStream.close();
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "IOException");
            isSuccessFul = false;
        } catch (JSONException e) {
            Log.e(TAG, "JSONException");
            isSuccessFul = false;
        }
        if (action == GeneralConstants.ACTION_PING_LIVE) {
            result = result + String.valueOf(isSuccessFul);
        }
        if (!isSuccessFul) {
            if (action == GeneralConstants.ACTION_PING_LIVE || value.equals(GeneralConstants.INIT_QUERY)) {
                Log.v(TAG,"Init action. Nothing to worry");
            } else {
                int index = SimpleUtils.getIndexForPort(String.valueOf(Integer.valueOf(rmtPort)/2));
                Log.i(TAG, "Port : " + rmtPort + " at index :  "+ index+" is down");
                SimpleDynamoProvider.isAlive[index] = false;
            }
        }
        return result;
    }


}