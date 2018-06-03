/*
References:
 * https://developer.android.com/training/data-storage/sqlite.html
 * https://developer.android.com/reference/android/content/ContentProvider.html
 * https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
 * https://developer.android.com/reference/android/database/MatrixCursor.html
 * https://developer.android.com/reference/java/io/PrintWriter.html
 * https://docs.oracle.com/javase/7/docs/api/java/util/Collections.html
 * https://developer.android.com/reference/org/json/JSONObject.html
 * https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
*/


package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int MSG_TYPE = 0;
    static final int PARENT_NODE = 1;
    static final int PRE_NODE = 2;
    static final int SUC_NODE = 3;
    static final int PARENT_PORT = 4;
    MatrixCursor globalQueryCursor;
    MatrixCursor selQueryCursor;
    DbHelper dhtDbHelper;
    String myNodeId;
    String myPort = null;
    ArrayList<String> chordList = new ArrayList<String>();
    HashMap<String,String> nodeHM = new HashMap<String, String>();
    int predecessorNode = 0;
    int successorNode = 0;
    boolean globalDbReceived = false;
    boolean selQueryResponse = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        // TODO Auto-generated method stub
        String tableName = "dhtTable";
        String hashedKey = null;
        String successorNodeId = null;
        String successorNodePort = String.valueOf(successorNode/2);
        String predecessorNodeId = null;
        String predecessorNodePort = String.valueOf(predecessorNode/2);

        try {
            successorNodeId = genHash(successorNodePort);
            predecessorNodeId = genHash(predecessorNodePort);
            hashedKey = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(successorNode == 0 && predecessorNode == 0) {

            if(selection.equals("*") || selection.equals("@")) {
                dhtDbHelper.getWritableDatabase().delete(tableName,null,null);
            } else {
                String[] selArgs = {selection};
                dhtDbHelper.getWritableDatabase().delete(tableName,"key = ?",selArgs);
            }

        } else {
            if (selection.equals("*")) {

                Log.v("Delete: ","global delete");
                dhtDbHelper.getWritableDatabase().delete(tableName,null,null);
                String globalDelReq = "globalDelete#" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, globalDelReq, String.valueOf(myPort));

            } else if (selection.equals("@")) {

                Log.v("Delete: ","local delete");
                dhtDbHelper.getWritableDatabase().delete(tableName,null,null);

            } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                    (hashedKey.compareTo(predecessorNodeId) > 0)) {

                Log.v("Delete: ","local selected delete");
                String[] selArgs = {selection};
                dhtDbHelper.getWritableDatabase().delete(tableName,"key = ?",selArgs);

            } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                    (hashedKey.compareTo(myNodeId) < 0)) {

                Log.v("Delete: ","local selected delete");
                String[] selArgs = {selection};
                dhtDbHelper.getWritableDatabase().delete(tableName,"key = ?",selArgs);

            } else if ((predecessorNodeId.compareTo(hashedKey) < 0) &&
                    (hashedKey.compareTo(myNodeId) < 0)) {

                Log.v("Delete: ","local selected delete");
                String[] selArgs = {selection};
                dhtDbHelper.getWritableDatabase().delete(tableName,"key = ?",selArgs);

            } else {

                Log.v("Delete: ","successor selected delete");
                String selDelReq = "selDelete#" + myPort + "#"+selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selDelReq, String.valueOf(myPort));

            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String tableName = "dhtTable";
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String hashedKey = null;
        String successorNodeId = null;
        String successorNodePort = String.valueOf(successorNode/2);
        String predecessorNodeId = null;
        String predecessorNodePort = String.valueOf(predecessorNode/2);

        try {
            successorNodeId = genHash(successorNodePort);
            predecessorNodeId = genHash(predecessorNodePort);
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(successorNode == 0 && predecessorNode == 0) {
            Log.v("Insert0: ",key);
            dhtDbHelper.getWritableDatabase().insertWithOnConflict(tableName, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                (hashedKey.compareTo(predecessorNodeId) > 0)) {
            Log.v("Insert1",key);
            dhtDbHelper.getWritableDatabase().insertWithOnConflict(tableName, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                (hashedKey.compareTo(myNodeId) < 0)) {
            Log.v("Insert2",key);
            dhtDbHelper.getWritableDatabase().insertWithOnConflict(tableName, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        } else if ((predecessorNodeId.compareTo(hashedKey) < 0) &&
                (hashedKey.compareTo(myNodeId) < 0)) {
            Log.v("Insert3",key);
            dhtDbHelper.getWritableDatabase().insertWithOnConflict(tableName, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        } else {
            Log.v("Insert4", String.valueOf(successorNode));
            String insertMsg = "insert#" + successorNode + "#" + key + "#" + value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg, String.valueOf(myPort));
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dhtDbHelper = new DbHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            try {
                myNodeId = genHash(portStr);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            try {
            Log.v(TAG, "Create a ServerSocket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        Message joinMsg = new Message();
        joinMsg.msgType = "join";
        joinMsg.parentNode = Integer.parseInt(myPort);
        joinMsg.preNode = Integer.parseInt(myPort);
        joinMsg.succNode = Integer.parseInt(myPort);
        joinMsg.parentPort = Integer.parseInt(portStr);
        notifyInitialNode(joinMsg);
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        selQueryResponse = false;
        String tableName = "dhtTable";
        Cursor cursor = null;
        String predecessorNodeId = null;
        String predecessorNodePort = String.valueOf(predecessorNode/2);
        String hashedKey = null;
        try {
            predecessorNodeId = genHash(predecessorNodePort);
            if(selection.contains("#")) {
                String[] sl = selection.split("#");
                hashedKey = genHash(sl[1]);
            } else {
                hashedKey = genHash(selection);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(successorNode == 0 && predecessorNode == 0) {

            if(selection.equals("@") || selection.equals("*")) {
                String q = "SELECT * FROM dhtTable";
                cursor = dhtDbHelper.getReadableDatabase().rawQuery(q, null);
                if (cursor != null)
                    cursor.moveToFirst();
            } else {
                cursor = dhtDbHelper.getReadableDatabase().query(tableName,  new String[] {"key", "value"},
                        "key=?", new String[] {selection}, null, null, null);
            }

        } else {
            if(selection.equals("*")) {

                Log.v("Query: ","global dump query");
                String queryReq = "dbQuery#" + myPort;
                globalQueryCursor = new MatrixCursor(new String[]{"key", "value"});
                String q = "SELECT * FROM dhtTable";
                Cursor localcursor = dhtDbHelper.getReadableDatabase().rawQuery(q, null);
                localcursor.moveToFirst();
                while (!localcursor.isAfterLast()) {
                    Object[] values = {localcursor.getString(0), localcursor.getString(1)};
                    globalQueryCursor.addRow(values);
                    localcursor.moveToNext();
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryReq, String.valueOf(myPort));
                Log.v("In query", "before globalDbReceived while loop");
                while(!globalDbReceived) {

                }
                cursor = globalQueryCursor;

            } else if (selection.equals("@")) {

                Log.v("Query: ","local dump query");
                String q = "SELECT * FROM dhtTable";
                cursor = dhtDbHelper.getReadableDatabase().rawQuery(q, null);
                if (cursor != null)
                    cursor.moveToFirst();

            } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                    (hashedKey.compareTo(predecessorNodeId) > 0)) {

                Log.v("Query: ","local sel query");
                if(selection.contains("#")) {
                    String[] sl = selection.split("#");
                    selection = sl[1];
                }
                Log.v("selection",selection);
                cursor = dhtDbHelper.getReadableDatabase().query(tableName,  new String[] {"key", "value"},
                        "key=?", new String[] {selection}, null, null, null);

            } else if ((predecessorNodeId.compareTo(myNodeId) > 0) &&
                    (hashedKey.compareTo(myNodeId) < 0)) {

                Log.v("Query: ","local sel query");
                if(selection.contains("#")) {
                    String[] sl = selection.split("#");
                    selection = sl[1];
                }
                Log.v("selection",selection);
                cursor = dhtDbHelper.getReadableDatabase().query(tableName,  new String[] {"key", "value"},
                        "key=?", new String[] {selection}, null, null, null);

            } else if ((predecessorNodeId.compareTo(hashedKey) < 0) &&
                    (hashedKey.compareTo(myNodeId) < 0)) {

                Log.v("Query: ","local sel query");
                if(selection.contains("#")) {
                    String[] sl = selection.split("#");
                    selection = sl[1];
                }
                Log.v("selection",selection);
                cursor = dhtDbHelper.getReadableDatabase().query(tableName,  new String[] {"key", "value"},
                        "key=?", new String[] {selection}, null, null, null);

            } else {

                Log.v("Query: ","successor sel query");
                if(selection.contains("#")) {
                    String[] sl = selection.split("#");
                    String insertMsg = "dbSelQuery#" + sl[0] + "#" + successorNode + "#" + sl[1];
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg, String.valueOf(myPort));
                } else {
                    String insertMsg = "dbSelQuery#" + myPort + "#" + successorNode + "#" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg, String.valueOf(myPort));
                    Log.v("In query", "before selQueryResponse while loop");
                    Log.v("got the resonse", String.valueOf(selQueryResponse));
                    while(!selQueryResponse) {

                    }
                    Log.v("Query: Response - ", String.valueOf(selQueryResponse));
                    cursor = selQueryCursor;
                }

            }
        }
        return cursor;
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private  Void notifyInitialNode(Message msg) {
        String joinMsg = msg.msgType + "#" + msg.parentNode + "#" + msg.preNode + "#" + msg.succNode + "#" +
                msg.parentPort;
        Log.v("notify initial node", String.valueOf(msg.parentPort));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMsg, String.valueOf(msg.parentNode));
        return null;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while(true) {
                    Socket ss1 = serverSocket.accept();
                    DataInputStream dis = new DataInputStream(ss1.getInputStream());
                    String msg = dis.readLine();
                    PrintWriter cpw = new PrintWriter(ss1.getOutputStream(), true);

                    if (msg != null) {
                        String[] strReceived = msg.trim().split("#");
                        Message msgReceived = new Message();
                        msgReceived.msgType = strReceived[MSG_TYPE];

                        if(msgReceived.msgType.equals("join")) {

                            msgReceived.parentNode = Integer.parseInt(strReceived[PARENT_NODE]);
                            msgReceived.parentPort = Integer.parseInt(strReceived[PARENT_PORT]);
                            Log.v("server join req", String.valueOf(msgReceived.parentPort));
                            String hashedNodeId = genHash(String.valueOf(msgReceived.parentPort));
                            chordList.add(hashedNodeId);
                            nodeHM.put(hashedNodeId, String.valueOf(msgReceived.parentNode));
                            Collections.sort(chordList);
                            Message preSuccMsg = new Message();
                            preSuccMsg.msgType = "preSuccInfo";
                            preSuccMsg.parentNode = 11108;
                            preSuccMsg.preNode = 0;
                            preSuccMsg.succNode = 0;
                            preSuccMsg.parentPort = 5554;
                            Log.v("chordsize", String.valueOf(chordList.size()));
                            if(chordList.size() == 1) {
                                preSuccMsg.preNode = 0;
                                preSuccMsg.succNode = 0;
                            } else {
                                Socket[] clientSockets = new Socket[chordList.size()];
                                for (int i = 0; i < chordList.size(); i++) {

                                    int destPort = Integer.parseInt(nodeHM.get(chordList.get(i)));

                                    preSuccMsg.parentNode = destPort;

                                    if (i == 0) {
                                        preSuccMsg.preNode = Integer.parseInt(nodeHM.get(chordList.get(chordList.size() - 1)));
                                        preSuccMsg.succNode = Integer.parseInt(nodeHM.get(chordList.get(i + 1)));
                                    } else if (i == (chordList.size() - 1)) {
                                        preSuccMsg.preNode = Integer.parseInt(nodeHM.get(chordList.get(i - 1)));
                                        preSuccMsg.succNode = Integer.parseInt(nodeHM.get(chordList.get(0)));
                                    } else {
                                        preSuccMsg.preNode = Integer.parseInt(nodeHM.get(chordList.get(i - 1)));
                                        preSuccMsg.succNode = Integer.parseInt(nodeHM.get(chordList.get(i + 1)));
                                    }
                                    Log.v("dest port", String.valueOf(destPort));
                                    clientSockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            destPort);
                                    clientSockets[i].setSoTimeout(5000);
                                    //DataOutputStream cdos = new DataOutputStream(cs.getOutputStream());
                                    PrintWriter spw = new PrintWriter(clientSockets[i].getOutputStream(), true);
                                    String preSuccMsgStr = preSuccMsg.msgType + "#" +
                                            preSuccMsg.parentNode + "#" + preSuccMsg.preNode + "#"
                                            + preSuccMsg.succNode + "#" + preSuccMsg.parentPort;
                                    Log.v("c | preSuccMsgStr", preSuccMsgStr);
                                    spw.println(preSuccMsgStr);
                                }
                            }
                        }

                        if (msgReceived.msgType.equals("preSuccInfo")) {

                            predecessorNode = Integer.parseInt(strReceived[PRE_NODE]);
                            successorNode = Integer.parseInt(strReceived[SUC_NODE]);
                            Log.v("Received pre", String.valueOf(predecessorNode));
                            Log.v("Received succ", String.valueOf(successorNode));
                            dis.close();

                        } else if (msgReceived.msgType.equals("insert")) {

                            String key = strReceived[2];
                            String value = strReceived[3];
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentValues cVals = new ContentValues();
                            cVals.put("key", key);
                            cVals.put("value", value);
                            Log.v("Server: insert - ", key);
                            insert(mUri, cVals);
                            dis.close();

                        } else if (msgReceived.msgType.equals("dbQuery")) {

                            Log.v("Server: ", "dbQuery");
                            int destPort = Integer.parseInt(strReceived[1]);
                            JSONObject localDB = new JSONObject();
                            String q = "SELECT * FROM dhtTable";
                            Cursor localcursor = dhtDbHelper.getReadableDatabase().rawQuery(q, null);
                            JSONArray keyArr = new JSONArray();
                            JSONArray valueArr = new JSONArray();
                            localcursor.moveToFirst();
                            for(int j = 0; !localcursor.isAfterLast(); j++) {
                                keyArr.put(j, localcursor.getString(localcursor.getColumnIndex("key")));
                                valueArr.put(j, localcursor.getString(localcursor.getColumnIndex("value")));
                                localcursor.moveToNext();
                            }
                            localDB.put("keys", keyArr);
                            localDB.put("values", valueArr);
                            localcursor.close();
                            String dbResponse = "dbResponse#" + destPort + "#" + localDB.toString() + "#" + myPort;
                            String dbQuery = "dbQuery#" + destPort;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dbResponse, String.valueOf(myPort));
                            if(!myPort.equals(String.valueOf(destPort)))
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dbQuery, String.valueOf(myPort));

                        } else if(msgReceived.msgType.equals("dbResponse")) {

                            Log.v("Server: ", "dbResponse");
                            if(myPort.equals(strReceived[3])) {
                                globalDbReceived = true;
                                Log.v("globalDbReceived", String.valueOf(globalDbReceived));
                            }
                            String dbRes = strReceived[2];
                            Log.v("dbRes", dbRes);
                            JSONObject jsonObject = new JSONObject(dbRes);
                            JSONArray keysArray = jsonObject.getJSONArray("keys");
                            JSONArray valuesArray = jsonObject.getJSONArray("values");
                            for (int p = 0; p < keysArray.length(); p++) {
                                Object[] values = {keysArray.getString(p), valuesArray.getString(p)};
                                globalQueryCursor.addRow(values);
                            }

                        } else if(msgReceived.msgType.equals("dbSelQuery")) {

                            Log.v("Server: ", "dbSelQuery");
                            String selection = strReceived[3];
                            String destPort = strReceived[1];
                            selection = destPort+"#"+selection;
                            Cursor cursor = query(null,null,selection,null,null);
                            if(cursor != null) {
                                JSONObject localDB = new JSONObject();
                                JSONArray keyArr = new JSONArray();
                                JSONArray valueArr = new JSONArray();
                                cursor.moveToFirst();
                                for(int j = 0; !cursor.isAfterLast(); j++) {
                                    keyArr.put(j, cursor.getString(cursor.getColumnIndex("key")));
                                    valueArr.put(j, cursor.getString(cursor.getColumnIndex("value")));
                                    cursor.moveToNext();
                                }
                                localDB.put("keys", keyArr);
                                localDB.put("values", valueArr);
                                cursor.close();
                                String dbResponse = "dbSelResponse#" + destPort + "#" + localDB.toString();
                                Log.v("server db resonse",dbResponse);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dbResponse, String.valueOf(myPort));
                            }

                        } else if (msgReceived.msgType.equals("dbSelResponse")) {

                            Log.v("Server: ", "dbSelResponse");
                            String dbRes = strReceived[2];
                            Log.v("Server: db res",dbRes);
                            JSONObject jsonObject = new JSONObject(dbRes);
                            JSONArray keysArray = jsonObject.getJSONArray("keys");
                            JSONArray valuesArray = jsonObject.getJSONArray("values");
                            selQueryCursor = new MatrixCursor(new String[]{"key", "value"});
                            for (int p = 0; p < keysArray.length(); p++) {
                                Object[] values = {keysArray.getString(p), valuesArray.getString(p)};
                                selQueryCursor.addRow(values);
                            }
                            selQueryResponse = true;
                            Log.v("selQueryResponse", String.valueOf(selQueryResponse));

                        } else if (msgReceived.msgType.equals("globalDelete")) {

                            Log.v("Server: ", "globalDelete");
                            dhtDbHelper.getWritableDatabase().delete("dhtTable",null,null);
                            if(!strReceived[1].equals(String.valueOf(myPort))) {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(myPort));
                            }

                        } else if(msgReceived.msgType.equals("selDelete")) {

                            Log.v("Server: ","selDelete");
                            String selection = strReceived[2];
                            delete(null,selection,null);

                        }
                    }
                }
            } catch (Exception e) {
                Log.e(TAG, "Error in server task");
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String[] msgReceived = msgs[0].split("#");

                if(msgReceived[MSG_TYPE].equals("join")) {

                    int remotePort = 11108;
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            remotePort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: joinMsg", msgs[0]);
                    cpw.println(msgs[0]);

                } else if (msgReceived[MSG_TYPE].equals("insert")) {

                    int remotePort = Integer.parseInt(msgReceived[1]);
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            remotePort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client | insertMsg", msgs[0]);
                    cpw.println(msgs[0]);

                } else if (msgReceived[MSG_TYPE].equals("dbQuery")) {

                    int destPort = successorNode;
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            destPort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client | dbQueryMsg", msgs[0]);
                    cpw.println(msgs[0]);

                } else if (msgReceived[MSG_TYPE].equals("dbResponse")) {

                    int destPort = Integer.parseInt(msgReceived[1]);
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            destPort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: dbResponse", msgs[0]);
                    cpw.println(msgs[0]);

                } else if(msgReceived[MSG_TYPE].equals("dbSelQuery")) {

                    int destPort = successorNode;
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            destPort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: dbSelQuery", msgs[0]);
                    cpw.println(msgs[0]);

                } else if(msgReceived[MSG_TYPE].equals("dbSelResponse")) {

                    int destPort = Integer.parseInt(msgReceived[1]);
                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            destPort);
                    cs.setSoTimeout(2000);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: dbSelResponse", msgs[0]);
                    cpw.println(msgs[0]);

                } else if(msgReceived[MSG_TYPE].equals("globalDelete")) {

                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successorNode);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: globalDelete", msgs[0]);
                    cpw.println(msgs[0]);

                } else if(msgReceived[MSG_TYPE].equals("selDelete")) {

                    Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successorNode);
                    PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                    Log.v("Client: selDelete", msgs[0]);
                    cpw.println(msgs[0]);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

    }
    private class Message {
        String msgType;
        int parentNode;
        int preNode;
        int succNode;
        int parentPort;
    }
}

