package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import  java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
//import java.util.Formatter;
import java.util.*;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final List<String> PORTS = new ArrayList<String>();
    //static final String[] PORTS = new String[]{REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static String successor = "";
    static String predecessor = "";
    static List<String> chord = new ArrayList<String>();
    static HashMap<String, String> address = new HashMap<String, String>();
    static final int SERVER_PORT = 10000;
    static final String sep_key_val = "@##@";
    static final String sep_msg = "@#@";
    static int status_deleteAll = 0;
    static int status_delete = 0;
    static int status_queryAll = 0;
    static int status_query = 0;
    static String status_queryAll_msg = "";
    static String status_query_msg = "";
    static String myPort;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try {
            String key = selection;

            Log.v("P1", "Received delete request");
            Log.v("P1", "Key: " + key);

            if (key.equalsIgnoreCase("@")) {
                deleteAll();
                return 1;
            } else if (key.equalsIgnoreCase("*")) {
                status_deleteAll = 0;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-All-request");
                while(status_deleteAll == 0){

                }
                return 1;
            } else {

                //Find the node where the values is stored
                String obj_id = genHash(key);

                //**Pass the obj_id around to get the destination node PORT number**
                String destPort = getDestinationNode(obj_id);
                //String destPort = myPort(); //Testing

                Log.v("P1", "Delete this from : " + destPort);

                //Send the values to the destination node
                status_delete = 0;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-request",destPort,key);

                while(status_delete == 0){

                }
                return 1;
            }

        } catch (Exception e) {
            Log.v("P1", "Exception in query");
            e.printStackTrace();
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
        try {
            String key = (String) values.get("key");
            String value = (String) values.get("value");

            Log.v("P1", "Received insert request");
            Log.v("P1", "Key: " + key);
            Log.v("P1", "Value: " + value);

            //Find the node where the values needs to be stored
            String obj_id = genHash(key);

            //**Pass the obj_id around to get the destination node PORT number**
            String destPort = getDestinationNode(obj_id);
            //String destPort = myPort(); //Testing

            Log.v("P1", "Insert this to : " + destPort);


            //Send the values to the destination node


            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "I-request",destPort,key,value);

        } catch (Exception e) {
            Log.v("P1", "Exception in insert");
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            Log.v("P1", "Do initial stuff here");

            String myEmulatorID = myId();
            myPort = myPort();
            Log.v("P1", "My id:" + myEmulatorID);

            PORTS.add(myPort);

            genChord();
            //String[] neighbours = getNeighbours(myEmulatorID);

            //predecessor = neighbours[0];
            //successor = neighbours[1];

            /*Log.v("P1", "Chord values");
            for(String key: chord){
                Log.v("P1","AVD:" + address.get(key));
                Log.v("P1", key);
            }*/

            //Delete all messages on my machine
            deleteAll();

            //CREATE SERVER SOCKET
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e("P1", "Can't create a ServerSocket");
            }

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "join-request", myPort);

        } catch (Exception e) {
            Log.e("P1", "Some exception");
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            String key = selection;
            String value = "";

            Log.v("P1", "Received query request");
            Log.v("P1", "Key: " + key);

            if (key.equalsIgnoreCase("@")) {
                String allMsg = queryAll();
                if(allMsg.isEmpty()) return cursor;
                String[] msgList = allMsg.split(sep_msg);
                for (String msg : msgList) {
                    String[] keyValPair = msg.split(sep_key_val);
                    String k = keyValPair[0];
                    String v = keyValPair[1];

                    MatrixCursor.RowBuilder builder = cursor.newRow();
                    builder.add("value", v);
                    builder.add("key", k);
                }

            } else if (key.equalsIgnoreCase("*")) {
                Socket socket;
                for (String dest : PORTS) {

                    status_queryAll = 0;

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Q-All-request", dest);


                    while(status_queryAll == 0){

                    }
                    String allMsg = status_queryAll_msg;
                    if(allMsg.isEmpty()) continue;

                    Log.v("P1", "All Query: " + allMsg);

                    String[] msgList = allMsg.split(sep_msg);
                    for (String msg : msgList) {

                        String[] keyValPair = msg.split(sep_key_val);
                        String k = keyValPair[0];
                        String v = keyValPair[1];

                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("value", v);
                        builder.add("key", k);
                    }
                }

            } else {
                //Find the node where the values is stored
                String obj_id = genHash(key);

                //**Pass the obj_id around to get the destination node PORT number**
                String destPort = getDestinationNode(obj_id);
                //String destPort = myPort(); //Testing

                Log.v("P1", "Query this to : " + destPort);


                status_query = 0;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Q-request", destPort,key);


                while(status_query == 0){

                }
                value = status_query_msg;


                MatrixCursor.RowBuilder builder = cursor.newRow();
                builder.add("value", value);
                builder.add("key", selection);
            }

        } catch (Exception e) {
            Log.v("P1", "Exception in query");
            e.printStackTrace();
        }
        return cursor;
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

    private String getDestinationNode(String objId) {
        String destNode = "";

        if (objId.compareTo(chord.get(0)) < 0 || objId.compareTo(chord.get(chord.size() - 1)) > 0)
            return address.get(chord.get(0));

        for (int k = 0; k < chord.size() - 1; k++) {
            if (objId.compareTo(chord.get(k)) > 0 && objId.compareTo(chord.get(k + 1)) < 0)
                return address.get(chord.get(k + 1));
        }


        return destNode;
    }


    private void genChord() {
        try {

            chord.clear();

            for (String remotePort : PORTS) {
                String emuId = Integer.toString(Integer.valueOf(remotePort) / 2);
                chord.add(genHash(emuId));
                address.put(genHash(emuId), remotePort);
            }
            Collections.sort(chord);
        } catch (Exception e) {

        }
    }

    private String[] getNeighbours(String myId) {
        String[] neighbours = new String[2];
        int nodeCount = chord.size();
        try {

            String myObjId = genHash(myId);
            int succ = 0, pred = 0;

            for (int k = 0; k < nodeCount; k++) {
                if (chord.get(k).equalsIgnoreCase(myObjId)) {
                    succ = k + 1;
                    pred = k - 1;
                    if (succ > nodeCount - 1) succ = 0;
                    if (pred < 0) pred = nodeCount - 1;
                    break;
                }
            }

            neighbours[0] = chord.get(pred);
            neighbours[1] = chord.get(succ);
        } catch (Exception e) {

        }
        return neighbours;
    }




    public String lookup(String id) {
        return "";
    }

    public String myId() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return portStr;
    }

    public String myPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    public void insertIntoLocal(String key, String value) {
        String string = value;
        FileOutputStream outputStream;

        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(string.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("P2", "File write failed");
            e.printStackTrace();
        }
    }

    public String getFromLocal(String key) {
        String msg = "";
        try {
            FileInputStream fis = getContext().openFileInput(key);
            StringBuilder builder = new StringBuilder();
            int ch;
            while ((ch = fis.read()) != -1) {
                builder.append((char) ch);
            }
            msg = builder.toString();
            fis.close();
        } catch (Exception e) {
            Log.e("P1", "File read failed");
            e.printStackTrace();
        }

        return msg;
    }

    public boolean deleteFromLocal(String key) {
        boolean success = false;
        try {
            success = getContext().deleteFile(key);

        } catch (Exception e) {
            Log.e("P1", "File delete failed");
            e.printStackTrace();
        }
        return success;
    }

    public void deleteAll() {
        File dir = getContext().getFilesDir();
        if (dir.isDirectory()) {
            String[] children = dir.list();
            Log.v("P1", "(Delete) File count: " + children.length);
            for (int i = 0; i < children.length; i++) {
                File f = new File(dir, children[i]);
                f.delete();
            }
        }
    }

    public String queryAll() {
        File dir = getContext().getFilesDir();
        String res = "";
        if (dir.isDirectory()) {
            String[] children = dir.list();
            Log.v("P1", "(Query) File count: " + children.length);
            for (int i = 0; i < children.length; i++) {
                File f = new File(dir, children[i]);
                String key = f.getName();
                String val = getFromLocal(key);
                String msg = key + sep_key_val + val;
                if (!res.isEmpty()) res = res + sep_msg + msg;
                else res = msg;
            }
        }
        return res;
    }


    public void addNewNode(String node) {
        try {
            if (!PORTS.contains(node)) {
                PORTS.add(node);
                genChord();
            }
            String nodeList = "";
            for (String n : PORTS) {
                if (nodeList.isEmpty()) nodeList = n;
                else nodeList = nodeList + "," + n;
            }

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "port-list", nodeList);

        } catch (Exception e) {

        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    InputStream is = socket.getInputStream();
                    DataInputStream dis = new DataInputStream(is);
                    String type = dis.readUTF();

                    OutputStream os = socket.getOutputStream();
                    os.flush();
                    DataOutputStream dos = new DataOutputStream(os);


                    if (type.equalsIgnoreCase("I-request")) {
                        dos.writeUTF("Ack of type: " + type);
                        String key = dis.readUTF();
                        String value = dis.readUTF();

                        Log.v("P2", "Received Request to insert  :" + key + ":" + value);

                        insertIntoLocal(key, value);
                    } else if (type.equalsIgnoreCase("Q-request")) {
                        String key = dis.readUTF();
                        Log.v("P2", "Received Request to query  :" + key);
                        String msg = getFromLocal(key);
                        dos.writeUTF(msg);
                    } else if (type.equalsIgnoreCase("Q-All-request")) {
                        Log.v("P2", "Received Request to query  all");
                        String allMsg = queryAll();
                        dos.writeUTF(allMsg);
                    } else if (type.equalsIgnoreCase("D-request")) {
                        String key = dis.readUTF();
                        Log.v("P2", "Received Request to delete  :" + key);
                        boolean msg = deleteFromLocal(key);
                        if (msg) dos.writeUTF("Success");
                        else dos.writeUTF("Fail");
                    } else if (type.equalsIgnoreCase("D-All-request")) {
                        Log.v("P2", "Received Request to delete all");
                        deleteAll();
                        dos.writeUTF("Success");
                    } else if (type.equalsIgnoreCase("D-All-request")) {
                        Log.v("P2", "Received Request to delete all");
                        deleteAll();
                        dos.writeUTF("Success");
                    } else if (type.equalsIgnoreCase("join-request")) {
                        String node = dis.readUTF();
                        dos.writeUTF("ack");
                        Log.v("P2", "Received join request from: " + node);
                        addNewNode(node);
                    } else if (type.equalsIgnoreCase("port-list")) {
                        String nodeList = dis.readUTF();
                        Log.v("P2", "Received new port list");
                        dos.writeUTF("ack");
                        String[] nodes = nodeList.split(",");
                        PORTS.clear();
                        for (String p : nodes) {
                            PORTS.add(p);
                        }
                        genChord();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {


        private String sendMessage(Socket socket, String remotePort, String... msgs) {
            int timeout = 10000;
            String ack = "";
            try {
                socket.setSoTimeout(timeout);

                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(os);

                for (int i = 0; i < msgs.length; i++)
                    dos.writeUTF(msgs[i]);

                InputStream is = socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);
                ack = dis.readUTF();

            } catch (Exception e) {
                //e.printStackTrace();
                Log.v("P2", "This port is dead  :" + remotePort);
            }
            return ack;
        }


        protected Void doInBackground(String... msgs) {
            try {
                Socket socket;

                if (msgs[0].toString().equalsIgnoreCase("join-request")) {

                    String myPort = msgs[1].toString();

                    Log.v("P1", "Sending join request to 5554");

                    //Send message to 5554
                    String msg = "";
                    int attempt = 0;
                    while (!msg.equalsIgnoreCase("ack") && attempt < 2) {
                        attempt++;
                        try {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(REMOTE_PORT0));
                            msg = sendMessage(socket, REMOTE_PORT0, "join-request", myPort);
                        } catch (Exception e) {
                            //e.printStackTrace();
                            //break;
                        }
                    }
                    Log.v("P1", "Sent request to 5554 successfully");
                }
                else if (msgs[0].toString().equalsIgnoreCase("port-list")) {
                    String nodeList = msgs[1].toString();

                    for (String dest : PORTS) {
                        if (dest != myPort) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(dest));
                            sendMessage(socket, dest, "port-list", nodeList);
                        }
                    }

                }
                else if (msgs[0].toString().equalsIgnoreCase("D-All-request")) {
                    for (String dest : PORTS) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(dest));
                        String msg = sendMessage(socket, dest, "D-All-request");
                        if (msg != "Success"){
                            Log.e("P1", "Some error in delete all");
                        }
                    }
                    status_deleteAll = 1;
                }
                else if (msgs[0].toString().equalsIgnoreCase("D-request")) {
                    String destPort = msgs[1].toString();
                    String key = msgs[2].toString();
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destPort));
                    String msg = sendMessage(socket, destPort, "D-request", key);
                    if (msg != "Success"){
                        Log.e("P1", "Some error in delete");
                    }
                    status_delete = 1;
                }
                else if (msgs[0].toString().equalsIgnoreCase("I-request")) {
                    String destPort = msgs[1].toString();
                    String key = msgs[2].toString();
                    String value = msgs[3].toString();
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destPort));
                    sendMessage(socket, destPort, "I-request", key, value);
                }
                else if (msgs[0].toString().equalsIgnoreCase("Q-All-request")) {
                    String dest = msgs[1].toString();

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(dest));
                    String allMsg = sendMessage(socket, dest, "Q-All-request");
                    status_queryAll_msg = allMsg;
                    status_queryAll = 1;
                }
                else if (msgs[0].toString().equalsIgnoreCase("Q-request")) {
                    String destPort = msgs[1].toString();
                    String key = msgs[2].toString();

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destPort));
                    status_query_msg = sendMessage(socket, destPort, "Q-request", key);
                    status_query = 1;

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}


