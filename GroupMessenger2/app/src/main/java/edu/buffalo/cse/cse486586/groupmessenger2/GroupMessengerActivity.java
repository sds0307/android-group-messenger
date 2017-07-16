package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.EditText;
import android.widget.TextView;
import android.view.View;
import android.content.ContentValues;
import android.net.Uri;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * 
 * @author stevko
 *
 * References:
 * Algorithm refered from http://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages
 *
 */
public class GroupMessengerActivity extends Activity {

    public static class utility {
        public static int getIndexFromPort(String portNumber) {
            try {
                if (portNumber.equalsIgnoreCase("11108")) return 0;
                if (portNumber.equalsIgnoreCase("11112")) return 1;
                if (portNumber.equalsIgnoreCase("11116")) return 2;
                if (portNumber.equalsIgnoreCase("11120")) return 3;
                if (portNumber.equalsIgnoreCase("11124")) return 4;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return -1;
        }

        public static String getPortFromIndex(int index) {
            try {
                if (index == 0) return "11108";
                if (index == 1) return "11112";
                if (index == 2) return "11116";
                if (index == 3) return "11120";
                if (index == 4) return "11124";
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "";
        }

        public static tObject getObjectedFromPQ(String mId, String fromPort) {
            for (tObject obj : priorityQueue) {
                if (obj.msgId.equalsIgnoreCase(mId) && obj.fromPort.equalsIgnoreCase(fromPort))
                    return obj;
            }
            return null;
        }


    }


    public static class tObjComparator implements Comparator<tObject> {
        @Override
        public int compare(tObject x, tObject y) {
            try {
                if (x.seqId < y.seqId) {
                    return -1;
                } else if (x.seqId > y.seqId) {
                    return 1;
                } else {
                    if (x.deliveryStatus < y.deliveryStatus) {
                        return -1;
                    } else if (x.deliveryStatus > y.deliveryStatus) {
                        return 1;
                    } else {
                        if (x.suggestedPort < y.suggestedPort) {
                            return -1;
                        } else if (x.suggestedPort > y.suggestedPort) {
                            return 1;
                        }
                        return 0;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }
    }

    public static class tObject {
        String msg;
        String msgId;
        String fromPort;
        int seqId;
        int suggestedPort;
        int deliveryStatus;

        public tObject(String message, String messageID, String fromPort, int sequenceId, String suggestedPort, int deliveryStatus) {
            this.msg = message;
            this.msgId = messageID;
            this.fromPort = fromPort;
            this.seqId = sequenceId;
            this.suggestedPort = utility.getIndexFromPort(suggestedPort);
            this.deliveryStatus = deliveryStatus;
        }
    }

    //Static Variables
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static int keyVal = 0;
    static final String[] PORTS = new String[]{REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    //static final String[] PORTS = new String[]{REMOTE_PORT0};
    static final int activeAVD = 5;
    static final int SERVER_PORT = 10000;
    final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

    //FIFO Variables
    static HashMap<String, Integer> fifo = new HashMap<String, Integer>();
    static HashMap<String, HashMap<Integer, String>> buffer = new HashMap<String, HashMap<Integer, String>>();
    static int msgId = 0;

    //Total Variables
    static String myPortGlobal;
    static int seqenceId = 0;
    static Comparator<tObject> comparator = new tObjComparator();
    static PriorityQueue<tObject> priorityQueue = new PriorityQueue<tObject>(100, comparator);
    static HashMap<Integer, String[]> suggestions = new HashMap<Integer, String[]>();
    static int foundDead = 0;
    static int heartbeatEnabled = 0;

    //Failure handling
    static String[] liveness = {"1", "1", "1", "1", "1"};

    protected void initializeFifo() {
        try {
            //Intilialize fifo
            fifo.put(REMOTE_PORT0, 0);
            fifo.put(REMOTE_PORT1, 0);
            fifo.put(REMOTE_PORT2, 0);
            fifo.put(REMOTE_PORT3, 0);
            fifo.put(REMOTE_PORT4, 0);

            //Initialize buffer
            buffer.put(REMOTE_PORT0, new HashMap<Integer, String>());
            buffer.put(REMOTE_PORT1, new HashMap<Integer, String>());
            buffer.put(REMOTE_PORT2, new HashMap<Integer, String>());
            buffer.put(REMOTE_PORT3, new HashMap<Integer, String>());
            buffer.put(REMOTE_PORT4, new HashMap<Integer, String>());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        initializeFifo();

        //PORT CONNECTION
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortGlobal = myPort;

        //Start checking for port aliveness
        //new FailureDetectionTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "");

        //CREATE SERVER SOCKET
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        final EditText editText = (EditText) findViewById(R.id.editText1);

        //OnClick Listener
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "message", msg, myPort);
            }
        });



    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     * <p>
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        protected void sendViaFifo(String sourcePort, String counter, String msg) {
            try {

                String portIndex = Integer.toString(utility.getIndexFromPort(sourcePort));

                if ((fifo.get(sourcePort) + 1) == Integer.parseInt(counter)) {
                    fifo.put(sourcePort, Integer.parseInt(counter));
                    //Log.v(TAG, "Sending message from fifo to screen for : " + msg + " with id: " + counter);
                    publishProgress("deliver", msg, "From " + portIndex + " - " + counter + ". " + msg);

                    while (buffer.get(sourcePort).containsKey(fifo.get(sourcePort) + 1)) {
                        counter = Integer.toString(fifo.get(sourcePort) + 1);
                        msg = buffer.get(sourcePort).get(fifo.get(sourcePort) + 1);
                        buffer.get(sourcePort).remove(fifo.get(sourcePort) + 1);
                        fifo.put(sourcePort, Integer.parseInt(counter));
                        //Log.v(TAG, "Sending message from fifo buffer to screen for : " + msg + " with id: " + counter);
                        publishProgress("deliver", msg, "From " + portIndex + " - " + counter + ". " + msg);
                    }
                } else if ((fifo.get(sourcePort) + 1) < Integer.parseInt(counter)) {
                    //Log.v(TAG, "Using fifo buffer for : " + msg + " with id: " + counter);
                    buffer.get(sourcePort).put(Integer.parseInt(counter), msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected void deliverTotal() {
            while ((!priorityQueue.isEmpty()) && priorityQueue.peek().deliveryStatus == 1) {
                tObject obj = priorityQueue.remove();
                printPQ();
                String msg = obj.msg;
                String sourcePort = Integer.toString(utility.getIndexFromPort(obj.fromPort));
                String counter = Integer.toString(obj.seqId);

                //Log.v(TAG, "Delivering message from total for Mid: " + obj.msgId);

                //publishProgress("deliver",msg,"From "+sourcePort+ " - " + counter + ". " + msg);
                sendViaFifo(obj.fromPort, obj.msgId, msg);
            }
        }

        protected void printPQ() {
            tObject tObj = priorityQueue.peek();
            if(tObj == null) return;
            Log.v(TAG, "PQ front: ");
            Log.v(TAG, "SeqId: " + Integer.toString(tObj.seqId));
            Log.v(TAG, tObj.msg);
            Log.v(TAG, "msgId: " + tObj.msgId);
            Log.v(TAG, "from: " + tObj.fromPort);
            Log.v(TAG, "delivery: " + tObj.deliveryStatus);
        }

        protected void checkAllProposals(String mId){
            printPQ();
            //Log.v(TAG, "All proposal received for msg " + mId + "?");
                //Log.v(TAG, "YES");
                int finalSID = -1;
                int k = 0;
                String[] props = suggestions.get(Integer.parseInt(mId));
                for (int i = 0; i < activeAVD; i++) { //Change to 5 **CHANGE**
                    if(props[i].equalsIgnoreCase("")) props[i] = "-1";
                    if (Integer.parseInt(props[i]) > finalSID) {
                        finalSID = Integer.parseInt(props[i]);
                        k = i;
                    }
                }
                //Log.v(TAG, "1. Send final for msg " + mId + " from port :" + myPortGlobal + "as num: " + finalSID);
                publishProgress("toClient", "send final", mId, myPortGlobal, Integer.toString(finalSID), Integer.toString(k)); //Send final

        }

        protected void suggestionupdate(String mId, String port, String sId){
            int index = utility.getIndexFromPort(port);


            if (!suggestions.containsKey(Integer.parseInt(mId))) {
                String[] empty = {"", "", "", "", ""};
                for(int i=0; i< activeAVD; i++){
                    if(liveness[i] == "0")
                        empty[i] = "-1";
                }
                suggestions.put(Integer.parseInt(mId), empty);
            }

            String[] props = suggestions.get(Integer.parseInt(mId));

            props[index] = sId;

            for (int i = 0; i < activeAVD; i++) { //Change to 5 **CHANGE**
                if (props[i].isEmpty()) {
                    Log.v(TAG, "Pending for msg: " + mId + " from port: " + port + " at index : " + i);
                    return;
                }
            }
            checkAllProposals(mId);
        }

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
                    dos.writeUTF("Ack");


                    if (type.equalsIgnoreCase("T-msg")) {

                        String msg = dis.readUTF();
                        String sourceMsgID = dis.readUTF();
                        String sourcePort = dis.readUTF();

                        Log.v(TAG, "Received message " + msg + " from port :" + sourcePort);

                        seqenceId = seqenceId + 1; //Increment seq number
                        //Log.v(TAG, "1. Sending proposal for " + msg + " with seq num :" + seqenceId);
                        publishProgress("toClient", "send proposal", sourcePort, sourceMsgID, Integer.toString(seqenceId)); //Send proposal
                        //Log.v(TAG, "Adding to PQ for msg: " + msg);
                        priorityQueue.add(new tObject(msg, sourceMsgID, sourcePort, seqenceId, myPortGlobal, 0)); //Add to PQ

                        printPQ();
                        deliverTotal();

                    } else if (type.equalsIgnoreCase("T-proposal")) {
                        String mId = dis.readUTF();
                        String sId = dis.readUTF();
                        String fromPort = dis.readUTF();
                        int index = utility.getIndexFromPort(fromPort);


                        Log.v(TAG, "Received proposal for msg " + mId + " from port :" + fromPort + " as num: " + sId);

                        suggestionupdate(mId,fromPort,sId);

                        //checkAllProposals(mId);

                    } else if (type.equalsIgnoreCase("T-final")) {
                        String mId = dis.readUTF();
                        String fromPort = dis.readUTF();
                        String finalSid = dis.readUTF();
                        String k = dis.readUTF();

                        Log.v(TAG, "Received FINAL for: " + mId + " from " + utility.getIndexFromPort(fromPort) + " as Sid: " + finalSid);

                        seqenceId = Math.max(seqenceId, Integer.parseInt(finalSid)); //Update the sequence id

                        //Get the message from PQ
                        tObject ele = utility.getObjectedFromPQ(mId, fromPort);
                        if(ele != null) {
                            priorityQueue.remove(ele);
                            ele.seqId = Integer.parseInt(finalSid);
                            ele.suggestedPort = Integer.parseInt(k);
                            ele.deliveryStatus = 1;
                            priorityQueue.add(ele);
                        }

                        //Log.v(TAG, "Priority queue modified for final sequence of Mid: " + mId);
                        printPQ();
                        deliverTotal();

                    }else if (type.equalsIgnoreCase("dead")) {
                        String deadPort = dis.readUTF();
                        foundDead = 1;

                        Log.v(TAG, "updating suggestion for dead");
                        Log.v(TAG, "messages updated");

                        for (Map.Entry<Integer, String[]> e: suggestions.entrySet()) {
                            int i = e.getKey();
                            Log.v(TAG, Integer.toString(i));
                            suggestionupdate(Integer.toString(i),deadPort,"-1");
                        }
                    }else if (type.equalsIgnoreCase("T-test")) {
                        //System.out.println("Successfull test");
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            try {
                if(heartbeatEnabled == 0){
                    heartbeatEnabled = 1;
                    //Heartbeat
                    new Heartbeat().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "message");
                }
                if (strings[0].trim().equalsIgnoreCase("deliver")) {
                    String strReceived = strings[1].trim();
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append(strings[2].trim() + "\t\n");

                    Log.v(TAG, "Displayed msg for : " + strReceived);

                    //Log.v(TAG, "Adding to content provider for : " + strReceived);

                    //Insert the message
                    ContentValues keyValueToInsert = new ContentValues();

                    keyValueToInsert.put("key", Integer.toString(keyVal));
                    keyValueToInsert.put("value", strReceived);
                    keyVal = keyVal + 1;

                    Uri newUri = getContentResolver().insert(
                            providerUri,    // assume we already created a Uri object with our provider URI
                            keyValueToInsert
                    );

                    //Log.v(TAG, "Added successfully to content provider for : " + strReceived);
                } else if (strings[0].trim().equalsIgnoreCase("toClient")) {
                    String type = strings[1].trim();
                    if (type.equalsIgnoreCase("send proposal")) {
                        String toPort = strings[2].trim();
                        String msgId = strings[3].trim();
                        String seqId = strings[4].trim();
                        //Log.v(TAG, "2. Sending proposal for " + msgId + " with seq num :" + seqId);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "fromServer", "send proposal", toPort, msgId, seqId);
                    } else if (type.equalsIgnoreCase("send final")) {
                        //Log.v(TAG, "2. Send final for msg");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "fromServer", "send final", strings[2].trim(), strings[3].trim(), strings[4].trim(), strings[5].trim());
                    }else if (type.equalsIgnoreCase("do test")) {
                        //Log.v(TAG, "Testing for : " + strings[2].trim());
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "testMessage", strings[2].trim());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {


        protected void sendMessage(Socket socket, String remotePort, String... msgs){
            boolean alive = false;
            int timeout = 10000;
            try {


                socket.setSoTimeout(timeout);

                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(os);
                for(int i=0; i< msgs.length; i++)
                    dos.writeUTF(msgs[i]);

                InputStream is = socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);
                String ack = dis.readUTF();
                if (ack.equalsIgnoreCase("ACK"))
                    alive = true;

            } catch (SocketTimeoutException e) {
                Log.v(TAG, "This port is dead  :" + remotePort);
            } catch (StreamCorruptedException e) {
                Log.v(TAG, "This port is dead  :" + remotePort);
            } catch (EOFException e) {
                Log.v(TAG, "This port is dead  :" + remotePort);
            } catch (IOException e) {
                Log.v(TAG, "This port is dead  :" + remotePort);
            } catch (Exception e) {
                Log.v(TAG, "This port is dead  :" + remotePort);
            }
            if (!alive) {
                int index = utility.getIndexFromPort(remotePort);
                liveness[index] = "0";
                Log.v(TAG, "This port is dead  :" + remotePort);

                //TELL SERVER that PORT is dead
                try {

                    removeDeadPort(remotePort);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(myPortGlobal));
                    OutputStream os1 = socket.getOutputStream();
                    DataOutputStream dos1 = new DataOutputStream(os1);
                    dos1.writeUTF("dead"); //T-msg, T-proposal, T-final
                    dos1.writeUTF(remotePort);

                } catch (Exception e) {

                }
            }

        }


        protected  void removeDeadPort(String port){
            for(tObject tobj: priorityQueue){
                if(tobj.fromPort.equalsIgnoreCase(port)) {
                    priorityQueue.remove(tobj);
                }
            }
        }

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket;

                boolean alive = false;

                if (msgs[0].toString().equalsIgnoreCase("message")) {

                    String msgToSend = msgs[1];
                    Log.v(TAG, "Received message from screen :" + msgToSend);

                    msgId = msgId + 1;

                    for (String remotePort : PORTS) {
                        alive = false;
                        if (liveness[utility.getIndexFromPort(remotePort)] == "0") continue;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        sendMessage(socket,remotePort,"T-msg",msgToSend,Integer.toString(msgId),msgs[2]);
                    }
                    //socket.close();
                } else if (msgs[0].toString().equalsIgnoreCase("fromServer")) {
                    String type = msgs[1];
                    if (type.equalsIgnoreCase("send proposal")) {
                        String toPort = msgs[2].trim();
                        String mId = msgs[3].trim();
                        String sId = msgs[4].trim();

                        if (liveness[utility.getIndexFromPort(toPort)] == "0") return null;

                        Log.v(TAG, "3. Sending proposal for " + mId + " with seq num :" + sId);

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(toPort));

                        sendMessage(socket,toPort,"T-proposal",mId,sId,myPortGlobal);

                    } else if (type.equalsIgnoreCase("send final")) {
                        String mId = msgs[2].trim();
                        String fromPort = msgs[3].trim();
                        String finalSid = msgs[4].trim();
                        String k = msgs[5].trim();

                        for (String remotePort : PORTS) {

                            if (liveness[utility.getIndexFromPort(remotePort)] == "0") continue;
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort));
                            Log.v(TAG, "3. Sending final for " + mId + " with seq num :" + finalSid + " to port: " + remotePort);

                            sendMessage(socket,remotePort,"T-final",mId,fromPort,finalSid,k);
                        }
                    }
                }else if (msgs[0].toString().equalsIgnoreCase("test-message")) {
                    String port = msgs[1];
                    if (liveness[utility.getIndexFromPort(port)] == "0") return null;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    sendMessage(socket,port,"T-test");
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    private class Heartbeat extends AsyncTask<String, String, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
        try {
            while (foundDead == 0) {
                for (String remotePort : PORTS) {
                    TimeUnit.SECONDS.sleep(1);
                    if((foundDead == 0))
                        publishProgress(remotePort);
                }
            }
        }catch(Exception e){

        }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            try{
                String port = strings[0];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "test-message", port);

            }catch(Exception e){

            }
        }


    }
}
