package edu.buffalo.cse.cse486586.groupmessenger1;

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
import android.database.Cursor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.File;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    //Static Variables
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static int keyVal = 0;
    static final String[] PORTS = new String[]{REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        //SET VIEW
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //PORT CONNECTION
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

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

        //ONCLICK LISTENER
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
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
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
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
                    String msg = dis.readUTF();
                    publishProgress(msg);
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            return null;
        }
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            //Insert the message
            ContentValues keyValueToInsert = new ContentValues();

            //for(String s: fileList())
                //deleteFile(s);

            int size = 0;
            for(String s: fileList())
                if(s.startsWith("gm")) size++;


            /*if(size > 0){
                File file = new File(ih.getImgPath(id));
                boolean deleted = file.delete();
            }*/
            //Log.v(TAG, "FileName: " + fileList().toString());
            //Log.v(TAG, "Total number of files in are: " + size);

            // inserting <”key-to-insert”, “value-to-insert”>
            keyValueToInsert.put("key", Integer.toString(keyVal));
            keyValueToInsert.put("value", strReceived);
            keyVal = keyVal + 1;

            Uri newUri = getContentResolver().insert(
                        providerUri,    // assume we already created a Uri object with our provider URI
                        keyValueToInsert
                );

            /*try {
                Cursor cursor = getContentResolver().query(newUri,null,"1",null,null);
                String data = "";
                if (cursor.moveToFirst()) {
                    do {
                        data = cursor.getString(cursor.getColumnIndex("value"));
                        // do what ever you want here
                    } while (cursor.moveToNext());
                }
                cursor.close();
                Log.v(TAG, "I received cursor shit and it is: " + data);
            }
            catch(Exception e){
                e.printStackTrace();
            }

            Log.v(TAG, "I received message and it is: " + strReceived);*/

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                /*String remotePort = REMOTE_PORT0;
                if (msgs[1].equals(REMOTE_PORT0))
                    remotePort = REMOTE_PORT1;*/

                String msgToSend = msgs[0];
                Socket socket;

                for(String remotePort: PORTS){
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    //Write to output stream
                    OutputStream os = socket.getOutputStream();
                    DataOutputStream dos = new DataOutputStream(os);
                    dos.writeUTF(msgToSend);
                }

                //socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }
}
