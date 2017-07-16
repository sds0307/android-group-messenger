package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

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

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String[] PORTS = new String[]{REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
	static String successor1 = "";
	static String successor2 = "";
	static String predecessor1 = "";
	static String predecessor2 = "";
	static List<String> chord = new ArrayList<String>();
	static HashMap<String, String> address = new HashMap<String, String>();
	static final int SERVER_PORT = 10000;
	static final String sep_key_val = "@##@";
	static final String sep_msg = "@#@";
	static final String sep_time = "@###@";
	static String myPort;
	static String myEmulatorID;
	static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	//static int req_id = 0;
	AtomicInteger req_id = new AtomicInteger();
	static HashMap<Integer,Integer> status_req = new HashMap<Integer,Integer>();
	static HashMap<Integer,String> status_req_msg = new HashMap<Integer,String>();
	static HashMap<String, Integer> life = new HashMap<String, Integer>();
	static int syncDone = 0;
	static int onCreateDone = 0;
	static int syncProcess = 0;
	static String testFileName = "test_for_freshness";

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		try {

			//while(syncProcess == 1){
			//}

			String key = selection;
			int rid = req_id.getAndIncrement();
			status_req.put(rid,0);



			Log.v("delete", "Received delete request");
			Log.v("delete", "Key: " + key);

			if (key.equalsIgnoreCase("@")) {
				deleteAll();
				return 1;
			} else if (key.equalsIgnoreCase("*")) {
				status_req.put(rid,0);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-All-request",Integer.toString(rid));
				while(status_req.get(rid) == 0){

				}
				return 1;
			} else {

				//Find the node where the values is stored
				String obj_id = genHash(key);

				//**Pass the obj_id around to get the destination node PORT number**
				String destPort = getDestinationNode(obj_id);
				//String destPort = myPort(); //Testing

				Log.v("delete", "Delete this from : " + destPort);

				//Send the values to the destination node
				status_req.put(rid,0);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-request",destPort,key,Integer.toString(rid));

				while(status_req.get(rid) == 0){

				}

				String[] neighbours = getNeighbours(Integer.toString(Integer.parseInt(destPort)/2));
				String dest1 = neighbours[0];
				String dest2 = neighbours[1];

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-request",dest1,key,Integer.toString(rid));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D-request",dest2,key,Integer.toString(rid));

				return 1;
			}

		} catch (Exception e) {
			Log.v("delete", "Exception in delete");
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

			//while(syncProcess == 1){
			//}

			String key = (String) values.get("key");
			String value = (String) values.get("value");
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			value = value + sep_time + sdf.format(timestamp);
			//Timestamp t = new Timestamp(sdf.parse(key).getTime());


			Log.v("insert", "Received insert request");
			Log.v("insert", "Key: " + key);
			Log.v("insert", "Value: " + value);

			//Find the node where the values needs to be stored
			String obj_id = genHash(key);

			//**Pass the obj_id around to get the destination node PORT number**
			String destPort = getDestinationNode(obj_id);
			//String destPort = myPort(); //Testing

			Log.v("insert", "Insert this to : " + destPort);


			//Send the values to the destination node

			String[] neighbours = getNeighbours(Integer.toString(Integer.parseInt(destPort)/2));
			String dest1 = neighbours[0];
			String dest2 = neighbours[1];

			if(destPort.equals(myPort) || dest1.equals(myPort) || dest2.equals(myPort))
				insertIntoLocal(key,value);

			Log.v("insert", "Insert this to : " + destPort);
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "I-request",destPort,key,value,destPort);

			Log.v("insert", "Insert this to : " + dest1);
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "I-request",dest1,key,value,destPort);

			Log.v("insert", "Insert this to : " + dest2);
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "I-request",dest2,key,value,destPort);

		} catch (Exception e) {
			Log.v("insert", "Exception in insert");
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try {

			if(onCreateDone == 1) return false;

			Log.v("onCreate", "Do initial stuff here");

			myEmulatorID = myId();
			myPort = myPort();
			Log.v("onCreate", "My emulator id:" + myEmulatorID);
			Log.v("onCreate", "My port:" + myPort);

			genChord();
			String[] neighbours = getNeighbours(myEmulatorID);

			successor1 = neighbours[0];
			successor2 = neighbours[1];
			predecessor1 = neighbours[2];
			predecessor2 = neighbours[3];

			Log.v("onCreate", "My successor1:" + successor1);
			Log.v("onCreate", "My successor2:" + successor2);
			Log.v("onCreate", "My predecessor1:" + predecessor1);
			Log.v("onCreate", "My predecessor2:" + predecessor2);


			for(String port: PORTS){
				life.put(port,1);
			}

            /*Log.v("P1", "Chord values");
            for(String key: chord){
                Log.v("P1","AVD:" + address.get(key));
                Log.v("P1", key);
            }*/

			//Delete all messages on my machine
			//deleteAll();

			//if(getContext().fileList().length > 0) {

			//CREATE SERVER SOCKET
			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e("onCreate", "Can't create a ServerSocket");
			}

			if(!isFreshStart()) {

				Log.v("onCreate", "Synchronization started");

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "sync-alert-request", myPort);

				/*syncDone = 0;
				while (syncDone == 0) {

				}
				//Log.v("onCreate", "Synchronization done");
				*/
			}
			//}

			onCreateDone = 1;

		} catch (Exception e) {
			Log.e("onCreate", "Some exception");
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		try {
			//while(syncProcess == 1){
			//}
			String key = selection;
			String value = "";
			int qid = req_id.getAndIncrement();

			status_req.put(qid,0);
			status_req_msg.put(qid,"");

			Log.v("query", "Received query request");
			Log.v("query", "Key: " + key);

			if (key.equalsIgnoreCase("@")) {
				String allMsg = queryAll();
				if(allMsg.isEmpty()) return cursor;
				String[] msgList = allMsg.split(sep_msg);
				for (String msg : msgList) {
					String[] keyValPair = msg.split(sep_key_val);
					if(keyValPair.length == 2) {
						String k = keyValPair[0];
						String m[] = keyValPair[1].split(sep_time);
						String v = m[0];

						MatrixCursor.RowBuilder builder = cursor.newRow();
						builder.add("value", v);
						builder.add("key", k);
					}
					else
					{
						Log.v("query", "Unable to split: " + msg);
					}
				}

			} else if (key.equalsIgnoreCase("*")) {
				for (String dest : PORTS) {

					status_req.put(qid,0);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Q-All-request", dest,Integer.toString(qid));


					while(status_req.get(qid) == 0){

					}
					String allMsg = status_req_msg.get(qid);
					if(allMsg.isEmpty()) continue;

					//Log.v("query", "All Query: " + allMsg);

					String[] msgList = allMsg.split(sep_msg);
					for (String msg : msgList) {

						String[] keyValPair = msg.split(sep_key_val);
						if(keyValPair.length == 2) {
							String k = keyValPair[0];
							String m[] = keyValPair[1].split(sep_time);
							String v = m[0];

							MatrixCursor.RowBuilder builder = cursor.newRow();
							builder.add("value", v);
							builder.add("key", k);
						}
						else{
							Log.v("query", "Unable to split: " + msg);
						}
					}
				}

			} else {

				if (fileExists(getContext().fileList(), key)) {
					String m[] = getFromLocal(key).split(sep_time);
					value = m[0];
					Log.v("query", "final Query result: " + value);
					MatrixCursor.RowBuilder builder = cursor.newRow();
					builder.add("value", value);
					builder.add("key", selection);
					return cursor;
				}

				//Find the node where the values is stored
				String obj_id = genHash(key);

				//**Pass the obj_id around to get the destination node PORT number**
				String destPort = getDestinationNode(obj_id);
				//String destPort = myPort(); //Testing

				Log.v("query", "Query this to : " + destPort);

				List<String> messages = new ArrayList<String>();
				String[] neighbours = getNeighbours(Integer.toString(Integer.parseInt(destPort)/2));
				String dest1 = neighbours[0];
				String dest2 = neighbours[1];

				//Log.v("query", "Query id: " + qid);
				int qid1 = qid + 1001;
				status_req.put(qid1,0);
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "Q-request", destPort,key,Integer.toString(qid1));

				int qid2 = qid + 1002;
				status_req.put(qid2,0);
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "Q-request", dest1,key,Integer.toString(qid2));


				int qid3 = qid + 1003;
				status_req.put(qid3,0);
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "Q-request", dest2,key,Integer.toString(qid3));

				while(status_req.get(qid1) == 0 || status_req.get(qid2) == 0 || status_req.get(qid3) == 0){
				}
				Log.v("query", "Query proccessing done for: " + key);
				if(!status_req_msg.get(qid1).isEmpty())
					messages.add(status_req_msg.get(qid1));

				//Log.v("query", "Query id: " + qid);

				if(!status_req_msg.get(qid2).isEmpty())
					messages.add(status_req_msg.get(qid2));

				//Log.v("query", "Query id: " + qid);

				if(!status_req_msg.get(qid3).isEmpty())
					messages.add(status_req_msg.get(qid3));

				String m[] = messages.get(0).split(sep_time);
				if(m.length == 2) {
					value = m[0];
					Timestamp tm1 = new Timestamp(sdf.parse(m[1]).getTime());

					Log.v("query", "Query response received");
					for (String msgs : messages) {
						String n[] = msgs.split(sep_time);
						if(n.length == 2) {
							Log.v("query", msgs);
							Timestamp tm2 = new Timestamp(sdf.parse(n[1]).getTime());
							if (n[1].compareTo(m[1]) > 0) {
								tm1 = tm2;
								value = n[0];
							}
						}
						else{
							Log.v("query", "Unable to split: " + msgs);
						}
					}
					//value = m[0];
					Log.v("query", "final Query result: " + value);
					MatrixCursor.RowBuilder builder = cursor.newRow();
					builder.add("value", value);
					builder.add("key", selection);
				}
				else{
					Log.v("query", "Unable to split: " + messages.get(0));
				}
			}

		} catch (Exception e) {
			Log.e("query", "Exception in query");
			e.printStackTrace();
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
		String[] neighbours = new String[4];
		int nodeCount = chord.size();
		try {

			String myObjId = genHash(myId);
			int succ1 = 0, pred1 = 0;
			int succ2 = 0, pred2 = 0;

			for (int k = 0; k < nodeCount; k++) {
				if (chord.get(k).equalsIgnoreCase(myObjId)) {
					succ1 = k + 1;
					pred1 = k - 1;

					if (succ1 > nodeCount - 1) succ1 = 0;
					if (pred1 < 0) pred1 = nodeCount - 1;

					succ2 = succ1 + 1;
					pred2 = pred1 - 1;

					if (succ2 > nodeCount - 1) succ2 = 0;
					if (pred2 < 0) pred2 = nodeCount - 1;
					break;
				}
			}

			neighbours[0] = address.get(chord.get(succ1));
			neighbours[1] = address.get(chord.get(succ2));
			neighbours[2] = address.get(chord.get(pred1));
			neighbours[3] = address.get(chord.get(pred2));
		} catch (Exception e) {

		}
		return neighbours;
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

	public boolean fileExists(String[] fileList, String key){
		for(String fileName : fileList){
			if(fileName.equals(key))
				return true;
		}
		return false;
	}

	public boolean isFreshStart(){
		boolean res = true;
		try {
			String val = "";
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			val = sdf.format(timestamp);



			String[] all_Keys = getContext().fileList();

			for (int i = 0; i < all_Keys.length; i++) {
				if (all_Keys[i].equalsIgnoreCase(testFileName)){
					FileInputStream fis = getContext().openFileInput(testFileName);
					StringBuilder builder = new StringBuilder();
					int ch;
					while ((ch = fis.read()) != -1) {
						builder.append((char) ch);
					}
					Log.v("isFreshStart", "Started before at: " + builder.toString());
					fis.close();
					return false;
				}

			}

			FileOutputStream outputStream;
			outputStream = getContext().openFileOutput(testFileName, Context.MODE_PRIVATE);
			outputStream.write(val.getBytes());
			outputStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return res;
	}

	public synchronized void insertIntoLocal(String key, String value) {
		String new_msg = value;
		FileOutputStream outputStream;
		String old_msg = "";
		String old_time = "";
		String new_time = "";

		try {

			String m1[] = new_msg.split(sep_time);
			if(m1.length == 2) {
				new_time = m1[1];

				if (fileExists(getContext().fileList(), key)) {
					//Log.v("insertIntoLocal", "File already exists");
					try {
						FileInputStream fis = getContext().openFileInput(key);
						StringBuilder builder = new StringBuilder();
						int ch;
						while ((ch = fis.read()) != -1) {
							builder.append((char) ch);
						}
						old_msg = builder.toString();
						String[] m2 = old_msg.split(sep_time);
						old_time = m2[1];
						fis.close();
					} catch (Exception e) {
						Log.e("insertIntoLocal", "File read failed");
						e.printStackTrace();
					}

					//Timestamp old_timestamp = new Timestamp(sdf.parse(old_time).getTime());
					//Timestamp new_timestamp = new Timestamp(sdf.parse(new_time).getTime());
					//if (sdf.parse(old_time).getTime() > sdf.parse(new_time).getTime()) {

					if (old_time.compareTo(new_time) > 0) {
						Log.v("insertIntoLocal", "New msg is old");
						new_msg = old_msg;
					}

				}

				Log.v("insertIntoLocal", "Inserting... key: " + key + " val: " + new_msg);

				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(new_msg.getBytes());
				outputStream.close();
			}
			else{
				Log.v("insertIntoLocal", "Unable to split: " + new_msg);
			}
		} catch (Exception e) {
			Log.e("insertIntoLocal", "File write failed");
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
			if(!msg.contains(sep_time))
				Log.v("getFromLocal", "Time missing for key: " + key + " and val: " + msg);
			fis.close();
		} catch (Exception e) {
			Log.e("getFromLocal", "File read failed");
			e.printStackTrace();
		}

		return msg;
	}

	public boolean deleteFromLocal(String key) {
		boolean success = false;
		Log.v("deleteFromLocal", "Trying to delete from local: " + key);
		try {
			success = getContext().deleteFile(key);
			if(success)
				Log.v("deleteFromLocal", "Successfully deleted from local: " + key);
			else
				Log.v("deleteFromLocal", "File not found in local: " + key);
		} catch (Exception e) {
			Log.e("deleteFromLocal", "File delete failed");
			e.printStackTrace();
		}
		return success;
	}

	public void deleteAll() {
		File dir = getContext().getFilesDir();
		if (dir.isDirectory()) {
			String[] children = dir.list();
			Log.v("deleteAll", "(Delete) File count: " + children.length);
			for (int i = 0; i < children.length; i++) {
				File f = new File(dir, children[i]);
				if(f.getName().equalsIgnoreCase(testFileName)) continue;
				f.delete();
			}
		}
	}

	public String queryAll() {
		File dir = getContext().getFilesDir();
		String res = "";
		if (dir.isDirectory()) {
			String[] children = dir.list();
			//Log.v("queryAll", "(Query) File count: " + children.length);
			for (int i = 0; i < children.length; i++) {
				File f = new File(dir, children[i]);
				String key = f.getName();
				if(key.equalsIgnoreCase(testFileName)) continue;
				String val = getFromLocal(key);
				String msg = key + sep_key_val + val;
				if (!res.isEmpty()) res = res + sep_msg + msg;
				else res = msg;
			}
		}
		return res;
	}

	public String GetPersonalMessages(List<String> ports){
		String res = "";
		File dir = getContext().getFilesDir();
		try {

			if (dir.isDirectory()) {
				String[] children = dir.list();
				//Log.v("GetPersonalMessages", "(Query) File count: " + children.length);
				for (int i = 0; i < children.length; i++) {
					File f = new File(dir, children[i]);
					String key = f.getName();
					if(key.equalsIgnoreCase(testFileName)) continue;
					if (ports.contains(getDestinationNode(genHash(key)))) {
						String val = getFromLocal(key);
						String msg = key + sep_key_val + val;
						if (!res.isEmpty()) res = res + sep_msg + msg;
						else res = msg;
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return res;
	}

	public String GetAllMessagesFor(String fromPort){
		String res = "";
		List<String> ports = new ArrayList<String>();

		String[] neigh = getNeighbours(Integer.toString(Integer.parseInt(fromPort)/2));

		ports.add(fromPort);
		ports.add(neigh[2]);
		ports.add(neigh[3]);

		res = GetPersonalMessages(ports);

		return res;
	}

	public List<String> getListOfKeysWithMe(){
		List<String> res = new ArrayList<String>();

		String[] all_Keys = getContext().fileList();

		for(int i=0; i< all_Keys.length; i++){
			if(all_Keys[i].equalsIgnoreCase(testFileName)) continue;
			res.add(all_Keys[i]);
		}

		return res;
	}

	public void SyncAllMessages(String msgList){
		//List<String> myKeys;
		//myKeys = getListOfKeysWithMe();
		String allMsgs[] = msgList.split(sep_msg);
		String m_part[];
		String key;
		String val;
		for(String m: allMsgs){
			m_part = m.split(sep_key_val);
			if(m_part.length == 2) {
				key = m_part[0];
				val = m_part[1];
				//if (fileExists(getContext().fileList(), key))
					//myKeys.remove(key);
				insertIntoLocal(key, val);
			}
			else{
				Log.v("SyncAllMessages", "Unable to split : " + m);
			}
		}

		/*for(String k: myKeys){
			deleteFromLocal(k);
		}*/

	}


	public void SyncRequest(){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "sync-request", myPort);
	}

	public void SyncDoneRequest(){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "sync-done-request", myPort);
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
						String owner = dis.readUTF();

						Log.v("server", "Received Request to insert  :" + key + ":" + value + " which belongs to " + owner);

						insertIntoLocal(key, value);

					} else if (type.equalsIgnoreCase("Q-request")) {
						String key = dis.readUTF();
						Log.v("server", "Received Request to query  :" + key);
						String msg = getFromLocal(key);
						dos.writeUTF(msg);
					} else if (type.equalsIgnoreCase("Q-All-request")) {
						Log.v("server", "Received Request to query  all");
						String allMsg = queryAll();
						dos.writeUTF(allMsg);
					} else if (type.equalsIgnoreCase("D-request")) {
						String key = dis.readUTF();
						Log.v("server", "Received Request to delete  :" + key);
						boolean msg = deleteFromLocal(key);
						if (msg) dos.writeUTF("Success");
						else dos.writeUTF("Fail");
					} else if (type.equalsIgnoreCase("D-All-request")) {
						Log.v("server", "Received Request to delete all");
						deleteAll();
						dos.writeUTF("Success");
					} else if (type.equalsIgnoreCase("sync-request")) {
						syncProcess = 1;
						String fromPort = dis.readUTF();
						Log.v("server", "Received Request to sync from: " + fromPort);
						//TimeUnit.SECONDS.sleep(2);
						String msgList = "";
						Log.v("server", "Sending my messages...");
						msgList = GetAllMessagesFor(fromPort);
						dos.writeUTF(msgList);
					} else if (type.equalsIgnoreCase("sync-on")) {
						String fromPort = dis.readUTF();
						Log.v("server", "Sync is getting on by: " + fromPort);
						syncProcess = 1;
						dos.writeUTF("Success");
					} else if (type.equalsIgnoreCase("sync-off")) {
						String fromPort = dis.readUTF();
						Log.v("server", "Sync is getting off: " + fromPort);
						syncProcess = 0;
						dos.writeUTF("Success");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}

		protected void onProgressUpdate(String... strings) {
			try {

				if (strings[0].trim().equalsIgnoreCase("toClient")) {
					String type = strings[1].trim();
					if (type.equalsIgnoreCase("I-request")) {
						String destPort = strings[2].trim();
						String key = strings[3].trim();
						String value = strings[4].trim();

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "I-request",destPort,key,value);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			return;
		}
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {


		private String sendMessage(Socket socket, String remotePort, String... msgs) {
			int timeout = 5000;
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
				life.put(remotePort,1);

			} catch (Exception e) {
				//e.printStackTrace();
				Log.v("sendMessage", "This port is dead  :" + remotePort);
				life.put(remotePort,0);
			}
			return ack;
		}


		protected Void doInBackground(String... msgs) {
			try {
				Socket socket;
				if (msgs[0].toString().equalsIgnoreCase("D-All-request")) {
					String id = msgs[1].toString();
					for (String dest : PORTS) {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(dest));
						String msg = sendMessage(socket, dest, "D-All-request");
						if (msg != "Success"){
							Log.v("Client", "Some error in delete all");
						}
					}
					status_req.put(Integer.parseInt(id),1);
				}
				else if (msgs[0].toString().equalsIgnoreCase("D-request")) {
					String destPort = msgs[1].toString();
					String key = msgs[2].toString();
					String id = msgs[3].toString();
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(destPort));
					String msg = sendMessage(socket, destPort, "D-request", key);
					status_req.put(Integer.parseInt(id),1);
				}
				else if (msgs[0].toString().equalsIgnoreCase("I-request")) {
					String destPort = msgs[1].toString();
					String key = msgs[2].toString();
					String value = msgs[3].toString();
					String owner = msgs[4].toString();
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(destPort));
					sendMessage(socket, destPort, "I-request", key, value,owner);
				}
				else if (msgs[0].toString().equalsIgnoreCase("Q-All-request")) {
					String dest = msgs[1].toString();
					String id = msgs[2].toString();

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(dest));
					String allMsg = sendMessage(socket, dest, "Q-All-request");
					status_req_msg.put(Integer.parseInt(id),allMsg);
					status_req.put(Integer.parseInt(id),1);
				}
				else if (msgs[0].toString().equalsIgnoreCase("Q-request")) {
					String destPort = msgs[1].toString();
					String key = msgs[2].toString();
					String id = msgs[3].toString();

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(destPort));
					String m = sendMessage(socket, destPort, "Q-request", key);
					status_req_msg.put(Integer.parseInt(id),m);
					status_req.put(Integer.parseInt(id),1);
				}
				else if (msgs[0].toString().equalsIgnoreCase("sync-alert-request")) {
					String myPort = msgs[1].toString();

					for (String dest : PORTS) {
						if(dest.equals(myPort)) continue;
						Log.v("client", "Sending sync alert to " + dest);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(dest));
						sendMessage(socket, dest, "sync-on",myPort);
					}

					SyncRequest();

				} else if (msgs[0].toString().equalsIgnoreCase("sync-request")) {
					String msgList = "";

					for (String dest : PORTS) {
						if(dest.equals(myPort)) continue;
						Log.v("client", "Sending sync request to " + dest);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(dest));
						String msg = sendMessage(socket, dest, "sync-request",myPort);
						if (!msgList.isEmpty()) msgList = msgList + sep_msg + msg;
						else msgList = msg;
					}
					if(!msgList.isEmpty()){
						SyncAllMessages(msgList);
					}

					syncDone = 1;
					Log.v("client", "Synchronization done");

					//SyncDoneRequest();

				}else if (msgs[0].toString().equalsIgnoreCase("sync-done-request")) {

					for (String dest : PORTS) {
						if(dest.equals(myPort)) continue;
						Log.v("client", "Sending sync done to " + dest);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(dest));
						sendMessage(socket, dest, "sync-off",myPort);
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
