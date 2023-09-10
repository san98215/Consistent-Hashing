import java.net.*;
import java.io.*;
import java.util.*;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.NavigableMap;
import java.util.Vector;

public class bnserver {

	private int port;
	private int serverID;

	private int successorID;
	private int successorPort;
	private InetAddress successorIP;

	private int predecessorID;
	private int predecessorPort;
	private InetAddress predecessorIP;

	private ServerSocket server = null;

	private Vector<Integer> visitedNodes;

	private TreeMap<Integer, String> keyRange; // Stores current key range pairs of this node
	private int start; // beginning of range
	private int end; // end of range

	/*
	 * Constructor for Bootstrap Config is the configuration file of [ID, port,
	 * Initial keys] required to run the server
	 */
	public bnserver(File config) {

		// Scan the file for the server ID, port number, and Initial key value pairs
		try {
			Scanner sc = new Scanner(config);
			serverID = Integer.parseInt(sc.nextLine());
			port = Integer.parseInt(sc.nextLine());

			keyRange = new TreeMap<Integer, String>();
			while (sc.hasNextLine()) {
				String[] line = sc.nextLine().split(" ");
				keyRange.put(Integer.parseInt(line[0]), line[1]);
			}
			sc.close();

		} catch (FileNotFoundException e) {
			System.out.println("Incorrect file format.");
		}

		this.successorID = serverID;
		this.predecessorID = serverID;
		this.start = 1;
		this.end = serverID;

		try {
			// Create server on designated port from config
			server = new ServerSocket(port);
			server.setReuseAddress(true);

			InputThread userInput = new InputThread();
			userInput.start();

			connect();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// Handle incoming connections
	public void connect() {

		while (true) {
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			Socket user = null;
			try {
				user = server.accept();
				in = new ObjectInputStream(user.getInputStream());
				out = new ObjectOutputStream(user.getOutputStream());

				String cmd = in.readUTF();
				recvCommand(cmd, in, out);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null)
						in.close();
					if (out != null)
						out.close();
					if (user != null)
						user.close();
				} catch (IOException e) {
				}
			}
		}

	}

	// Handles incoming commands that are not user input
	@SuppressWarnings("unchecked")
	public void recvCommand(String cmd, ObjectInputStream in, ObjectOutputStream out) {
		try {
			// Current name server sends enter command
			if (cmd.equalsIgnoreCase("enter")) {
				int nameID = in.readInt();
				int port = in.readInt();
				InetAddress ip = (InetAddress) in.readObject();

				enter(nameID, port, ip, out);
			}
			// Current name server sends exit command
			else if (cmd.equalsIgnoreCase("exit")) {
				exit(in);
			} else if (cmd.equalsIgnoreCase("successor")) {
				successorID = in.readInt();
				successorPort = in.readInt();
				successorIP = (InetAddress) in.readObject();
			}
			// Handle information received from lookup command
			else if (cmd.equalsIgnoreCase("lookup-msg")) {
				boolean exists = in.readBoolean();
				if (exists) {
					String value = in.readUTF();
					int finalServer = in.readInt();
					Vector<Integer> lookups = (Vector<Integer>) in.readObject();

					System.out.println("Value: " + value);
					System.out.println("Servers visited: " + lookups.toString());
					System.out.println("Final server: " + finalServer);
				} else {
					System.out.println("Key not found");
				}
			}
			// Handle message received from insert command
			else if (cmd.equalsIgnoreCase("insert-msg")) {
				System.out.println(in.readUTF());
				System.out.println(in.readUTF());
			}
			// Handle message received from delete command
			else if (cmd.equalsIgnoreCase("delete-msg")) {
				if (in.readBoolean()) {
					System.out.println(in.readUTF());
					System.out.println(in.readUTF());
				} else {
					System.out.println(in.readUTF());
				}

			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void enter(int nameID, int port, InetAddress ip, ObjectOutputStream out) {
		try {
			// First entry of name server
			if (successorID == serverID) {
				out.writeBoolean(true); // Tells new node that it is going to enter from this node
				out.flush();
				out.writeBoolean(true); // Tells new node that it is the first new node
				out.flush();

				successorID = nameID;
				predecessorID = nameID;

				successorPort = port;
				predecessorPort = port;

				successorIP = ip;
				predecessorIP = ip;

				// Copy key-value pairs that are in the range of the node entering
				NavigableMap<Integer, String> nameRange = keyRange.subMap(start, false, nameID, true);
				out.writeObject(nameRange);
				out.flush();

				start = nameID + 1;

				// Set new key range
				TreeMap<Integer, String> temp = new TreeMap<Integer, String>();
				nameRange = keyRange.subMap(start, true, 1024, true);
				if (keyRange.containsKey(serverID))
					temp.put(serverID, keyRange.get(serverID));
				temp.putAll(nameRange);
				keyRange.clear();
				keyRange = temp;
				temp = null;
				nameRange = null;
			}
			// New server is in range of bootstrap server
			else if (inRange(nameID)) {
				out.writeBoolean(true);
				out.flush();
				out.writeBoolean(false);
				out.flush();

				// Update predecessor for new node
				out.writeInt(predecessorID);
				out.writeInt(predecessorPort);
				out.writeObject(predecessorIP);

				predecessorID = nameID;
				predecessorPort = port;
				predecessorIP = ip;

				// Copy key-value pairs that are in the range of the node entering
				NavigableMap<Integer, String> nameRange = keyRange.subMap(start, false, nameID, true);
				out.writeObject(nameRange);
				nameRange = null;

				start = nameID + 1;

				// Set new key range
                                TreeMap<Integer, String> temp = new TreeMap<Integer, String>();
                                nameRange = keyRange.subMap(start, true, 1024, true);
                                if (keyRange.containsKey(serverID))
                                        temp.put(serverID, keyRange.get(serverID));
                                temp.putAll(nameRange);
                                keyRange.clear();
                                keyRange = temp;
                                temp = null;
                                nameRange = null;

				out.flush();
			}
			// New node not in range, send request to next node
			else {
				out.writeBoolean(false);
				out.flush();

				Socket forward = null;
				ObjectOutputStream output = null;
				ObjectInputStream input = null;

				try {
					forward = new Socket(successorIP, successorPort);
					output = new ObjectOutputStream(forward.getOutputStream());
					input = new ObjectInputStream(forward.getInputStream());

					output.writeUTF("enter");
					output.writeInt(nameID);
					output.writeInt(port);
					output.writeObject(ip);
					output.flush();

					visitedNodes = new Vector<Integer>();
					visitedNodes.add(serverID); // Adds node to list of visited nodes
					output.writeObject(visitedNodes); // Sends list of visited nodes to successor

					visitedNodes = null;
					output.flush();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (output != null)
							output.close();
						if (input != null)
							input.close();
						if (forward != null)
							forward.close();
					} catch (IOException e) {
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Handles exit of a node, updating that node's predecessor and successor
	@SuppressWarnings("unchecked")
	public void exit(ObjectInputStream in) {
		try {
			// Handles exit if there is only one other node in system
			if (successorID == predecessorID) {
				successorID = predecessorID = 0;
				start = 1;
				end = 0;

				keyRange.putAll((NavigableMap<Integer, String>) in.readObject());
			} else {
				// Updates info for successor
				if (in.readBoolean()) {
					predecessorID = in.readInt();
					predecessorPort = in.readInt();
					predecessorIP = (InetAddress) in.readObject();

					keyRange.putAll((NavigableMap<Integer, String>) in.readObject());
					start = predecessorID + 1;
				}
				// Updates infor for predecessor
				else {
					successorID = in.readInt();
					successorPort = in.readInt();
					successorIP = (InetAddress) in.readObject();
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	// Checks to see if an entering node is in range of the current node
	public boolean inRange(int nameID) {
		if (start <= end)
			return nameID >= start && nameID <= end;
		else
			return nameID >= start || nameID <= end;
	}

	/*
	 * Input Thread. A thread for inputs This is the thread to spawn off the
	 * bootstrap server and handle commands
	 */
	class InputThread extends Thread {

		private String listenIP;

		private BufferedReader input;

		public InputThread() {
			input = new BufferedReader(new InputStreamReader(System.in));
		}

		public void run() {
			try {
				// Handles client commands until they quit
				while (true) {
					System.out.print("> ");
					String[] command = input.readLine().split(" ");

					if (command[0].equalsIgnoreCase("lookup")) {
						int key = Integer.parseInt(command[1]);
						lookup(key);
					} else if (command[0].equalsIgnoreCase("insert")) {
						int key = Integer.parseInt(command[1]);
						String value = command[2];
						insert(key, value);
					} else if (command[0].equalsIgnoreCase("delete")) {
						int key = Integer.parseInt(command[1]);
						delete(key);
					} else if (command[0].equalsIgnoreCase("print")) {
						printValues();
					} else if (command[0].equalsIgnoreCase("quit")) {
						break;
					}

					sleep(20);
				}
				input.close();
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Checks if key being looked-up is in this server's key range and prints
		// message. If not, forward command to successor
		public void lookup(int key) {
			// Key can exist in this server's range
			if (inRange(key)) {
				// Checks if key is actually in range
				if (keyRange.containsKey(key)) {
					System.out.println("Value: " + keyRange.get(key));
					System.out.println("Servers visited: [" + serverID + "]");
					System.out.println("Final server: " + serverID);
				} else {
					System.out.println("Key not found");
				}
			}
			// Key can't exist in this server's range, forward command to successor
			else {
				Socket successor = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;

				try {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());

					Vector<Integer> lookups = new Vector<Integer>();
					lookups.add(serverID);

					out.writeUTF("lookup");
					out.writeInt(key);
					out.writeObject(lookups);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

		// Checks if key being looked-up is in this server's key range, inserts key if
		// in range, and prints message. If not, forward command to successor
		public void insert(int key, String value) {
			if (inRange(key)) {
				keyRange.put(key, value);
				System.out.println("Key-Value pair inserted into server: (" + key + ", " + value + ")");
				System.out.println("Servers visited: [" + serverID + "]");

			} else {
				Socket successor = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;

				try {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());

					Vector<Integer> lookups = new Vector<Integer>();

					out.writeUTF("insert");
					out.writeInt(key);
					out.writeUTF(value);
					lookups.add(serverID);
					out.writeObject(lookups);
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}
		}

		// Checks if key being looked-up is in this server's key range, deletes key if
		// in range, and prints message. If not, forward command to successor
		public void delete(int key) {

			if (inRange(key)) {
				if (keyRange.containsKey(key)) {
					keyRange.remove(key);
					System.out.println("Successful deletion");
					System.out.println("Servers visited: [" + serverID + "]");
				} else {
					System.out.println("Key not found");
				}
			} else {
				Socket successor = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;

				try {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());

					Vector<Integer> lookups = new Vector<Integer>();

					out.writeUTF("delete");
					out.writeInt(key);
					lookups.add(serverID);
					out.writeObject(lookups);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}

		// Prints out all values, primarily for bug-testing purposes
		public void printValues() {
			// Convert Tree Map to Map type to iterate
			Set<Map.Entry<Integer, String>> entries = keyRange.entrySet();
			// For loop and lambda iterates through the tree map values
			entries.forEach(entry -> {
				System.out.println(serverID + " " + entry.getKey() + ", " + entry.getValue());
			});

			// Pass on command to next ID if it exists
			if (successorID != serverID) {
				Socket successor = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;

				try {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());
					Vector<Integer> lookups = new Vector<Integer>();

					out.writeUTF("print");
					lookups.add(serverID);
					out.writeObject(lookups);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}

	}

	// Main
	public static void main(String[] args) {
		File config = new File(System.getProperty("user.dir") + "/" + args[0]);

		bnserver boostrap = new bnserver(config);
	}

}
