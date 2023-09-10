import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

public class nmserver {

	private int port;
	private int serverID;
	private InetAddress serverIP;

	private int bnPort;
	private InetAddress bnIP;

	private int successorID;
	private int successorPort;
	private InetAddress successorIP;

	private int predecessorID;
	private int predecessorPort;
	private InetAddress predecessorIP;

	private ServerSocket server;

	private Vector<Integer> visitedNodes;

	private TreeMap<Integer, String> keyRange; // Stores current key range pairs of this node
	private int start; // beginning of range
	private int end; // end of range

	/*
	 * Constructor for Bootstrap Config is the configuration file of [ID, port,
	 * Initial keys] required to run the server
	 */
	public nmserver(File config) {

		// Scan the file for the server ID, port number, and Initial key value pairs
		try {
			Scanner sc = new Scanner(config);
			serverID = Integer.parseInt(sc.nextLine());
			port = Integer.parseInt(sc.nextLine());
			bnIP = InetAddress.getByName(sc.next());
			bnPort = Integer.parseInt(sc.next());
			sc.close();

			keyRange = new TreeMap<Integer, String>();
			visitedNodes = new Vector<Integer>();

			this.serverIP = InetAddress.getLocalHost();
		} catch (FileNotFoundException | UnknownHostException e) {
			System.out.println("Incorrect file format.");
		}

		// Create server on designated port from config
		try {
			server = new ServerSocket(port);
			server.setReuseAddress(true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		InputThread userInput = new InputThread();
		userInput.start();

		connect();
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
				recvCommand(cmd, in);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Close all connections
			finally {
				try {
					if (in != null)
						in.close();
					if (out != null)
						out.close();
					if (user != null)
						user.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	// Handles incoming commands that are not user input
	@SuppressWarnings("unchecked")
	public void recvCommand(String cmd, ObjectInputStream in) {
		try {
			// Command for enter forwarded by predecessor
			if (cmd.equals("enter")) {
				int nameID = in.readInt();
				int port = in.readInt();
				InetAddress ip = (InetAddress) in.readObject();

				rcvEnter(nameID, port, ip, in);
			}
			// Node entering ring must enter as predecessor to this node
			else if (cmd.equals("entrance")) {
				nodeEntry(in);
			}
			// Update successor info for predecessor of an entering node. Handled along with
			// updatePredecesor()
			else if (cmd.equalsIgnoreCase("successor")) {
				successorID = in.readInt();
				successorPort = in.readInt();
				successorIP = (InetAddress) in.readObject();
			}
			// Handles a forwarded lookup command, updating the visited nodes
			else if (cmd.equalsIgnoreCase("lookup")) {
				int key = in.readInt();
				visitedNodes = (Vector<Integer>) in.readObject();
				visitedNodes.add(serverID);
				lookup(key);
			}
			// Handles a forwarded insert command, updating the visited nodes
			else if (cmd.equalsIgnoreCase("insert")) {
				int key = in.readInt();
				String value = in.readUTF();
				visitedNodes = (Vector<Integer>) in.readObject();
				visitedNodes.add(serverID);
				insert(key, value);
			}
			// Handles a forwarded delete command, updating the visited nodes
			else if (cmd.equalsIgnoreCase("delete")) {
				int key = in.readInt();
				visitedNodes = (Vector<Integer>) in.readObject();
				visitedNodes.add(serverID);
				delete(key);
			} else if (cmd.equalsIgnoreCase("print")) {
				visitedNodes = (Vector<Integer>) in.readObject();
				visitedNodes.add(serverID);
				printValues();
			}
			// Current name server sends exit command
			else if (cmd.equals("exit")) {
				rcvExit(in);
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Forwarded message received, handles enter command appropriately and connects
	// to node trying to enter if it's in range
	@SuppressWarnings("unchecked")
	public void rcvEnter(int nameID, int port, InetAddress ip, ObjectInputStream in) {
		try {
			Vector<Integer> currentVisits = (Vector<Integer>) in.readObject();
			currentVisits.add(serverID);

			// New server is in range, contact node trying to enter
			if (inRange(nameID)) {
				Socket entryNode = null;
				ObjectOutputStream output = null;
				ObjectInputStream input = null;

				try {
					entryNode = new Socket(ip, port); // Contact node trying to enter
					output = new ObjectOutputStream(entryNode.getOutputStream());
					input = new ObjectInputStream(entryNode.getInputStream());
					
					// Entrance point found
					output.writeUTF("entrance");

					// Update predecessor info for new node
					output.writeInt(predecessorID);
					output.writeInt(predecessorPort);
					output.writeObject(predecessorIP);

					// Updates successor info for new node
					output.writeInt(serverID);
					output.writeInt(this.port);
					output.writeObject(serverIP);

					// Updates predecessor info for this node
					predecessorID = nameID;
					predecessorPort = port;
					predecessorIP = ip;

					// Send nodes visited
					output.writeObject(currentVisits);

					// Copy key-value pairs that are in the range of the node entering
					NavigableMap<Integer, String> nameRange = keyRange.subMap(start, false, nameID, true);
					output.writeObject(nameRange);

					// Set new start of key range
					start = nameID + 1;

					// Set new key range
					TreeMap<Integer, String> temp = new TreeMap<Integer, String>();
					nameRange = keyRange.subMap(start, true, end, true);
					temp.putAll(nameRange);
					keyRange.clear();
					keyRange = temp;
					temp = null;
					nameRange = null;
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (output != null)
							output.close();
						if (entryNode != null)
							entryNode.close();
					} catch (IOException e) {
					}
				}

			}
			// New node not in range, send request to next node
			else {
				Socket forward = null;
				ObjectOutputStream output = null;
				ObjectInputStream input = null;

				try {
					forward = new Socket(successorIP, successorPort);
					output = new ObjectOutputStream(forward.getOutputStream());
					input = new ObjectInputStream(forward.getInputStream());

					// Send command and entering server info
					output.writeUTF("enter");
					output.writeInt(nameID);
					output.writeInt(port);
					output.writeObject(ip);

					output.writeObject(currentVisits); // Sends list of visited nodes to successor

					currentVisits = null;
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (output != null)
							output.close();
						if (input != null)
						    input.close();
						if (input != null)
							input.close();
						if (forward != null)
							forward.close();
					} catch (IOException e) {
					}
				}

			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Handles exit of a node, updating that node's predecessor and successor
	@SuppressWarnings("unchecked")
	public void rcvExit(ObjectInputStream in) {
		try {
			// Current node is successor of exiting node
			if (in.readBoolean()) {
				predecessorID = in.readInt();
				predecessorPort = in.readInt();
				predecessorIP = (InetAddress) in.readObject();

				// Include exiting node's key-range in current key-range
				keyRange.putAll((NavigableMap<Integer, String>) in.readObject());
				start = predecessorID + 1;
			}
			// Current node is predecessor of exiting node
			else {
				successorID = in.readInt();
				successorPort = in.readInt();
				successorIP = (InetAddress) in.readObject();
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Handles entrance of new node once entry point is found via forwarded messages
	@SuppressWarnings("unchecked")
	public void nodeEntry(ObjectInputStream in) {
		try {
			// Get predecessor info
			predecessorID = in.readInt();
			predecessorPort = in.readInt();
			predecessorIP = (InetAddress) in.readObject();

			// Get successor info
			successorID = in.readInt();
			successorPort = in.readInt();
			successorIP = (InetAddress) in.readObject();

			// Get nodes visited before entry
			visitedNodes = (Vector<Integer>) in.readObject();

			// Get key-value pairs from successor
			keyRange.putAll((NavigableMap<Integer, String>) in.readObject());
			start = predecessorID + 1;
			end = serverID;

			updatePredecessor(); // Send message to predecessor so it can update it's successor info

			synchronized (System.out) {
				System.out.println("Entry successful");
				System.out.println("Range: [" + start + "," + end + "]");
				System.out.println("Predecessor: " + predecessorID);
				System.out.println("Successor: " + successorID);
				System.out.println("Nodes visited: " + visitedNodes.toString());
			}

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	// Checks to see if an entering node is in range of the current node
	public boolean inRange(int value) {
		if (start <= end)
			return value >= start && value <= end;
		else
			return value >= start || value <= end;
	}

	// Tells predecessor of entering node to update its successor info
	public void updatePredecessor() {
		Socket predecessor = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			predecessor = new Socket(predecessorIP, predecessorPort);
			out = new ObjectOutputStream(predecessor.getOutputStream());
			in = new ObjectInputStream(predecessor.getInputStream());

			out.writeUTF("successor");
			out.writeInt(serverID);
			out.writeInt(port);
			out.writeObject(InetAddress.getLocalHost());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
				if (predecessor != null)
					predecessor.close();
			} catch (IOException e) {
			}
		}

	}

	// Checks if key being looked-up is in this server's key range and returns
	// message to bootstrap server. If not, forward command
	public void lookup(int key) {
		Socket bootstrap = null;
		Socket successor = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			// Key can exist in this server's range
			if (inRange(key)) {
				// Connect to bootstrap to send info
				bootstrap = new Socket(bnIP, bnPort);
				out = new ObjectOutputStream(bootstrap.getOutputStream());
				in = new ObjectInputStream(bootstrap.getInputStream());

				out.writeUTF("lookup-msg");
				out.flush();

				// Checks if key is actually in range
				if (keyRange.containsKey(key)) {
					out.writeBoolean(true);
					out.flush();

					out.writeUTF(keyRange.get(key)); // Send value of key
					out.writeInt(serverID);
					out.writeObject(visitedNodes);
					out.flush();
				}
				// Key does not exist
				else {
					out.writeBoolean(false);
					out.flush();
				}
			}
			// Key can't exist in this server's range, forward command
			else {
				successor = new Socket(successorIP, successorPort);
				out = new ObjectOutputStream(successor.getOutputStream());
				in = new ObjectInputStream(successor.getInputStream());

				out.writeUTF("lookup");
				out.writeInt(key);
				out.writeObject(visitedNodes);
				out.flush();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}
	}

	// Checks if key being looked-up is in this server's key range, inserts key if
	// in range, and returns message to bootstrap server. If not, forward command
	public void insert(int key, String value) {
		Socket successor = null;
		Socket bootstrap = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			// Key can exist in this server's range
			if (inRange(key)) {
				keyRange.put(key, value);

				// Connect to bootstrap to send success msg
				bootstrap = new Socket(bnIP, bnPort);
				out = new ObjectOutputStream(bootstrap.getOutputStream());
				in = new ObjectInputStream(bootstrap.getInputStream());

				out.writeUTF("insert-msg");
				out.writeUTF("Key-Value pair inserted into server: (" + key + ", " + value + ")");
				out.writeUTF("Servers visited: " + visitedNodes.toString());
			}
			// Key can't exist in this server's range, forward command
			else {
				successor = new Socket(successorIP, successorPort);
				out = new ObjectOutputStream(successor.getOutputStream());
				in = new ObjectInputStream(successor.getInputStream());

				out.writeUTF("insert");
				out.writeInt(key);
				out.writeUTF(value);
				out.writeObject(visitedNodes);
				out.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}

	}

	// Checks if key being looked-up is in this server's key range, deletes key if
	// in range, and returns message to bootstrap server. If not, forward command
	public void delete(int key) {
		Socket successor = null;
		Socket bootstrap = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			// Key can exist in this server's range
			if (inRange(key)) {
				// Connect to bootstrap to send info
				bootstrap = new Socket(bnIP, bnPort);
				out = new ObjectOutputStream(bootstrap.getOutputStream());
				in = new ObjectInputStream(bootstrap.getInputStream());
				

				out.writeUTF("delete-msg");

				// Key exists in this server
				if (keyRange.containsKey(key)) {
					keyRange.remove(key);
					out.writeBoolean(true);
					out.flush();

					out.writeUTF("Successful deletion");
					out.writeUTF("Servers visited: " + visitedNodes.toString());
				}
				// Key doesn't exist in this server
				else {
					out.writeBoolean(false);
					out.flush();

					out.writeUTF("Key not found");
				}
			}
			// Key can't exist in this server's range
			else {
				successor = new Socket(successorIP, successorPort);
				out = new ObjectOutputStream(successor.getOutputStream());
				in = new ObjectInputStream(successor.getInputStream());

				out.writeUTF("delete");
				out.writeInt(key);
				out.writeObject(visitedNodes);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}
	}

	// Prints out all values, primarily for bug-testing purposes
	public void printValues() {
		// Convert Tree Map to Map type to iterate
		Set<Map.Entry<Integer, String>> entries = keyRange.entrySet();
		// For loop and lambda iterates through the tree map values
		System.out.println();
		entries.forEach(entry -> {
			System.out.println(serverID + " " + entry.getKey() + ", " + entry.getValue());
		});
		System.out.print("> ");

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

	/*
	 * Input Thread. A thread for inputs to handle the enter and exit commands. For
	 * enter command, handles immediate entry into bootstrap, otherwise waits for
	 * message to be forwarded from bootstrap until proper node for entry is found.
	 */
	class InputThread extends Thread {

		private String listenIP;

		private BufferedReader input;

		public InputThread() {
			input = new BufferedReader(new InputStreamReader(System.in));
		}

		public void run() {

			// Handles client commands until they quit
			while (true) {
				System.out.print("> ");
				String[] command = null;

				try {
					command = input.readLine().split(" ");
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (command[0].equalsIgnoreCase("enter")) {
					enter();
				} else if (command[0].equalsIgnoreCase("exit")) {
					exit();
				} else if (command[0].equalsIgnoreCase("quit")) {
					break;
				}
			}
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		public void read(String[] command) {
			if (command[0].equals("enter")) {
				enter();
			}
		}

		/*
		 * Contact bootstrap server and if it can enter immediately (is in range), enter
		 * into ring, otherwise do nothing after contacting bootstrap and wait for
		 * server in range to contact this node. If not immediate entry, a different
		 * enter command is utilized when the node in range contacts this node
		 */
		@SuppressWarnings("unchecked")
		public void enter() {
			Socket bnServer = null;
			ObjectOutputStream out = null;
			ObjectInputStream in = null;

			try {
				// Contacts bootstrap node
				bnServer = new Socket(bnIP, bnPort);
				out = new ObjectOutputStream(bnServer.getOutputStream());
				in = new ObjectInputStream(bnServer.getInputStream());

				out.writeUTF("enter");
				out.writeInt(serverID);
				out.writeInt(port);
				out.writeObject(InetAddress.getLocalHost());
				out.flush();

				boolean entryFound = in.readBoolean(); // Bootstrap confirms that this node can enter immediately

				if (entryFound) {
					boolean firstEntry = in.readBoolean(); // Bootstrap confirms this is first entry into system

					visitedNodes.add(0); // Update visitedNodes to include bootstrap node

					// If first entry confirmed, set predecessor and successor to bootstrap node
					if (firstEntry) {
						predecessorID = 0;
						predecessorPort = bnPort;
						predecessorIP = bnIP;

						successorID = 0;
						successorPort = bnPort;
						successorIP = bnIP;

						keyRange.putAll((NavigableMap<Integer, String>) in.readObject()); // Get key-value pairs from
																							// bootstrap node
						// Update key range
						start = predecessorID + 1;
						end = serverID;
					}

					// If entry in range of bootstrap node, but not first entry
					else {
						// Get predecessor info from bootstrap
						predecessorID = in.readInt();
						predecessorPort = in.readInt();
						predecessorIP = (InetAddress) in.readObject();

						// Update successor to be bootstrap node
						successorID = 0;
						successorPort = bnPort;
						successorIP = bnIP;

						keyRange.putAll((NavigableMap<Integer, String>) in.readObject()); // Get key-value pairs from

						// bootstrap node
						// Update key range
						start = predecessorID + 1;
						end = serverID;

						updatePredecessor();
					}

					// Print success message
					synchronized (System.out) {
						System.out.println("Successful entry");
						System.out.println("Range: [" + start + "," + end + "]");
						System.out.println("Predecessor: " + predecessorID);
						System.out.println("Successor: " + successorID);
						System.out.println("Nodes visited: " + visitedNodes.toString());
					}

				}
				// Entering node is not in range of bootstrap node
				else {
					synchronized (System.out) {
						System.out.println("Entry started, waiting for entry point to be found");
						try {
							sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (EOFException e) {
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Contact successor and predecessor to inform them of leaving. Update successor
		 * with current node's predecessor info and key range. Update predecessor with
		 * successor info.
		 */
		public void exit() {
			Socket successor = null;
			Socket predecessor = null;
			ObjectOutputStream out = null;
			ObjectInputStream in = null;
			NavigableMap<Integer, String> temp;

			visitedNodes.clear();

			try {
				// Current node is only node in the system
				if (successorID == predecessorID) {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());

					out.writeUTF("exit");

					// Send key range to bootstrap node
					temp = keyRange.subMap(start, false, end, true);
					out.writeObject(temp);
					temp = null;
					out.flush();
				}
				// Update successor and predecessor
				else {
					successor = new Socket(successorIP, successorPort);
					out = new ObjectOutputStream(successor.getOutputStream());
					in = new ObjectInputStream(successor.getInputStream());

					out.writeUTF("exit");
					out.writeBoolean(true); // Tells successor to handle exit as successor node
					out.flush();

					out.writeInt(predecessorID);
					out.writeInt(predecessorPort);
					out.writeObject(predecessorIP);
					out.flush();

					// Send key range to successor
					temp = keyRange.subMap(start, false, end, true);
					out.writeObject(temp);
					temp = null;
					out.flush();

					if (out != null)
						out.close();
					if (successor != null)
						successor.close();

					// Update predecessor
					predecessor = new Socket(predecessorIP, predecessorPort);
					out = new ObjectOutputStream(predecessor.getOutputStream());

					out.writeUTF("exit");
					out.writeBoolean(false); // Tells predecessor to handle exit as predecessor node
					out.flush();

					out.writeInt(successorID);
					out.writeInt(successorPort);
					out.writeObject(successorIP);
					out.flush();
				}

				// Print successful exit message
				synchronized (System.out) {
					System.out.println("Successful exit");
					System.out.println("SuccessorID: " + successorID);
					System.out.println("Key-range passed: [" + start + "," + end + "]");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (out != null)
						out.close();
					if (in != null)
						in.close();
					if (successor != null)
						successor.close();
					if (predecessor != null)
						predecessor.close();
				} catch (IOException e) {
				}

			}

		}

	}

	// Main
	public static void main(String[] args) {
		File config = new File(System.getProperty("user.dir") + "/" + args[0]);

		nmserver nmserver = new nmserver(config);
	}
}
