import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class func {

	public static PrintStream out = null;
	public static BufferedReader in = null;
	static ArrayList<String> file_table = new ArrayList<String>();
	static ArrayList<String> query_table = new ArrayList<String>();

	public static String hashfunc(String hash) throws NoSuchAlgorithmException {

		// Generating hash key based on port no
		byte[] ipInBytes = hash.getBytes();
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] thedigest = md.digest(ipInBytes);
		BigInteger bigInt = new BigInteger(1, thedigest);
		String ipHash = bigInt.toString(16);
		// System.out.println("hashkey" + ipHash);
		String hashkey = ipHash.substring(ipHash.length() - 6, ipHash.length());
		// System.out.println("hashkey" + hashkey);
		return hashkey;

	}

	// Printing Arrays
	public static void printRow(long[] row) {
		for (long i : row) {
			System.out.print(i);
			System.out.print("\t");
		}
		System.out.println();
	}

	// Allocating Files to node
	public static ArrayList<String> file_allocation() throws Exception {

		// Open input file and read line
		BufferedReader reader = new BufferedReader(new FileReader(
				"filenames.txt"));
		List<String> lines = new ArrayList<String>();
		String line = reader.readLine();

		// read until end of line and storing in array lines
		while (line != null) {
			line.trim();
			line = line.toLowerCase().replaceAll(" ", "_");
			lines.add(line);
			line = reader.readLine();
		}

		// Close the buffered reader object
		reader.close();

		Random random = new Random();

		// initialize 7 to 10 movie names to each node
		int num = random.nextInt(4) + 7;
		System.out.println("-------------------------------------------------");
		System.out.println("Allocating " + num + " Files to the node");
		System.out.println("The Files allocated to the node are as follows: ");
		System.out.println("-------------------------------------------------");

		// Create a new file to store the contents at each node
		File file = new File("lab5_files.txt");

		// if file doesn't exist, then create it
		if (!file.exists()) {
			file.createNewFile();
		} else {
			file.delete();
		}

		// Create unique number to be picked
		ArrayList<Integer> unique_list = new ArrayList<Integer>();
		for (int rand = 1; rand < lines.size(); rand++) {
			unique_list.add(new Integer(rand));
		}
		Collections.shuffle(unique_list);

		// Allocating 7-10 files to node
		for (int i = 0; i < num; i++) {

			// Choose a random line from the list
			String randomString = lines.get(unique_list.get(i));
			System.out.println(randomString);

			// Create a new file to store the contents at each node
			file = new File("lab5_files.txt");

			// if file doesnt exist, then create it

			if (!file.exists()) {
				file.createNewFile();
			}

			// true = append file, create buffered write and file write objects
			FileWriter fileWritter = new FileWriter(file.getName(), true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.write(randomString + "\n");
			bufferWritter.close();

		}

		System.out
				.println("-------------------------------------------------\n");

		// READ THE MOVIES FROM THE FILE
		reader = new BufferedReader(new FileReader("lab5_files.txt"));

		lines = new ArrayList<String>();

		line = reader.readLine();

		String ipport = tra.localip.trim() + " " + tra.self_port;

		// read untill end of line
		while (line != null) {

			String file_key = hashfunc(line.trim());

			// Call update file table to store
			update_file_table(file_key, ipport, line);

			// split and get words and hash it
			String[] movie_split = line.split("_");

			if (movie_split.length > 1) {

				for (int i = 0; i < movie_split.length; i++) {

					file_key = hashfunc(movie_split[i].trim());
					update_file_table(file_key, ipport, line);

				}

			}

			line = reader.readLine();

		}
		return file_table;

	}

	// Allocating Files to node
	public static ArrayList<String> query_read() throws Exception {

		// Open input file and read line
		BufferedReader reader = new BufferedReader(
				new FileReader("queries.txt"));
		List<String> lines = new ArrayList<String>();
		String line = reader.readLine();

		// read until end of line and storing in array lines
		while (line != null) {
			line.trim();
			line = line.toLowerCase().replaceAll(" ", "_");
			lines.add(line);
			line = reader.readLine();
		}

		// Close the buffered reader object
		reader.close();

		System.out.println(" Creating Query table");

		// READ THE MOVIES FROM THE FILE

		for (int i = 0; i < lines.size(); i++) {

			// read untill end of line
			if (lines.get(i) != null) {

				String file_key = hashfunc(lines.get(i).trim());

				// Call update file table to store
				update_query_table(file_key, lines.get(i), lines.get(i));

				// split and get words and hash it
				String[] movie_split = lines.get(i).split("_");

				if (movie_split.length > 1) {

					for (int j = 0; j < movie_split.length; j++) {

						file_key = hashfunc(movie_split[j].trim());
						update_query_table(file_key, movie_split[j],
								lines.get(i));

					}

				}

			}
		}

		for (int i = 0; i < query_table.size(); i++) {

			if (i % 3 == 0 && i != 0) {
				System.out.println();
			}

			System.out.print(query_table.get(i));
			System.out.print("\t" + "\t");
		}

		System.out.println("\n");

		return query_table;

	}

	public static void update_query_table(String key, String word,
			String movie_name) {

		query_table.add(key);
		query_table.add(word);
		query_table.add(movie_name);

	}

	public static void update_file_table(String key, String word,
			String movie_name) {

		file_table.add(key);
		file_table.add(word);
		file_table.add(movie_name);

	}

	public static void sendok(String mess, String ip, String port)
			throws Exception {

		int length = 8 + mess.length();

		StringBuilder packet = new StringBuilder();

		packet.append("00" + length).append(" ").append(mess).append(" ")
				.append("0");

		String message = packet.toString().trim();

		out.println(message);

		System.out.println("Message sent :" + message + "\n");

	}

	public static void send(StringBuilder packet, String ip, String port)
			throws Exception {

		String message = packet.toString().trim();

		// Opening socket and sending getkey
		InetSocketAddress send_inet_socket = new InetSocketAddress(ip.trim(),
				Integer.parseInt(port.trim()));
		Socket send_socket = new Socket();
		send_socket.connect(send_inet_socket);

		out = new PrintStream(send_socket.getOutputStream());
		in = new BufferedReader(new InputStreamReader(
				send_socket.getInputStream()));

		out.println(message);

		System.out.println("Message sent :" + message + "\n");

		send_socket.close();

	}

	public static String predecessor(String key) {

		int index_key = tra.livenodekey.indexOf(key);

		String predecessor_key = null;

		if (index_key == 0) {

			predecessor_key = tra.livenodekey.get(tra.livenodekey.size() - 1);
		}

		else {

			predecessor_key = tra.livenodekey.get(index_key - 1);
		}

		return predecessor_key;
	}

	public static String successor(String key) {

		int index_key = tra.livenodekey.indexOf(key);

		String successor_key = null;

		if (index_key == tra.livenodekey.size() - 1) {

			successor_key = tra.livenodekey.get(0);
		}

		else {

			successor_key = tra.livenodekey.get(index_key + 1);
		}

		return successor_key;
	}

	public static String reg_node(long key) {

		long reg_node_key = 0;

		for (int i = 0; i < 24; i++) {

			if (key >= tra.finger_table[i][0] && key < tra.finger_table[i][1]) {

				reg_node_key = tra.finger_table[i][2];

			}

			if (key >= tra.finger_table[i][0]
					&& tra.finger_table[i][0] > tra.finger_table[i][1]) {

				reg_node_key = tra.finger_table[i][2];
			}
		}

		String reg_key_hex = Long.toHexString(reg_node_key);
		tra.getkeystore.add(reg_key_hex);

		return reg_key_hex;
	}

	public static void own_file_store(String output) {

		String[] split = output.split(" ");
		int no_of_keys = Integer.parseInt(split[2].trim());

		for (int i = 1; i < no_of_keys + 1; i++) {

			if (no_of_keys == 1) {

				// Adding key and filename
				func.file_table.add(split[5].trim());
				func.file_table.add(split[3].trim() + " " + split[4].trim());
				func.file_table.add(split[6].trim());

				tra.ownfilelist.add(split[5].trim());
			}

			else {

				// Adding key and filename
				func.file_table.add(split[5 + (i - 1) * 4].trim());
				func.file_table.add(split[3 + (i - 1) * 4].trim() + " "
						+ split[4 + (i - 1) * 4].trim());
				func.file_table.add(split[6 + (i - 1) * 4].trim());

				tra.ownfilelist.add(split[5 + (i - 1) * 4].trim());
			}
		}

		for (int i = 0; i < file_table.size(); i++) {

			if (i % 3 == 0 && i != 0) {
				System.out.println();
			}

			System.out.print(file_table.get(i));
			System.out.print("\t");
		}

		System.out.println("\n");

	}

	public static void add_send(String file_key, String file_name,
			String ipaddress, String port) throws Exception {

		long file_key_dec = Long.parseLong(file_key.trim(), 16);

		// Iterate thru fingertable and check key with own node or succ
		for (int j = 0; j < 24; j++) {

			// if file key same as entry then send to succ and if succ
			// is not own node
			if ((file_key_dec == tra.finger_table[j][0])
					&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

				// Extracting successor in fingertable
				long succ = tra.finger_table[j][2];
				String succ_hex = Long.toHexString(succ);

				// Check if succ is own node
				if (tra.hashkey_decimal == succ) {

					tra.ownfilelist.add(file_key);
					System.out.println("The" + file_name + " with key "
							+ file_key_dec + " belongs to the own node \n");
				}

				// Send the key to succ
				else {

					int index_succ = tra.otherkey.indexOf(succ_hex);

					// choot code
					if (index_succ == -1) {
						StringBuilder packet = new StringBuilder();
						packet.append(0).append(succ_hex);
						succ_hex = packet.toString().trim();
						index_succ = tra.otherkey.indexOf(succ_hex);

					}

					// Extracting the Succussor IP and Port
					String succ_ip = tra.otherkey.get(index_succ + 1);
					String succ_port = tra.otherkey.get(index_succ + 2);

					// Building Packet
					int add_length = 13 + tra.self_port.length()
							+ ipaddress.length() + file_key.length()
							+ file_name.length();

					StringBuilder add_packet = new StringBuilder();

					add_packet.append("00" + add_length).append(" ")
							.append("ADD").append(" ").append(ipaddress)
							.append(" ").append(tra.self_port).append(" ")
							.append(file_key).append(" ").append(file_name)
							.append(" ").append(succ_ip).append(" ")
							.append(succ_port);
					String add_message = add_packet.toString().trim();

				}

			}

		}

		// check filekey with interval
		for (int k = 0; k < 24; k++) {

			// Enter if succ is not own node
			if (tra.finger_table[k][2] != tra.hashkey_decimal) {

				// Check if file key is withing range of start and stop
				// || check if key belongs to overflow condition

				if ((file_key_dec > tra.finger_table[k][0] && file_key_dec < tra.finger_table[k][1])
						|| ((file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
						|| ((file_key_dec < tra.finger_table[k][0])
								&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec < tra.finger_table[k][1]))) {

					// Extracting successor of the finger table
					long succ = tra.finger_table[k][2];
					String succ_hex = Long.toHexString(succ);
					int index_succ = tra.otherkey.indexOf(succ_hex);

					// choot code
					if (index_succ == -1) {
						StringBuilder packet = new StringBuilder();
						packet.append(0).append(succ_hex);
						succ_hex = packet.toString().trim();
						index_succ = tra.otherkey.indexOf(succ_hex);

					}

					// Extracting Successors IP and Port
					String succ_ip = tra.otherkey.get(index_succ + 1);
					String succ_port = tra.otherkey.get(index_succ + 2);

					// Building packet
					int length_add = 13 + tra.self_port.length()
							+ ipaddress.length() + file_key.length()
							+ file_name.length();

					StringBuilder packet_add = new StringBuilder();

					packet_add.append("00" + length_add).append(" ")
							.append("ADD").append(" ").append(ipaddress)
							.append(" ").append(tra.self_port).append(" ")
							.append(file_key).append(" ").append(file_name)
							.append(" ").append(succ_ip).append(" ")
							.append(succ_port);

					String add_message = packet_add.toString().trim();

				}

			}
		}
	}

	public static void ownfiles_delete_exit() {

		for (int i = 0; i < tra.inital_filetable_size; i++) {

			func.file_table.remove(0);

		}

		for (int i = 0; i < func.file_table.size(); i++) {

			if (i % 3 == 0 && i != 0) {
				System.out.println();
			}

			System.out.print(func.file_table.get(i));
			System.out.print("\t");
		}

		System.out.println("\n");

	}

	public static void results(String output, long delay, String hops)
			throws Exception {

		File file = new File("results" + "_" + tra.self_port + ".txt");
		FileWriter fileWritter = new FileWriter(file.getName(), true);
		BufferedWriter bufferWritter = new BufferedWriter(fileWritter);

		bufferWritter
				.write("---------------------------------------------------------------- \n");

		bufferWritter.write(output + "\n");

		bufferWritter.write("\n");

		bufferWritter.write("The Delay for the Query is: " + delay + "\n");
		bufferWritter.write("The total no of hops for the Query is : " + hops
				+ "\n");

		bufferWritter
				.write("---------------------------------------------------------------- \n");
		bufferWritter.write("\n");

		bufferWritter.close();

	}

	public static void ser_send(String file_key, String file_name,
			String ipaddress, String port) throws Exception {

		long file_key_dec = Long.parseLong(file_key.trim(), 16);

		// Iterate thru fingertable and check key
		for (int j = 0; j < 24; j++) {

			// if file key same as entry then send to succ and if succ
			// is not own node
			if ((file_key_dec == tra.finger_table[j][0])
					&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

				// Get the Succussor
				long succ = tra.finger_table[j][2];
				String succ_hex = Long.toHexString(succ);

				// Check if succ is own node
				if (tra.hashkey_decimal == succ) {
					System.out
							.println("The succ in finger table is own node so the Query "
									+ file_name
									+ " with key "
									+ file_key_dec
									+ " belongs to the Querying node \n");
				}

				// Send the key to succ
				else {

					int index_succ = tra.otherkey.indexOf(succ_hex);

					// choot code
					if (index_succ == -1) {
						StringBuilder packet = new StringBuilder();
						packet.append(0).append(succ_hex);
						succ_hex = packet.toString().trim();
						index_succ = tra.otherkey.indexOf(succ_hex);

					}

					// Getting IP and Port of successor
					String succ_ip = tra.otherkey.get(index_succ + 1);
					String succ_port = tra.otherkey.get(index_succ + 2);

					// Building Search Packet
					int length_ser = 13 + tra.self_port.length()
							+ ipaddress.length() + file_key.length();

					StringBuilder ser_packet = new StringBuilder();

					ser_packet.append("00" + length_ser).append(" ")
							.append("SER").append(" ").append(ipaddress)
							.append(" ").append(tra.self_port).append(" ")
							.append(file_key).append(" ").append("0");
					String ser_message = ser_packet.toString().trim();

					Socket ser_frwd = new Socket();

					// Opening socket and sending SER
					InetSocketAddress ser_frwd_inet = new InetSocketAddress(
							succ_ip, Integer.parseInt(succ_port.trim()));
					ser_frwd.connect(ser_frwd_inet);

					PrintStream ser_out = new PrintStream(
							ser_frwd.getOutputStream());

					ser_out.println(ser_message);
					System.out.println("SER message sent :" + ser_message);
					ser_frwd.close();

				}

			}

		}

		// check if query key belongs to Interval
		for (int k = 0; k < 24; k++) {

			// Enter if succ is not own node
			if (tra.finger_table[k][2] != tra.hashkey_decimal) {

				// Check if file key is withing range of start and stop
				// || check if key belongs to overflow condition

				if ((file_key_dec > tra.finger_table[k][0] && file_key_dec < tra.finger_table[k][1])
						|| ((file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
						|| ((file_key_dec < tra.finger_table[k][0])
								&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec < tra.finger_table[k][1]))) {

					// Extracting successor
					long succ = tra.finger_table[k][2];
					String succ_hex = Long.toHexString(succ);
					int index_succ = tra.otherkey.indexOf(succ_hex);

					// choot code
					if (index_succ == -1) {
						StringBuilder packet = new StringBuilder();
						packet.append(0).append(succ_hex);
						succ_hex = packet.toString().trim();
						index_succ = tra.otherkey.indexOf(succ_hex);

					}

					// Extracting IP and Port of Succussor
					String succ_ip = tra.otherkey.get(index_succ + 1);
					String succ_port = tra.otherkey.get(index_succ + 2);

					// Building SER packet
					int length_ser = 13 + tra.self_port.length()
							+ ipaddress.length() + file_key.length();

					StringBuilder ser_packet1 = new StringBuilder();

					ser_packet1.append("00" + length_ser).append(" ")
							.append("SER").append(" ").append(ipaddress)
							.append(" ").append(tra.self_port).append(" ")
							.append(file_key).append(" ").append("0");
					String ser_message1 = ser_packet1.toString().trim();

					Socket ser_frwd1 = new Socket();

					// Opening socket and sending SER
					InetSocketAddress ser_frwd_inet1 = new InetSocketAddress(
							succ_ip, Integer.parseInt(succ_port.trim()));
					ser_frwd1.connect(ser_frwd_inet1);

					PrintStream ser_out1 = new PrintStream(
							ser_frwd1.getOutputStream());

					ser_out1.println(ser_message1);
					System.out.println("SER message sent :" + ser_message1);
					ser_frwd1.close();

				}

			}
		}
	}

}
