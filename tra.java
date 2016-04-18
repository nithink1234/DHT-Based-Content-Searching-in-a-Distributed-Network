import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;

public class tra {

	public static Socket clientsocket = null;
	public static String self_port = null;
	public static String[] response = null;
	public static ArrayList<String> otherkey = new ArrayList<String>();
	public static ArrayList<String> livenodekey = new ArrayList<String>();
	public static ArrayList<String> ownfilelist = new ArrayList<String>();
	public static ArrayList<String> upfinnodes = new ArrayList<String>();
	public static ArrayList<String> getkeystore = new ArrayList<String>();
	public static int no_of_nodes = 0;
	public static String hashkey = null;
	public static long hashkey_decimal = 0;
	public static long[][] finger_table = new long[24][3];
	public static String localip = null;
	public static String bootstrap_name = null;
	public static String bootstrap_port = null;
	public static int hops_updated = 0;
	public static int messages_rcvd = 0;
	public static int messages_frwd = 0;
	public static int messages_answd = 0;
	public static int inital_filetable_size = 0;

	public static void main(String[] args) throws Exception {

		// Taking the arguments
		self_port = args[0];
		bootstrap_name = args[1];
		bootstrap_port = args[2];

		ServerSocket receive_socket = new ServerSocket(
				Integer.parseInt(tra.self_port.trim()));

		// Converting String boostrap_name and port to IP and int port
		// respectively
		InetAddress bootstrap_inet = InetAddress.getByName(bootstrap_name);
		String bootstrap_ip = bootstrap_inet.getHostAddress().trim();
		int bootstrap_port_int = Integer.parseInt(bootstrap_port);

		// Getting IP address of the client
		InetAddress localipinet = InetAddress.getLocalHost();
		localip = localipinet.getHostAddress().trim();

		clientsocket = new Socket(bootstrap_ip, bootstrap_port_int);

		// Declaring to Read and Write
		PrintStream out = new PrintStream(clientsocket.getOutputStream());
		BufferedReader in = new BufferedReader(new InputStreamReader(
				clientsocket.getInputStream()));

		// Generating hashkey based on port no by calling func class
		hashkey = func.hashfunc(self_port);

		// Preparing a REG message
		StringBuilder packet = new StringBuilder();
		int length = 13 + self_port.length() + bootstrap_ip.length()
				+ hashkey.length();

		// Making the packet ready for transmission to server
		packet.append("00" + length).append(" ").append("REG").append(" ")
				.append(localip).append(" ").append(self_port).append(" ")
				.append(hashkey).append("\n");

		// Sending the REG message
		String reg_message = packet.toString();
		System.out.println("Register message: " + reg_message);
		out.println(reg_message);

		// Receiving the Reg OK message
		String output = in.readLine();
		System.out.println(output);
		response = output.split(" ");

		out.close();
		in.close();

		// Closing Socket bound to bootstrap
		clientsocket.close();

		// Building Finger table
		hashkey_decimal = Long.parseLong(hashkey.trim(), 16);
		System.out.println("ownkey " + hashkey_decimal);
		// long[][] finger_table = new long[8][3];

		// Calculating Start entry
		for (int i = 0; i < 24; i++) {
			long func = (long) ((Math.pow(2, i) + hashkey_decimal) % Math.pow(
					2, 25));
			finger_table[i][0] = func;
		}

		// Calculating Interval
		for (int i = 0; i < 23; i++) {
			finger_table[i][1] = finger_table[i + 1][0];
		}
		finger_table[7][1] = finger_table[0][0];

		// Filling Succesor

		no_of_nodes = Integer.parseInt(response[2]);

		// Declare array to store otherkeys, IPs, ports

		// Storing self node details first
		// System.out.println(hashkey);
		otherkey.add(hashkey);
		otherkey.add(localip);
		otherkey.add(self_port);

		// When 0 nodes fill own key
		if (no_of_nodes == 0) {
			for (int i = 0; i < 24; i++) {
				finger_table[i][2] = hashkey_decimal;
			}

			// Allocating files to node by calling alloc func
			func.file_allocation();

			for (int i = 0; i < func.file_table.size(); i++) {

				if (i % 3 == 0 && i != 0) {
					System.out.println();
				}

				System.out.print(func.file_table.get(i));
				System.out.print("\t");
			}

			System.out.println("\n");

			// Extract only keys and convert them to decimal
			for (int k = 0; k < otherkey.size(); k = k + 3) {
				livenodekey.add(otherkey.get(k));
			}

			// Sort the keys and print sorted keys
			Collections.sort(livenodekey);
			for (int i = 0; i < livenodekey.size(); i++) {
				long inter = Long.parseLong(livenodekey.get(i).trim(), 16);
				// System.out.println("num " + inter);
			}

			// Copying livenodes elements to upfin
			for (int k = 0; k < livenodekey.size(); k++) {
				upfinnodes.add(livenodekey.get(k));
			}

			for (long[] row : finger_table) {
				func.printRow(row);
			}
		}

		else {

			// Store the keys, Ips, ports in the other array
			for (int k = 1; k < no_of_nodes + 1; k++) {
				// key
				otherkey.add(response[2 + k * 3]);
				// IP
				otherkey.add(response[k * 3]);
				// Port
				otherkey.add(response[1 + k * 3]);
			}

			// Extract only keys and convert them to decimal
			for (int k = 0; k < otherkey.size(); k = k + 3) {
				livenodekey.add(otherkey.get(k));
			}

			// Sort the keys and print sorted keys
			Collections.sort(livenodekey);
			for (int i = 0; i < livenodekey.size(); i++) {
				long inter = Long.parseLong(livenodekey.get(i).trim(), 16);
				// System.out.println("num " + inter);
			}

			// Copying livenodes elements to upfin
			for (int k = 0; k < livenodekey.size(); k++) {
				upfinnodes.add(livenodekey.get(k));
			}

			// Fill all succ as lowest entry
			for (int i = 0; i < 24; i++) {
				finger_table[i][2] = Long.parseLong(livenodekey.get(0).trim(),
						16);
			}

			// update succ if key > start && succ = key
			for (int k = 0; k < no_of_nodes + 1; k++) {
				for (int i = 0; i < 24; i++) {
					if (finger_table[i][0] > finger_table[i][2]
							&& finger_table[i][0] <= Long.parseLong(livenodekey
									.get(k).trim(), 16)) {
						finger_table[i][2] = Long.parseLong(livenodekey.get(k)
								.trim(), 16);
					}
				}
			}

			// Update succ when start = any live nodes
			for (int k = 0; k < no_of_nodes + 1; k++) {
				for (int i = 0; i < 24; i++) {
					if (finger_table[i][0] == Long.parseLong(livenodekey.get(k)
							.trim(), 16)) {
						finger_table[i][2] = Long.parseLong(livenodekey.get(k)
								.trim(), 16);
					}
				}
			}

			for (long[] row : finger_table) {
				func.printRow(row);
			}

			// Allocating files to node by calling alloc func
			func.file_table = func.file_allocation();

			for (int i = 0; i < func.file_table.size(); i++) {

				if (i % 3 == 0 && i != 0) {
					System.out.println();
				}

				System.out.print(func.file_table.get(i));
				System.out.print("\t");
			}

			System.out.println("\n");

			inital_filetable_size = func.file_table.size();

			/*
			 * System.out.println("func.file_table.size()" +
			 * func.file_table.size());
			 */

			try {
				// Send UPFIN
				mess_send.upfin();

				// Send GETKEY
				mess_send.getky();

				// Send ADD

				// Read each file key change if file_table is changed

				for (int i = 0; i < func.file_table.size(); i = i + 3) {

					String file_key = func.file_table.get(i).trim();
					String file_name = func.file_table.get(i + 2).trim();
					long file_key_dec = Long.parseLong(file_key.trim(), 16);

					String pred_key = func.predecessor(tra.hashkey);
					long pred_key_dec = Long.parseLong(pred_key, 16);

					String ipaddress = InetAddress.getLocalHost()
							.getHostAddress();

					String port = tra.self_port;

					if (pred_key_dec > tra.hashkey_decimal) {

						if (file_key_dec > tra.hashkey_decimal
								&& file_key_dec <= pred_key_dec) {

							// send
							// func.add_frwd_other(file_key, file_name,
							// ipaddress,port.trim());

							file_key_dec = Long.parseLong(file_key.trim(), 16);

							// Compare file key to entry of fingertable and send

							// Iterate thru fingertable and check key with own
							// node
							// or succ
							for (int j = 0; j < 24; j++) {

								// if file key same as entry then send to succ
								// and
								// if succ
								// is not own node
								if ((file_key_dec == tra.finger_table[j][0])
										&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

									long succ = tra.finger_table[j][2];
									String succ_hex = Long.toHexString(succ);
									// System.out.println(file_key_dec +
									// " Added");

									// Check if succ is own node
									if (tra.hashkey_decimal == succ) {
										tra.ownfilelist.add(file_key);
									}

									// Send the key to succ
									else {

										// System.out.println(file_key_dec
										// + " Sent");
										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										int length_add_send = 13
												+ tra.self_port.length()
												+ ipaddress.length()
												+ file_key.length()
												+ file_name.length();

										StringBuilder packet_add = new StringBuilder();

										packet_add
												.append("00" + length_add_send)
												.append(" ").append("ADD")
												.append(" ").append(ipaddress)
												.append(" ")
												.append(tra.self_port)
												.append(" ").append(file_key)
												.append(" ").append(file_name);
										String message_add = packet_add
												.toString().trim();

										Socket add_send = new Socket();

										// Opening socket and sending getkey
										InetSocketAddress add_frwd_inet = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										add_send.connect(add_frwd_inet);

										PrintStream add_out = new PrintStream(
												add_send.getOutputStream());

										add_out.println(message_add);

										System.out.println("ADD message sent :"
												+ message_add);

										add_send.close();

										Socket add_socket = receive_socket
												.accept();

										BufferedReader add_in = new BufferedReader(
												new InputStreamReader(
														add_socket
																.getInputStream()));

										String input = add_in.readLine();

										// Priniting UPFINOK
										System.out.println(input + "\n");

										add_socket.close();

									}

								}

							}

							// check filekey with interval
							for (int k = 0; k < 24; k++) {

								// Enter if succ is not own node
								if (tra.finger_table[k][2] != tra.hashkey_decimal) {

									// Check if file key is withing range of
									// start
									// and stop
									// || check if key belongs to overflow
									// condition

									if ((file_key_dec > tra.finger_table[k][0] && file_key_dec < tra.finger_table[k][1])
											|| ((file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
											|| ((file_key_dec < tra.finger_table[k][0])
													&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec < tra.finger_table[k][1]))) {

										// System.out.println(file_key_dec
										// + " Sent");
										long succ = tra.finger_table[k][2];
										String succ_hex = Long
												.toHexString(succ);

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										int length_add = 13
												+ tra.self_port.length()
												+ ipaddress.length()
												+ file_key.length()
												+ file_name.length();

										StringBuilder packet_add = new StringBuilder();

										packet_add.append("00" + length_add)
												.append(" ").append("ADD")
												.append(" ").append(ipaddress)
												.append(" ")
												.append(tra.self_port)
												.append(" ").append(file_key)
												.append(" ").append(file_name);
										String message_add = packet_add
												.toString().trim();

										Socket add_socket1 = new Socket();

										// Opening socket and sending getkey
										InetSocketAddress add_frwd_inet = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										add_socket1.connect(add_frwd_inet);

										PrintStream add_out = new PrintStream(
												add_socket1.getOutputStream());

										add_out.println(message_add);

										System.out.println("ADD message sent :"
												+ message_add);

										add_socket1.close();

										Socket add_socket_rcv = receive_socket
												.accept();

										BufferedReader add_in = new BufferedReader(
												new InputStreamReader(
														add_socket_rcv
																.getInputStream()));

										String input = add_in.readLine();

										// Priniting ADDOK
										System.out.println(input + "\n");

										add_socket_rcv.close();

									}

								}
							}

						}

						else {
							/*
							 * func.file_table.add(add_file_key);
							 * func.file_table.add("from add");
							 * func.file_table.add(split[5].trim());
							 */

						}
					}

					else {
						if (file_key_dec > pred_key_dec
								&& file_key_dec <= tra.hashkey_decimal) {

							/*
							 * func.file_table.add(add_file_key);
							 * func.file_table.add("from add");
							 * func.file_table.add(split[5].trim());
							 */

						}

						else {

							// Send
							// func.add_frwd_other(file_key, file_name,
							// ipaddress,port.trim());

							long file_key_dec1 = Long.parseLong(
									file_key.trim(), 16);

							// Compare file key to entry of fingertable and send

							// Iterate thru fingertable and check key with own
							// node
							// or succ
							for (int j = 0; j < 24; j++) {

								// if file key same as entry then send to succ
								// and
								// if succ
								// is not own node
								if ((file_key_dec1 == tra.finger_table[j][0])
										&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

									long succ = tra.finger_table[j][2];
									String succ_hex = Long.toHexString(succ);

									// Check if succ is own node
									if (tra.hashkey_decimal == succ) {
										tra.ownfilelist.add(file_key);
									}

									// Send the key to succ
									else {

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										int length_add = 13
												+ tra.self_port.length()
												+ ipaddress.length()
												+ file_key.length()
												+ file_name.length();

										StringBuilder packet_add = new StringBuilder();

										packet_add.append("00" + length_add)
												.append(" ").append("ADD")
												.append(" ").append(ipaddress)
												.append(" ")
												.append(tra.self_port)
												.append(" ").append(file_key)
												.append(" ").append(file_name);
										String message_add = packet_add
												.toString().trim();

										Socket add_send1 = new Socket();

										// Opening socket and sending getkey
										InetSocketAddress add_frwd_inet = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										add_send1.connect(add_frwd_inet);

										PrintStream add_out = new PrintStream(
												add_send1.getOutputStream());

										add_out.println(message_add);

										System.out.println("ADD message sent :"
												+ message_add);

										add_send1.close();

										Socket add_socket1 = receive_socket
												.accept();

										BufferedReader add_in = new BufferedReader(
												new InputStreamReader(
														add_socket1
																.getInputStream()));

										String input = add_in.readLine();

										// Priniting UPFINOK
										System.out.println(input + "\n");

										add_socket1.close();

									}

								}

							}

							// check filekey with interval
							for (int k = 0; k < 24; k++) {

								// Enter if succ is not own node
								if (tra.finger_table[k][2] != tra.hashkey_decimal) {

									// Check if file key is withing range of
									// start
									// and stop
									// || check if key belongs to overflow
									// condition

									if ((file_key_dec1 > tra.finger_table[k][0] && file_key_dec1 < tra.finger_table[k][1])
											|| ((file_key_dec1 > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
											|| ((file_key_dec1 < tra.finger_table[k][0])
													&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec1 < tra.finger_table[k][1]))) {

										long succ = tra.finger_table[k][2];
										String succ_hex = Long
												.toHexString(succ);

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										int length_add = 13
												+ tra.self_port.length()
												+ ipaddress.length()
												+ file_key.length()
												+ file_name.length();

										StringBuilder packet_add = new StringBuilder();

										packet_add.append("00" + length_add)
												.append(" ").append("ADD")
												.append(" ").append(ipaddress)
												.append(" ")
												.append(tra.self_port)
												.append(" ").append(file_key)
												.append(" ").append(file_name);
										String message_add = packet_add
												.toString().trim();

										Socket add_socket2 = new Socket();

										// Opening socket and sending getkey
										InetSocketAddress add_frwd_inet = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										add_socket2.connect(add_frwd_inet);

										PrintStream add_out = new PrintStream(
												add_socket2.getOutputStream());

										add_out.println(message_add);

										System.out.println("ADD message sent :"
												+ message_add);

										add_socket2.close();

										Socket add_socket_rcv1 = receive_socket
												.accept();

										BufferedReader add_in = new BufferedReader(
												new InputStreamReader(
														add_socket_rcv1
																.getInputStream()));

										String input = add_in.readLine();

										// Priniting UPFINOK
										System.out.println(input + "\n");

										add_socket_rcv1.close();

									}

								}
							}

						}
					}
				}
			}

			catch (Exception e) {

				// Send Upfin Update
				mess_send.upfin_leave();

				// Delete own files
				func.ownfiles_delete_exit();

				// Send Give key
				mess_send.give_key();

				// Unregister from bootstrap
				mess_send.unreg();
			}
		}

		(new Thread(new thread())).start();

		while (true) {

			System.out.println("I am listening \n");
			Socket suckit = receive_socket.accept();

			PrintStream rcv_out = new PrintStream(suckit.getOutputStream());
			BufferedReader rcv_in = new BufferedReader(new InputStreamReader(
					suckit.getInputStream()));

			String input = rcv_in.readLine();
			String[] split = input.toString().split(" ");

			System.out.println(input);
			switch (split[1].trim()) {

			case "SEROK":
				messages_rcvd++;
				long time_stop = System.currentTimeMillis();
				long delay = time_stop - mess_send.time_start;
				System.out.println("Delay for the Query is: " + delay);
				System.out.println("No of hops taken: " + split[3]);

				func.results(input, delay, split[3]);

				break;

			case "UPFIN":

				mess_receive.upfin_rcv(input, finger_table);

				long type = Long.parseLong(split[2].trim());
				// Sending UPFIN OK

				StringBuilder packet_upfin = new StringBuilder();

				packet_upfin.append("0015").append(" ").append("UPFIN")
						.append(" ").append("0");

				String message_upfin = packet_upfin.toString().trim();

				rcv_out.println(message_upfin);

				System.out.println("Message sent: " + message_upfin + "\n");

				break;

			case "GETKY":

				StringBuilder getokpacket = mess_receive.getky_rcv(input);
				// Send the Getok mess

				String message_getky = getokpacket.toString().trim();

				rcv_out.println(message_getky);

				System.out.println("Message sent :" + message_getky + "\n");

				break;

			case "GIVEKY":

				mess_receive.givky_rcv(input);

				// send ok back

				String message_gvkyok = "0015 GIVEKYOK 0";

				rcv_out.println(message_gvkyok);

				System.out.println("Message sent :" + message_gvkyok + "\n");

				break;

			case "ADD":

				String add_file_key = split[4];
				long add_file_key_dec = Long.parseLong(add_file_key.trim(), 16);

				String pred_key = func.predecessor(tra.hashkey);
				long pred_key_dec = Long.parseLong(pred_key, 16);

				String file_name = split[5].trim();
				String ipaddress = split[2].trim();
				String port = split[3].trim();
				long file_key_dec = Long.parseLong(add_file_key.trim(), 16);

				if (pred_key_dec > tra.hashkey_decimal) {

					if (add_file_key_dec > tra.hashkey_decimal
							&& add_file_key_dec <= pred_key_dec) {

						// forward add
						// Compare file key to entry of fingertable and send

						// Iterate thru fingertable and check key with own node
						// or succ
						for (int j = 0; j < 24; j++) {

							// if file key same as entry then send to succ and
							// if succ
							// is not own node
							if ((file_key_dec == tra.finger_table[j][0])
									&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

								long succ = tra.finger_table[j][2];
								String succ_hex = Long.toHexString(succ);

								// Check if succ is own node
								if (tra.hashkey_decimal == succ) {
									func.file_table.add(add_file_key);
									func.file_table.add(split[2].trim() + " "
											+ split[3].trim());
									func.file_table.add(split[5].trim());
								}

								// Send the key to succ
								else {

									int index_succ = tra.otherkey
											.indexOf(succ_hex);

									// choot code
									if (index_succ == -1) {
										StringBuilder packet_choot = new StringBuilder();
										packet_choot.append(0).append(succ_hex);
										succ_hex = packet_choot.toString()
												.trim();
										index_succ = tra.otherkey
												.indexOf(succ_hex);

									}

									String succ_ip = tra.otherkey
											.get(index_succ + 1);
									String succ_port = tra.otherkey
											.get(index_succ + 2);

									int length_add = 13
											+ tra.self_port.length()
											+ ipaddress.length()
											+ add_file_key.length()
											+ file_name.length();

									StringBuilder packet_add = new StringBuilder();

									packet_add.append("00" + length_add)
											.append(" ").append("ADD")
											.append(" ").append(ipaddress)
											.append(" ").append(port.trim())
											.append(" ").append(add_file_key)
											.append(" ").append(file_name);
									String add_message = packet_add.toString()
											.trim();

									// Opening socket and sending getkey
									InetSocketAddress add_frwd_inet = new InetSocketAddress(
											succ_ip, Integer.parseInt(succ_port
													.trim()));
									Socket add_frwd = new Socket();
									add_frwd.connect(add_frwd_inet);

									rcv_out = new PrintStream(
											add_frwd.getOutputStream());
									rcv_in = new BufferedReader(
											new InputStreamReader(
													add_frwd.getInputStream()));

									rcv_out.println(add_message);

									System.out.println("ADD message sent: "
											+ add_message);

									add_frwd.close();

								}

							}

						}

						// check filekey with interval
						for (int k = 0; k < 24; k++) {

							// Enter if succ is not own node
							if (tra.finger_table[k][2] != tra.hashkey_decimal) {

								// Check if file key is withing range of start
								// and stop
								// || check if key belongs to overflow condition

								if ((file_key_dec > tra.finger_table[k][0] && file_key_dec < tra.finger_table[k][1])
										|| ((file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
										|| ((file_key_dec < tra.finger_table[k][0])
												&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec < tra.finger_table[k][1]))) {

									long succ = tra.finger_table[k][2];
									String succ_hex = Long.toHexString(succ);

									int index_succ = tra.otherkey
											.indexOf(succ_hex);

									// choot code
									if (index_succ == -1) {
										StringBuilder packet_choot = new StringBuilder();
										packet_choot.append(0).append(succ_hex);
										succ_hex = packet_choot.toString()
												.trim();
										index_succ = tra.otherkey
												.indexOf(succ_hex);

									}

									String succ_ip = tra.otherkey
											.get(index_succ + 1);
									String succ_port = tra.otherkey
											.get(index_succ + 2);

									int length_add1 = 13
											+ tra.self_port.length()
											+ ipaddress.length()
											+ add_file_key.length()
											+ file_name.length();

									StringBuilder packet_add1 = new StringBuilder();

									packet_add1.append("00" + length_add1)
											.append(" ").append("ADD")
											.append(" ").append(ipaddress)
											.append(" ").append(port.trim())
											.append(" ").append(add_file_key)
											.append(" ").append(file_name);
									String message_add1 = packet_add1
											.toString().trim();

									// Opening socket and sending getkey
									InetSocketAddress add_frwd = new InetSocketAddress(
											succ_ip.trim(),
											Integer.parseInt(succ_port.trim()));
									Socket add_frwd1 = new Socket();
									add_frwd1.connect(add_frwd);

									rcv_out = new PrintStream(
											add_frwd1.getOutputStream());
									rcv_in = new BufferedReader(
											new InputStreamReader(
													add_frwd1.getInputStream()));

									rcv_out.println(message_add1);
									System.out.println("ADD message sent: "
											+ message_add1);

									add_frwd1.close();

								}

							}
						}

					}

					else {

						func.file_table.add(add_file_key);
						func.file_table.add(split[2].trim() + " "
								+ split[3].trim());
						func.file_table.add(split[5].trim());

						// func.sendok("ADDOK", split[2].trim(),
						// split[3].trim());

						InetAddress addok_ip = InetAddress.getByName(ipaddress);
						int addok_port = Integer.parseInt(port);
						// Opening socket and sending getkey
						InetSocketAddress add_frwd = new InetSocketAddress(
								addok_ip, addok_port);
						Socket add_ok_socket = new Socket();
						add_ok_socket.connect(add_frwd);

						PrintStream add_ok_send = new PrintStream(
								add_ok_socket.getOutputStream());

						StringBuilder packet_addok = new StringBuilder();

						packet_addok.append("0013").append(" ").append("ADDOK")
								.append(" ").append("0").append(" from ")
								.append(tra.self_port);

						String message_addok = packet_addok.toString().trim();

						add_ok_send.println(message_addok);

						System.out.println("Message sent: " + message_addok);

						add_ok_socket.close();

					}
				}

				else {
					if (add_file_key_dec > pred_key_dec
							&& add_file_key_dec <= tra.hashkey_decimal) {

						func.file_table.add(add_file_key);
						func.file_table.add(split[2].trim() + " "
								+ split[3].trim());
						func.file_table.add(split[5].trim());

						InetAddress addok_ip = InetAddress.getByName(ipaddress);
						int addok_port = Integer.parseInt(port);
						// Opening socket and sending getkey
						InetSocketAddress add_frwd = new InetSocketAddress(
								addok_ip, addok_port);
						Socket add_ok_socket1 = new Socket();
						add_ok_socket1.connect(add_frwd);

						PrintStream add_ok_send1 = new PrintStream(
								add_ok_socket1.getOutputStream());

						StringBuilder packet_addok = new StringBuilder();

						packet_addok.append("0013").append(" ").append("ADDOK")
								.append(" ").append("0").append(" from ")
								.append(tra.self_port);

						String message_addok = packet_addok.toString().trim();

						add_ok_send1.println(message_addok);

						System.out.println("Message sent: " + message_addok);

						add_ok_socket1.close();

					}

					else {

						// forward add
						// Compare file key to entry of fingertable and send

						// Iterate thru fingertable and check key with own node
						// or succ
						for (int j = 0; j < 24; j++) {

							// if file key same as entry then send to succ and
							// if succ
							// is not own node
							if ((file_key_dec == tra.finger_table[j][0])
									&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

								long succ = tra.finger_table[j][2];
								String succ_hex = Long.toHexString(succ);

								// Check if succ is own node
								if (tra.hashkey_decimal == succ) {
									tra.ownfilelist.add(add_file_key);
								}

								// Send the key to succ
								else {
									int index_succ = tra.otherkey
											.indexOf(succ_hex);

									// choot code
									if (index_succ == -1) {
										StringBuilder packet_choot = new StringBuilder();
										packet_choot.append(0).append(succ_hex);
										succ_hex = packet_choot.toString()
												.trim();
										index_succ = tra.otherkey
												.indexOf(succ_hex);

									}

									String succ_ip = tra.otherkey
											.get(index_succ + 1);
									String succ_port = tra.otherkey
											.get(index_succ + 2);

									int length_add = 13
											+ tra.self_port.length()
											+ ipaddress.length()
											+ add_file_key.length()
											+ file_name.length();

									StringBuilder packet_add = new StringBuilder();

									packet_add.append("00" + length_add)
											.append(" ").append("ADD")
											.append(" ").append(ipaddress)
											.append(" ").append(port.trim())
											.append(" ").append(add_file_key)
											.append(" ").append(file_name);
									String add_message = packet_add.toString()
											.trim();

									// Opening socket and sending getkey
									InetSocketAddress add_frwd_inet = new InetSocketAddress(
											succ_ip, Integer.parseInt(succ_port
													.trim()));
									Socket add_frwd2 = new Socket();
									add_frwd2.connect(add_frwd_inet);

									rcv_out = new PrintStream(
											add_frwd2.getOutputStream());
									rcv_in = new BufferedReader(
											new InputStreamReader(
													add_frwd2.getInputStream()));

									rcv_out.println(add_message);

									System.out.println("ADD message sent: "
											+ add_message);

									add_frwd2.close();

								}

							}

						}

						// check filekey with interval
						for (int k = 0; k < 24; k++) {

							// Enter if succ is not own node
							if (tra.finger_table[k][2] != tra.hashkey_decimal) {

								// Check if file key is withing range of start
								// and stop
								// || check if key belongs to overflow condition

								if ((file_key_dec > tra.finger_table[k][0] && file_key_dec < tra.finger_table[k][1])
										|| ((file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
										|| ((file_key_dec < tra.finger_table[k][0])
												&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (file_key_dec < tra.finger_table[k][1]))) {

									long succ = tra.finger_table[k][2];
									String succ_hex = Long.toHexString(succ);

									int index_succ = tra.otherkey
											.indexOf(succ_hex);

									// choot code
									if (index_succ == -1) {
										StringBuilder packet_choot = new StringBuilder();
										packet_choot.append(0).append(succ_hex);
										succ_hex = packet_choot.toString()
												.trim();
										index_succ = tra.otherkey
												.indexOf(succ_hex);

									}

									String succ_ip = tra.otherkey
											.get(index_succ + 1);
									String succ_port = tra.otherkey
											.get(index_succ + 2);

									int length_add1 = 13
											+ tra.self_port.length()
											+ ipaddress.length()
											+ add_file_key.length()
											+ file_name.length();

									StringBuilder packet_add1 = new StringBuilder();

									packet_add1.append("00" + length_add1)
											.append(" ").append("ADD")
											.append(" ").append(ipaddress)
											.append(" ").append(port.trim())
											.append(" ").append(add_file_key)
											.append(" ").append(file_name);
									String message_add1 = packet_add1
											.toString().trim();

									// Opening socket and sending getkey
									InetSocketAddress add_frwd = new InetSocketAddress(
											succ_ip.trim(),
											Integer.parseInt(succ_port.trim()));
									Socket add_frwd3 = new Socket();
									add_frwd3.connect(add_frwd);

									rcv_out = new PrintStream(
											add_frwd3.getOutputStream());
									rcv_in = new BufferedReader(
											new InputStreamReader(
													add_frwd3.getInputStream()));

									rcv_out.println(message_add1);

									System.out.println("ADD message sent: "
											+ message_add1);

									add_frwd3.close();

								}

							}
						}

					}
				}

				break;

			case "SER":

				messages_rcvd++;
				String ser_file_key = split[4];
				long ser_file_key_dec = Long.parseLong(ser_file_key.trim(), 16);

				String ser_pred_key = func.predecessor(tra.hashkey);
				long ser_pred_key_dec = Long.parseLong(ser_pred_key, 16);

				String hops = split[5].trim();
				hops_updated = Integer.parseInt(hops.trim());
				String query_node_ipaddress = split[2].trim();
				String query_node_port = split[3].trim();

				// Updating hop count
				hops_updated++;

				System.out.println("hop" + hops_updated);

				if (hops_updated < 7) {

					// Checking if the forwarding node owns overhead keys
					if (ser_pred_key_dec > tra.hashkey_decimal) {

						// When the query key doesnt belong to forwarding node
						if (ser_file_key_dec > tra.hashkey_decimal
								&& ser_file_key_dec <= ser_pred_key_dec) {

							// Iterate thru fingertable and check key with own
							// node
							// or succ
							for (int j = 0; j < 24; j++) {

								// if file key same as entry then send to succ
								// and
								// if succ
								// is not own node
								if ((ser_file_key_dec == tra.finger_table[j][0])
										&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

									// Getting successor
									long succ = tra.finger_table[j][2];
									String succ_hex = Long.toHexString(succ);

									// Check if succ is own node
									if (tra.hashkey_decimal == succ) {

										// Send SEROK to querying node
										StringBuilder serok_packet1 = new StringBuilder();
										int count = 0;

										// Iterating thru file table
										for (int i = 0; i < func.file_table
												.size(); i = i + 3) {

											long file_entry = Long
													.parseLong(func.file_table
															.get(i));

											// When the key is found
											if (ser_file_key_dec == file_entry) {

												String ipport1 = func.file_table
														.get(i + 1);
												serok_packet1
														.append(" ")
														.append(ipport1)
														.append(" ")
														.append(func.file_table
																.get(i + 2));

												count++;
											}
										}

										StringBuilder serokpacket1 = new StringBuilder();
										int seroklength1 = 16 + serok_packet1
												.length();

										if (count == 0) {
											serokpacket1.append("0015 SEROK 0")
													.append(" ")
													.append(hops_updated);
										}

										else {
											messages_answd++;
											serokpacket1
													.append("00" + seroklength1)
													.append(" ")
													.append("SEROK")
													.append(" ").append(count)
													.append(" ")
													.append(hops_updated)
													.append(serok_packet1);
										}

										InetAddress serok_ip = InetAddress
												.getByName(query_node_ipaddress);
										int serok_port = Integer
												.parseInt(query_node_port);
										// Opening socket and sending getkey
										InetSocketAddress serok_inet1 = new InetSocketAddress(
												serok_ip, serok_port);
										Socket ser_ok_socket1 = new Socket();
										ser_ok_socket1.connect(serok_inet1);

										PrintStream ser_ok_send1 = new PrintStream(
												ser_ok_socket1
														.getOutputStream());

										String message_serok1 = serokpacket1
												.toString().trim();

										ser_ok_send1.println(message_serok1);

										System.out.println("Message sent: "
												+ message_serok1);

										ser_ok_socket1.close();

									}

									// Forward the query key to succussor
									else {

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										// Extracting the Succussor's IP and
										// Port
										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										int ser_length = 13
												+ query_node_port.length()
												+ query_node_ipaddress.length()
												+ ser_file_key.length();

										StringBuilder ser_packet = new StringBuilder();

										messages_frwd++;
										ser_packet.append("00" + ser_length)
												.append(" ").append("SER")
												.append(" ")
												.append(query_node_ipaddress)
												.append(" ")
												.append(query_node_port.trim())
												.append(" ")
												.append(ser_file_key)
												.append(" ")
												.append(hops_updated);
										String ser_message = ser_packet
												.toString().trim();

										// Opening socket and sending getkey
										InetSocketAddress ser_frwd_inet = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										Socket ser_frwd = new Socket();
										ser_frwd.connect(ser_frwd_inet);

										rcv_out = new PrintStream(
												ser_frwd.getOutputStream());
										rcv_in = new BufferedReader(
												new InputStreamReader(ser_frwd
														.getInputStream()));

										rcv_out.println(ser_message);

										System.out.println(ser_message);

										ser_frwd.close();

									}

								}

							}

							// check filekey with interval
							for (int k = 0; k < 24; k++) {

								// Enter if succ is not own node
								if (tra.finger_table[k][2] != tra.hashkey_decimal) {

									// Check if file key is withing range of
									// start
									// and stop
									// || check if key belongs to overflow
									// condition

									if ((ser_file_key_dec > tra.finger_table[k][0] && ser_file_key_dec < tra.finger_table[k][1])
											|| ((ser_file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
											|| ((ser_file_key_dec < tra.finger_table[k][0])
													&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (ser_file_key_dec < tra.finger_table[k][1]))) {

										// Extracting the succussor
										long succ = tra.finger_table[k][2];
										String succ_hex = Long
												.toHexString(succ);
										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										// Extracting Successors Ip and Port
										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										// Building Packet
										int ser_length1 = 13
												+ query_node_port.length()
												+ query_node_ipaddress.length()
												+ ser_file_key.length();

										StringBuilder ser_packet1 = new StringBuilder();

										messages_frwd++;
										ser_packet1.append("00" + ser_length1)
												.append(" ").append("SER")
												.append(" ")
												.append(query_node_ipaddress)
												.append(" ")
												.append(query_node_port.trim())
												.append(" ")
												.append(ser_file_key)
												.append(" ")
												.append(hops_updated);
										String ser_message1 = ser_packet1
												.toString().trim();

										// Opening socket and sending getkey
										InetSocketAddress ser_frwd_inet1 = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										Socket ser_frwd1 = new Socket();
										ser_frwd1.connect(ser_frwd_inet1);

										rcv_out = new PrintStream(
												ser_frwd1.getOutputStream());
										rcv_in = new BufferedReader(
												new InputStreamReader(ser_frwd1
														.getInputStream()));

										rcv_out.println(ser_message1);

										System.out.println(ser_message1);

										ser_frwd1.close();

									}

								}
							}

						}

						// Query file belongs to node hence send SEROK
						else {

							StringBuilder serok_packet = new StringBuilder();
							int count = 0;

							// Iterating thru file table
							for (int i = 0; i < func.file_table.size(); i = i + 3) {

								long file_entry = Long.parseLong(
										func.file_table.get(i), 16);

								// When the key is found
								if (ser_file_key_dec == file_entry) {

									String ipport = func.file_table.get(i + 1);
									serok_packet.append(" ").append(ipport)
											.append(" ")
											.append(func.file_table.get(i + 2));

									count++;
								}
							}

							StringBuilder serokpacket = new StringBuilder();
							int seroklength = 16 + serok_packet.length();

							if (count == 0) {
								serokpacket.append("0015 SEROK 0").append(" ")
										.append(hops_updated);
							}

							else {
								messages_answd++;
								serokpacket.append("00" + seroklength)
										.append(" ").append("SEROK")
										.append(" ").append(count).append(" ")
										.append(hops_updated).append(" ")
										.append(serok_packet);
							}

							InetAddress serok_ip = InetAddress
									.getByName(query_node_ipaddress);
							int serok_port = Integer.parseInt(query_node_port);
							// Opening socket and sending getkey
							InetSocketAddress serok_inet = new InetSocketAddress(
									serok_ip, serok_port);
							Socket ser_ok_socket = new Socket();
							ser_ok_socket.connect(serok_inet);

							PrintStream ser_ok_send = new PrintStream(
									ser_ok_socket.getOutputStream());

							String message_serok = serokpacket.toString()
									.trim();

							ser_ok_send.println(message_serok);

							System.out
									.println("Message sent: " + message_serok);

							ser_ok_socket.close();

						}
					}

					else {
						if (ser_file_key_dec > ser_pred_key_dec
								&& ser_file_key_dec <= tra.hashkey_decimal) {

							StringBuilder serok_packet = new StringBuilder();
							int count = 0;

							// Iterating thru file table
							for (int i = 0; i < func.file_table.size(); i = i + 3) {

								long file_entry = Long.parseLong(
										func.file_table.get(i), 16);

								// When the key is found
								if (ser_file_key_dec == file_entry) {

									String ipport = func.file_table.get(i + 1);
									serok_packet.append(" ").append(ipport)
											.append(" ")
											.append(func.file_table.get(i + 2));

									count++;
								}
							}

							StringBuilder serokpacket = new StringBuilder();
							int seroklength = 16 + serok_packet.length();

							if (count == 0) {
								serokpacket.append("0015 SEROK 0").append(" ")
										.append(hops_updated);
							}

							else {
								messages_answd++;
								serokpacket.append("00" + seroklength)
										.append(" ").append("SEROK")
										.append(" ").append(count).append(" ")
										.append(hops_updated)
										.append(serok_packet);
							}

							InetAddress serok_ip = InetAddress
									.getByName(query_node_ipaddress);
							int serok_port = Integer.parseInt(query_node_port);
							// Opening socket and sending getkey
							InetSocketAddress serok_inet1 = new InetSocketAddress(
									serok_ip, serok_port);
							Socket ser_ok_socket1 = new Socket();
							ser_ok_socket1.connect(serok_inet1);

							PrintStream ser_ok_send1 = new PrintStream(
									ser_ok_socket1.getOutputStream());

							String message_serok1 = serokpacket.toString()
									.trim();

							ser_ok_send1.println(message_serok1);

							System.out.println("Message sent: "
									+ message_serok1);

							ser_ok_socket1.close();

						}

						else {

							// Iterate thru fingertable and check key with own
							// node
							// or succ
							for (int j = 0; j < 24; j++) {

								// if file key same as entry then send to succ
								// and
								// if succ
								// is not own node
								if ((ser_file_key_dec == tra.finger_table[j][0])
										&& (tra.finger_table[j][2] != tra.hashkey_decimal)) {

									// Extracting the successor
									long succ = tra.finger_table[j][2];
									String succ_hex = Long.toHexString(succ);

									// Check if succ is own node
									if (tra.hashkey_decimal == succ) {

										// Send SEROK to querying node
										StringBuilder serok_packet2 = new StringBuilder();
										int count = 0;

										// Iterating thru file table
										for (int i = 0; i < func.file_table
												.size(); i = i + 3) {

											long file_entry = Long
													.parseLong(func.file_table
															.get(i));

											// When the key is found
											if (ser_file_key_dec == file_entry) {

												String ipport1 = func.file_table
														.get(i + 1);
												serok_packet2
														.append(" ")
														.append(ipport1)
														.append(" ")
														.append(func.file_table
																.get(i + 2));

												count++;
											}
										}

										StringBuilder serokpacket2 = new StringBuilder();
										int seroklength1 = 16 + serok_packet2
												.length();

										if (count == 0) {
											serokpacket2.append("0015 SEROK 0")
													.append(" ")
													.append(hops_updated);
										}

										else {
											messages_answd++;
											serokpacket2
													.append("00" + seroklength1)
													.append(" ")
													.append("SEROK")
													.append(" ").append(count)
													.append(" ")
													.append(hops_updated)
													.append(serok_packet2);
										}

										InetAddress serok_ip = InetAddress
												.getByName(query_node_ipaddress);
										int serok_port = Integer
												.parseInt(query_node_port);
										// Opening socket and sending getkey
										InetSocketAddress serok_inet2 = new InetSocketAddress(
												serok_ip, serok_port);
										Socket ser_ok_socket2 = new Socket();
										ser_ok_socket2.connect(serok_inet2);

										PrintStream ser_ok_send2 = new PrintStream(
												ser_ok_socket2
														.getOutputStream());

										String message_serok2 = serokpacket2
												.toString().trim();

										ser_ok_send2.println(message_serok2);

										System.out.println("Message sent: "
												+ message_serok2);

										ser_ok_socket2.close();

									}

									// Send the query to succ
									else {

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										// Extracting Successors IP and Port
										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										// Building Packet
										int ser_length2 = 13
												+ query_node_port.length()
												+ query_node_ipaddress.length()
												+ ser_file_key.length();

										StringBuilder ser_packet2 = new StringBuilder();

										messages_frwd++;
										ser_packet2.append("00" + ser_length2)
												.append(" ").append("SER")
												.append(" ")
												.append(query_node_ipaddress)
												.append(" ")
												.append(query_node_port.trim())
												.append(" ")
												.append(ser_file_key)
												.append(" ")
												.append(hops_updated);
										String ser_message2 = ser_packet2
												.toString().trim();

										// Opening socket and sending getkey
										InetSocketAddress ser_frwd_inet2 = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										Socket ser_frwd2 = new Socket();
										ser_frwd2.connect(ser_frwd_inet2);

										rcv_out = new PrintStream(
												ser_frwd2.getOutputStream());
										rcv_in = new BufferedReader(
												new InputStreamReader(ser_frwd2
														.getInputStream()));

										rcv_out.println(ser_message2);

										System.out.println(ser_message2);

										ser_frwd2.close();

									}

								}

							}

							// check filekey with interval
							for (int k = 0; k < 24; k++) {

								// Enter if succ is not own node
								if (tra.finger_table[k][2] != tra.hashkey_decimal) {

									// Check if file key is withing range of
									// start
									// and stop
									// || check if key belongs to overflow
									// condition

									if ((ser_file_key_dec > tra.finger_table[k][0] && ser_file_key_dec < tra.finger_table[k][1])
											|| ((ser_file_key_dec > tra.finger_table[k][0]) && (tra.finger_table[k][0] > tra.finger_table[k][1]))
											|| ((ser_file_key_dec < tra.finger_table[k][0])
													&& (tra.finger_table[k][0] > tra.finger_table[k][1]) && (ser_file_key_dec < tra.finger_table[k][1]))) {

										long succ = tra.finger_table[k][2];
										String succ_hex = Long
												.toHexString(succ);

										int index_succ = tra.otherkey
												.indexOf(succ_hex);

										// choot code
										if (index_succ == -1) {
											StringBuilder packet_choot = new StringBuilder();
											packet_choot.append(0).append(
													succ_hex);
											succ_hex = packet_choot.toString()
													.trim();
											index_succ = tra.otherkey
													.indexOf(succ_hex);

										}

										// Extracting successors Ip and Port
										String succ_ip = tra.otherkey
												.get(index_succ + 1);
										String succ_port = tra.otherkey
												.get(index_succ + 2);

										// Building Packet
										int ser_length3 = 13
												+ query_node_port.length()
												+ query_node_ipaddress.length()
												+ ser_file_key.length();

										StringBuilder ser_packet3 = new StringBuilder();

										messages_frwd++;
										ser_packet3.append("00" + ser_length3)
												.append(" ").append("SER")
												.append(" ")
												.append(query_node_ipaddress)
												.append(" ")
												.append(query_node_port.trim())
												.append(" ")
												.append(ser_file_key)
												.append(" ")
												.append(hops_updated);
										String ser_message3 = ser_packet3
												.toString().trim();

										// Opening socket and sending getkey
										InetSocketAddress ser_frwd_inet3 = new InetSocketAddress(
												succ_ip,
												Integer.parseInt(succ_port
														.trim()));
										Socket ser_frwd3 = new Socket();
										ser_frwd3.connect(ser_frwd_inet3);

										rcv_out = new PrintStream(
												ser_frwd3.getOutputStream());
										rcv_in = new BufferedReader(
												new InputStreamReader(ser_frwd3
														.getInputStream()));

										rcv_out.println(ser_message3);

										System.out.println(ser_message3);

										ser_frwd3.close();

									}

								}
							}

						}
					}

				}

				else {

					// Send File not found

					// Send SEROK to querying node

					InetAddress sernotok_ip = InetAddress
							.getByName(query_node_ipaddress);
					int sernotok_port = Integer.parseInt(query_node_port);
					// Opening socket and sending getkey
					InetSocketAddress serok_inet1 = new InetSocketAddress(
							sernotok_ip, sernotok_port);
					Socket ser_notok_socket1 = new Socket();
					ser_notok_socket1.connect(serok_inet1);

					PrintStream ser_notok_send1 = new PrintStream(
							ser_notok_socket1.getOutputStream());
					String message_serok1 = " 0015 SEROK 0 " + hops_updated;

					ser_notok_send1.println(message_serok1);

					System.out.println("Message sent: " + message_serok1);

					ser_notok_socket1.close();

				}
			}
			suckit.close();
		}

	}
}
