import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class mess_send {

	public static PrintStream out = null;
	public static BufferedReader in = null;
	public static String ipaddress_send = null;
	public static int port_send = 0;
	public static String ipaddress = null;
	public static long time_start = 0;

	public static void upfin() throws Exception {

		StringBuilder packet = new StringBuilder();
		ipaddress = InetAddress.getLocalHost().getHostAddress();
		String hashkey = func.hashfunc(tra.self_port);

		int length = 16 + tra.self_port.length() + ipaddress.length()
				+ hashkey.length();
		packet.append("00" + length).append(" ").append("UPFIN").append(" ")
				.append("0").append(" ").append(ipaddress).append(" ")
				.append(tra.self_port).append(" ").append(hashkey);
		String message = packet.toString().trim();

		for (int i = 3; i < tra.otherkey.size(); i = i + 3) {

			ipaddress_send = tra.otherkey.get(i + 1);
			port_send = Integer.parseInt(tra.otherkey.get(i + 2).trim());

			InetSocketAddress upfin_socket = new InetSocketAddress(
					ipaddress_send, port_send);
			Socket upfinsocket = new Socket();
			upfinsocket.connect(upfin_socket);

			out = new PrintStream(upfinsocket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(
					upfinsocket.getInputStream()));

			out.println(message);
			System.out.println("UPFIN message sent :\n" + message);

			in = new BufferedReader(new InputStreamReader(
					upfinsocket.getInputStream()));

			String input = in.readLine();

			// Priniting UPFINOK
			System.out.println(input + "\n");

			upfinsocket.close();

		}
	}

	public static void upfin_leave() throws Exception {

		StringBuilder packet_leave = new StringBuilder();
		ipaddress = InetAddress.getLocalHost().getHostAddress();
		String hashkey = func.hashfunc(tra.self_port);

		int length_leave = 16 + tra.self_port.length() + ipaddress.length()
				+ hashkey.length();
		packet_leave.append("00" + length_leave).append(" ").append("UPFIN")
				.append(" ").append("1").append(" ").append(ipaddress)
				.append(" ").append(tra.self_port).append(" ").append(hashkey);
		String message_leave = packet_leave.toString().trim();

		for (int i = 3; i < tra.otherkey.size(); i = i + 3) {

			String ipaddress_send1 = tra.otherkey.get(i + 1);
			int port_send1 = Integer.parseInt(tra.otherkey.get(i + 2).trim());

			InetSocketAddress upfin_leave_inet = new InetSocketAddress(
					ipaddress_send1, port_send1);
			Socket upfin_leave_socket = new Socket();
			upfin_leave_socket.connect(upfin_leave_inet);

			out = new PrintStream(upfin_leave_socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(
					upfin_leave_socket.getInputStream()));

			out.println(message_leave);
			System.out.println("UPFIN message sent :\n" + message_leave);

			String input = in.readLine();

			System.out.println(input + "\n");

			upfin_leave_socket.close();

		}

	}

	public static void give_key() throws Exception {

		// Append all files owned by node

		StringBuilder packet_gk = new StringBuilder();

		int bla = 0;
		for (int i = 0; i < func.file_table.size(); i = i + 3) {

			packet_gk.append(func.file_table.get(i + 1)).append(" ");
			packet_gk.append(func.file_table.get(i)).append(" ");
			packet_gk.append(func.file_table.get(i + 2)).append(" ");
			bla++;
		}

		// Find the Succ of own node and its IP and Port
		String succ_key = func.successor(tra.hashkey);

		int index_succ = tra.otherkey.indexOf(succ_key);
		String succ_ip = tra.otherkey.get(index_succ + 1);
		String succ_port = tra.otherkey.get(index_succ + 2);

		// Build packet

		int length_gv = 15 + packet_gk.length();

		StringBuilder packet_giveky = new StringBuilder();
		packet_giveky.append("00" + length_gv).append(" ").append("GIVEKY")
				.append(" ").append(bla).append(" ").append(packet_gk);
		String message_givky = packet_giveky.toString().trim();

		// Send packet to succ
		// Opening socket and sending getkey
		InetSocketAddress givky_inetsoc = new InetSocketAddress(succ_ip,
				Integer.parseInt(succ_port.trim()));
		Socket givky_socket = new Socket();
		givky_socket.connect(givky_inetsoc);

		PrintStream out_givky = new PrintStream(givky_socket.getOutputStream());
		BufferedReader in_givky = new BufferedReader(new InputStreamReader(
				givky_socket.getInputStream()));

		out_givky.println(message_givky);
		System.out.println("GETKY message sent : \n" + message_givky);

		in_givky = new BufferedReader(new InputStreamReader(
				givky_socket.getInputStream()));

		String input = in_givky.readLine();

		System.out.println(input + "\n");

		givky_socket.close();

	}

	public static void getky() throws Exception {

		// Extracting index of own node key
		int index_ownkey = tra.livenodekey.indexOf(tra.hashkey);
		String succ_key = null;

		// If own node key is last in array then update first key as succ
		if (index_ownkey == tra.livenodekey.size() - 1) {

			succ_key = tra.livenodekey.get(0);
		}

		// else take next key as succ
		else {
			succ_key = tra.livenodekey.get(index_ownkey + 1);

		}

		// Extracting ip and port of succ
		int index_succkey = tra.otherkey.indexOf(succ_key);

		String succ_ip = tra.otherkey.get(index_succkey + 1);
		String succ_port = tra.otherkey.get(index_succkey + 2);

		// Bulding Getkey packet to succ
		StringBuilder packet = new StringBuilder();
		int length = 12 + tra.hashkey.length();
		packet.append("00" + length).append(" ").append("GETKY").append(" ")
				.append(tra.hashkey);
		String message = packet.toString().trim();

		// Opening socket and sending getkey
		InetSocketAddress getky_inetsoc = new InetSocketAddress(succ_ip,
				Integer.parseInt(succ_port.trim()));
		Socket getky_socket = new Socket();
		getky_socket.connect(getky_inetsoc);

		PrintStream out_getky = new PrintStream(getky_socket.getOutputStream());
		BufferedReader in_getky = new BufferedReader(new InputStreamReader(
				getky_socket.getInputStream()));

		out_getky.println(message);
		System.out.println("GETKY message sent : \n" + message);

		String input = in_getky.readLine();

		System.out.println(input + "\n");

		func.own_file_store(input);

		getky_socket.close();

	}

	public static void unreg() throws Exception {

		// Bulding Getkey packet to succ
		StringBuilder unregpacket = new StringBuilder();

		int length = 12 + tra.hashkey.length();
		unregpacket.append("00" + length).append(" ").append("UNREG")
				.append(" ").append(tra.hashkey);
		String unreg_message = unregpacket.toString().trim();

		// Opening socket and sending getkey
		InetSocketAddress unreg_inetsoc = new InetSocketAddress(
				tra.bootstrap_name, Integer.parseInt(tra.bootstrap_port.trim()));
		Socket unreg_socket = new Socket();
		unreg_socket.connect(unreg_inetsoc);

		PrintStream out_unreg = new PrintStream(unreg_socket.getOutputStream());
		BufferedReader in_unreg = new BufferedReader(new InputStreamReader(
				unreg_socket.getInputStream()));

		out_unreg.println(unreg_message);
		System.out.println("UNREG message sent : \n" + unreg_message);

		String input = in_unreg.readLine();

		System.out.println(input + "\n");

		unreg_socket.close();

	}

	public static void addkey(long[][] finger_table) throws Exception {

		for (int i = 0; i < func.file_table.size(); i = i + 3) {

			String file_key = func.file_table.get(i).trim();
			String file_name = func.file_table.get(i + 2).trim();
			long file_key_dec = Long.parseLong(file_key.trim(), 16);

			String pred_key = func.predecessor(tra.hashkey);
			long pred_key_dec = Long.parseLong(pred_key, 16);

			ipaddress = InetAddress.getLocalHost().getHostAddress();
			String port = tra.self_port;

			// When node owns overheadrange of keys
			if (pred_key_dec > tra.hashkey_decimal) {

				// if the query file is not owned by the node
				if (file_key_dec > tra.hashkey_decimal
						&& file_key_dec <= pred_key_dec) {

					// Forward the query file to other nodes
					func.add_send(file_key, file_name, ipaddress, port.trim());

				}

				// if the query file is owned by the node
				else {

					// Display file belongs to the query node
					System.out.println("The" + file_name + " with key "
							+ file_key_dec + " belongs to the own node \n");

				}
			}

			// When node owns normal range of keys
			else {

				// Check if query file belongs to the own node
				if (file_key_dec > pred_key_dec
						&& file_key_dec <= tra.hashkey_decimal) {

					// Display file belongs to the query node
					System.out.println("The" + file_name + " with key "
							+ file_key_dec + " belongs to the own node \n");

				}

				// else query file doesnt belong to own node
				else {

					// Forward the query file to other nodes
					func.add_send(file_key, file_name, ipaddress, port.trim());
				}
			}
		}

	}

	public static void serkey(long[][] finger_table) throws Exception {

		// Getting the queries

		func.query_read();

		for (int i = 0; i < func.query_table.size(); i = i + 3) {

			time_start = System.currentTimeMillis();

			String file_key = func.query_table.get(i).trim();
			String file_name = func.query_table.get(i + 2).trim();
			long file_key_dec = Long.parseLong(file_key.trim(), 16);

			String pred_key = func.predecessor(tra.hashkey);
			long pred_key_dec = Long.parseLong(pred_key, 16);

			ipaddress = InetAddress.getLocalHost().getHostAddress();
			String port = tra.self_port;

			// When node owns overheadrange of keys
			if (pred_key_dec > tra.hashkey_decimal) {

				// if the query file is not owned by the node
				if (file_key_dec > tra.hashkey_decimal
						&& file_key_dec <= pred_key_dec) {

					tra.messages_frwd++;
					// Forward the query file to other nodes
					func.ser_send(file_key, file_name, ipaddress, port.trim());

				}

				// if the query file is owned by the node
				else {

					tra.messages_answd++;
					// Display file belongs to the query node
					System.out
							.println("The Query " + file_name + " with key "
									+ file_key_dec
									+ " belongs to the Querying node \n");

				}
			}

			// When node owns normal range of keys
			else {

				// Check if query file belongs to the own node
				if (file_key_dec > pred_key_dec
						&& file_key_dec <= tra.hashkey_decimal) {

					tra.messages_answd++;
					// Display file belongs to the query node
					System.out
							.println("The Query " + file_name + " with key "
									+ file_key_dec
									+ " belongs to the Querying node \n");

				}

				// else query file doesnt belong to own node
				else {

					tra.messages_frwd++;
					// Forward the query file to other nodes
					func.ser_send(file_key, file_name, ipaddress, port.trim());
				}

			}

		}

	}

}
