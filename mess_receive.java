import java.util.Collections;

public class mess_receive {

	public static void upfin_rcv(String output, long[][] finger_table)
			throws Exception {

		String[] split = output.toString().split(" ");

		if (Integer.parseInt(split[2].trim()) == 0) {

			String new_key = split[5];

			tra.livenodekey.add(new_key);

			tra.otherkey.add(new_key);
			tra.otherkey.add(split[3]);
			tra.otherkey.add(split[4]);

			Collections.sort(tra.livenodekey);

			for (int i = 0; i < tra.livenodekey.size(); i++) {
				long inter = Long.parseLong(tra.livenodekey.get(i).trim(), 16);
				// System.out.println("num " + inter);
			}

			// Fill all succ as lowest entry
			for (int i = 0; i < 24; i++) {
				finger_table[i][2] = Long.parseLong(tra.livenodekey.get(0)
						.trim(), 16);
			}

			// update succ if key > start && succ = key
			for (int k = 0; k < tra.livenodekey.size(); k++) {
				for (int i = 0; i < 24; i++) {
					if (finger_table[i][0] > finger_table[i][2]
							&& finger_table[i][0] <= Long.parseLong(
									tra.livenodekey.get(k).trim(), 16)) {
						finger_table[i][2] = Long.parseLong(tra.livenodekey
								.get(k).trim(), 16);
					}
					// System.out.println(finger_table[i][2]);
				}

				// System.out.println("\n");
			}

			// Update succ when start = any live nodes
			for (int k = 0; k < tra.livenodekey.size(); k++) {
				for (int i = 0; i < 8; i++) {
					if (finger_table[i][0] == Long.parseLong(tra.livenodekey
							.get(k).trim(), 16)) {
						finger_table[i][2] = Long.parseLong(tra.livenodekey
								.get(k).trim(), 16);
					}
				}
			}

		}

		else {

			String leave_key = split[5].trim();
			long leave_key_dec = Long.parseLong(leave_key, 16);

			// Get the successor for the leaving key
			String successor = func.successor(leave_key);
			long successor_dec = Long.parseLong(successor, 16);

			for (int j = 0; j < 24; j++) {

				if (tra.finger_table[j][2] == leave_key_dec) {

					tra.finger_table[j][2] = successor_dec;

				}

			}

			// Remove from live node key
			for (int k = 0; k < tra.livenodekey.size(); k++) {

				if (leave_key
						.compareToIgnoreCase(tra.livenodekey.get(k).trim()) == 0) {

					tra.livenodekey.remove(k);

				}
			}

			// System.out.println(tra.otherkey.size());
			// Remove key and its entries from array list
			for (int i = 0; i < tra.otherkey.size(); i = i + 3) {

				if (leave_key.compareToIgnoreCase(tra.otherkey.get(i).trim()) == 0) {

					tra.otherkey.remove(i);
					tra.otherkey.remove(i);
					tra.otherkey.remove(i);
				}
			}

		}

		for (long[] row : finger_table) {
			func.printRow(row);
		}

	}

	public static StringBuilder getky_rcv(String output) throws Exception {

		String[] split = output.split(" ");

		String getky_node = split[2];

		// Getting Predessosr of Get key node
		String predecessor_key = func.predecessor(getky_node);
		StringBuilder packet = new StringBuilder();

		int count = 0;
		// Finding files owned btw getky and its predecessor in file_table
		for (int i = 0; i < func.file_table.size(); i = i + 3) {

			long file_key_dec = Long.parseLong(func.file_table.get(i), 16);
			long predecessor_key_dec = Long.parseLong(predecessor_key, 16);
			long get_key_node_dec = Long.parseLong(getky_node, 16);
			String reg_ip = null;
			String reg_port = null;

			// Comparing if file_key belongs to the range of getkey node to its
			// predecessor
			if (file_key_dec > predecessor_key_dec
					&& file_key_dec <= get_key_node_dec) {

				packet.append(" ").append(func.file_table.get(i + 1))
						.append(" ").append(func.file_table.get(i)).append(" ")
						.append(func.file_table.get(i + 2));
				count++;

			}

		}

		// Send the Getok mess

		StringBuilder getokpacket = new StringBuilder();
		int length = 16 + packet.length();

		if (count == 0) {
			getokpacket.append("0015 GETKYOK 0");
		}

		else {
			getokpacket.append("00" + length).append(" ").append("GETKYOK")
					.append(" ").append(count).append(packet);
		}

		return getokpacket;
	}

	public static void givky_rcv(String incoming) {

		String[] gvkeysplit = incoming.split(" ");
		int noofkeys = Integer.parseInt(gvkeysplit[2]);

		for (int i = 1; i < noofkeys + 1; i++) {

			if (noofkeys == 1) {

				// Adding key and filename
				func.file_table.add(gvkeysplit[5].trim());
				func.file_table.add(gvkeysplit[3].trim() + " "
						+ gvkeysplit[4].trim());
				func.file_table.add(gvkeysplit[6].trim());

				tra.ownfilelist.add(gvkeysplit[5].trim());
			}

			else {

				// Adding key and filename
				func.file_table.add(gvkeysplit[5 + (i - 1) * 4].trim());
				func.file_table.add(gvkeysplit[3 + (i - 1) * 4].trim() + " "
						+ gvkeysplit[4 + (i - 1) * 4].trim());
				func.file_table.add(gvkeysplit[6 + (i - 1) * 4].trim());

				tra.ownfilelist.add(gvkeysplit[5 + (i - 1) * 4].trim());
			}
		}

	}

	public static void add_rcv(String output) throws Exception {

		String[] split = output.split(" ");

		String add_file_key = split[4];
		long add_file_key_dec = Long.parseLong(add_file_key.trim(), 16);

		String pred_key = func.predecessor(tra.hashkey);
		long pred_key_dec = Long.parseLong(pred_key, 16);

		if (pred_key_dec > tra.hashkey_decimal) {

			if (add_file_key_dec > tra.hashkey_decimal
					&& add_file_key_dec <= pred_key_dec) {

				/*
				 * // Send func.add_frwd(add_file_key, split[5].trim(),
				 * split[2].trim(), split[3].trim());
				 */

			}

			else {

				func.file_table.add(add_file_key);
				func.file_table.add("from add");
				func.file_table.add(split[5].trim());

				func.sendok("ADDOK", split[2].trim(), split[3].trim());

			}
		}

		else {
			if (add_file_key_dec > pred_key_dec
					&& add_file_key_dec <= tra.hashkey_decimal) {

				func.file_table.add(add_file_key);
				func.file_table.add("from add");
				func.file_table.add(split[5].trim());

				func.sendok("ADDOK", split[2].trim(), split[3].trim());
			}

			else {

				/*
				 * // Send func.add_frwd(add_file_key, split[5].trim(),
				 * split[2].trim(), split[3].trim());
				 */
			}
		}

	}

}
