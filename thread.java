import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

//Thread implementation ro receive user input from the console
public class thread implements Runnable {

	public void run() {

		try {
			// Get what the user types.
			System.out
					.println("---------------------------------------------------------------");
			System.out.println("Please Enter the option:");
			System.out
					.println("1.details 2. fingertable 3. keytable 4. files 5. search 6. exit");
			System.out
					.println("----------------------------------------------------------------");

			// create object of scanner class
			Scanner reader = new Scanner(System.in);
			String user_input;

			// continously read user input from the console
			while ((user_input = reader.nextLine()) != null) {

				switch (user_input) {

				case "search":

					// Sending Ser

					mess_send.serkey(tra.finger_table);

					break;

				case "details":

					System.out.println("-------------------------");
					System.out.println("IP Address: " + tra.localip);
					System.out.println("Port: " + tra.self_port);
					System.out.println("Key: " + tra.hashkey);
					System.out.println("-------------------------");

					break;

				case "fingertable":

					System.out
							.println("---------------------FINGER TABLE--------------------");

					System.out.println("Start\t\tStop\t\tSuccessor");
					for (long[] row : tra.finger_table) {

						for (long i : row) {
							System.out.print(i);
							System.out.print("\t");
						}
						System.out.println();
					}

					System.out
							.println("--------------------------------------------------");

					break;

				case "keytable":

					System.out
							.println("---------------------KEY TABLE--------------------");
					for (int i = 0; i < func.file_table.size(); i++) {

						if (i % 3 == 0 && i != 0) {
							System.out.println();
						}

						System.out.print(func.file_table.get(i));
						System.out.print("\t");
					}

					System.out.println();
					System.out
							.println("--------------------------------------------------");
					System.out.println("\n");

					break;

				case "files":

					System.out
							.println("------------------FILES PRESENT IN THIS NODE---------------------");
					BufferedReader br;

					br = new BufferedReader(new FileReader("lab5_files.txt"));
					String line = null;
					while ((line = br.readLine()) != null) {
						System.out.println(line);
					}
					System.out
							.println("-----------------------------------------------------------------");

					break;

				case "exit":

					// Send Upfin Update
					mess_send.upfin_leave();

					// Delete own files
					func.ownfiles_delete_exit();

					// Send Give key
					mess_send.give_key();

					// Unregister from bootstrap
					mess_send.unreg();

					System.exit(0);
					break;

				case "exitall":
					System.out.println("exitall code");
					break;

				case "stat":
					System.out
							.println("-----------------STATISTICS--------------");

					System.out.println("Finger Table Size is: "
							+ tra.finger_table.length);
					System.out.println("Key Table Size is: "
							+ func.file_table.size() / 3);
					System.out.println("Query Messages Received: "
							+ tra.messages_rcvd);
					System.out.println("Query Messages Forwarded: "
							+ tra.messages_frwd);
					System.out.println("Query Messages Answered: "
							+ tra.messages_answd);

					System.out
							.println("-----------------------------------------");

				default:
					System.out.println("Please enter a valid option:");
					break;

				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
