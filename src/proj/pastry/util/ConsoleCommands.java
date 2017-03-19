package proj.pastry.util;

import proj.pastry.discoverynode.DiscoveryNode;
import proj.pastry.pastryclient.PastryClient;
import proj.pastry.peer.Peer;
import proj.pastry.transport.Node;
import java.io.File;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 *
 * @author Naman
 */
public class ConsoleCommands implements Runnable {

    private final Node node;
    private Scanner sc;
    Logger logger = Logger.getLogger(getClass().getName());

    public ConsoleCommands(Node node) {
        this.node = node;
        sc = new Scanner(System.in);
    }

    @Override
    public void run() {
        while (true) {
            String command = sc.nextLine();
            this.processCommand(command);
        }
    }

    public void processCommand(String command) {
        logger.info("Command : " + command);
        String[] instruction = command.split(" ");
        switch (instruction[0].toLowerCase()) {
            case Constants.COMMANDS.DISCOVERY_NODE.LIST_PEERS:
                ((DiscoveryNode) node).printingPeers();
                break;
            case Constants.COMMANDS.PEER.LEAVE_PASTRY:
                ((Peer) node).leavePastry();
                break;
            case Constants.COMMANDS.PEER.LIST_FILES:
                ((Peer) node).listFiles();
                break;
            case Constants.COMMANDS.PEER.SELF_INFO:
                ((Peer) node).printSelfInfo();
                break;
            case Constants.COMMANDS.STORE_CLINET.STORE_FILE:
                PastryClient client = ((PastryClient) node);
                String filePath = instruction[1];
                if (instruction.length == 2) {
                    client.storeFile(filePath, null);
                } else if (instruction.length == 3) {
                    String key = instruction[2];
                    int intValue;
                    try {
                        intValue = Integer.parseInt(key, 16);
                        synchronized (client.filesWithCustomKey) {
                            client.filesWithCustomKey.put(filePath, key);
                        }
                        client.storeFile(filePath, key);
                    } catch (NumberFormatException nfe) {
                        System.err.println("ERROR : Invalid key, please try again.");
                    }
                } else {
                    System.err.println("ERROR : Invalid store command, please try again with 1 or 2 parameters.");
                }
                break;
            case Constants.COMMANDS.STORE_CLINET.RETRIEVE_FILE:
                PastryClient clientR = ((PastryClient) node);
                String filePathToRetrieve = instruction[1];
                if (instruction.length == 2) {
                    clientR.retrieveFile(filePathToRetrieve);
                } else {
                    System.err.println("ERROR : Invalid read command, please try again with 1 parameter only.");
                }
                break;
            default:
                logger.warning("Invalid command");
        }
//        if (command.length() == 4) {
//            //Peer id
//        }
    }
}
