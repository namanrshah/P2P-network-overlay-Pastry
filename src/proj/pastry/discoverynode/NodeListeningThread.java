package proj.pastry.discoverynode;

import proj.pastry.transport.Node;
import proj.pastry.transport.TCPConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class NodeListeningThread implements Runnable {

    Node node;
    Logger logger = Logger.getLogger(getClass().getName());

    public NodeListeningThread(Node node) {
        this.node = node;
    }

    @Override
    public void run() {
        try {
            ServerSocket listeningsocket = new ServerSocket(node.retrieveListeningPort());
            while (true) {
                Socket accept = listeningsocket.accept();
                TCPConnection connection = new TCPConnection(accept, node);
                connection.startReceiverThread(node);
            }
        } catch (IOException ex) {
            logger.severe("Exception in listening on : " + node.retrieveListeningPort());
        }
    }

}
