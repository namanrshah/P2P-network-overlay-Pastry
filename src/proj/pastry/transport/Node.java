package proj.pastry.transport;

import java.net.Socket;

/**
 *
 * @author Rajiv
 */
public interface Node {

    public void processRequest(byte[] bytes, Socket socket);

    public int retrieveListeningPort();
}
