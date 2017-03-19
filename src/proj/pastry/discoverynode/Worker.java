package proj.pastry.discoverynode;

import proj.pastry.peer.PeerInfo;
import proj.pastry.transport.TCPSender;
import proj.pastry.util.Constants;
import proj.patry.wireformats.DNRegistrationResponse;
import proj.patry.wireformats.PeerSendsRegistrationRequest;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class Worker implements Runnable {

    DiscoveryNode discoveryNode;
    final Object initial;
    final Object response;

    public Worker(DiscoveryNode discoveryNode) {
        this.initial = new Object();
        this.response = new Object();
        this.discoveryNode = discoveryNode;
    }

    @Override
    public void run() {
        while (true) {
            PeerSendsRegistrationRequest poll = discoveryNode.regReqs.poll();
            if (poll == null) {
                synchronized (initial) {
                    try {
                        initial.wait();
                        poll = discoveryNode.regReqs.poll();
                    } catch (InterruptedException ex) {
                        System.err.println("ERROR : Waiting for initial.");
                    }
                }
            }
//            PeerSendsRegistrationRequest poll = new PeerSendsRegistrationRequest(bytes);
            String peerId = poll.getId();
            String peerIpPort = poll.getIpPort();
            String peerNickname = poll.getNickname();
            String[] split = peerIpPort.split("" + Constants.DELIMITORS.IP_PORT_DELIM);

            synchronized (discoveryNode.registeredPeers) {
                try {
                    Random random = new Random();
                    Socket socket = new Socket(split[0], Integer.parseInt(split[1]));
                    DNRegistrationResponse regResponse = new DNRegistrationResponse();
                    if (discoveryNode.registeredPeers.containsKey(peerId)) {
                        regResponse.setRegistered(false);
//                        TCPSender sender = new TCPSender(socket);
//                        sender.sendData(regResponse.getBytes());
                    } else {
                        List<String> keys = new ArrayList(discoveryNode.registeredPeers.keySet());
                        if (discoveryNode.registeredPeers.isEmpty()) {
                            regResponse.setRandomPeerIpPortLen(0);
                            regResponse.setRandomPeerIdLen(0);
                        } else {
                            int randomPeerIndex = random.nextInt(discoveryNode.registeredPeers.size());
                            String randomPeerId = keys.get(randomPeerIndex);
                            PeerInfo peerInfo = discoveryNode.registeredPeers.get(randomPeerId);
                            String randomIpPortString = peerInfo.getIp() + Constants.DELIMITORS.IP_PORT_DELIM + peerInfo.getPort();
                            regResponse.setRandomPeerIpPort(randomIpPortString);
                            regResponse.setRandomPeerIpPortLen(randomIpPortString.length());
                            regResponse.setRandomPeerId(peerInfo.getId());
                            regResponse.setRandomPeerIdLen(peerInfo.getId().length());
                        }
                        regResponse.setRegistered(true);
//                        discoveryNode.registeredPeers.put(peerId, new PeerInfo(peerId, split[0], Integer.parseInt(split[1]), peerNickname));
//                        TCPSender sender = new TCPSender(socket);
//                        sender.sendData(regResponse.getBytes());
                    }
                    TCPSender sender = new TCPSender(socket);
                    sender.sendData(regResponse.getBytes());
                } catch (IOException ex) {
                    System.err.println("ERROR : Sending registration response to Peer.");
                }
            }
            synchronized (response) {
                try {
                    response.wait();
                } catch (InterruptedException ex) {
                    System.err.println("ERROR : Interruption in waiting for JOIN response for " + peerId);
                }
            }
        }
    }

}
