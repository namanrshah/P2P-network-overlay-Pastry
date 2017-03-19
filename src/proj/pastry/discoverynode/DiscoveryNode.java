package proj.pastry.discoverynode;

import proj.pastry.peer.PeerInfo;
import proj.pastry.transport.Node;
import proj.pastry.transport.TCPSender;
import proj.pastry.util.ConsoleCommands;
import proj.pastry.util.Constants;
import proj.patry.wireformats.ClientToDNRequestRandomPeer;
import proj.patry.wireformats.DNRegistrationResponse;
import proj.patry.wireformats.DNSendsRandomPeerToClient;
import proj.patry.wireformats.PeerLeavesPastryToDN;
import proj.patry.wireformats.PeerSendsAckToDN;
import proj.patry.wireformats.PeerSendsReRegistrationRequest;
import proj.patry.wireformats.PeerSendsRegistrationRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class DiscoveryNode implements Node {

    int listeningPort;
    String ownIp;
    String hostname;
    Logger logger = Logger.getLogger(getClass().getName());
    Map<String, PeerInfo> registeredPeers = new HashMap<>();
    Queue<PeerSendsRegistrationRequest> regReqs = new LinkedList<>();
    Worker worker;

    public static void main(String[] args) {
        new DiscoveryNode().start(args);
    }

    public void start(String[] args) {
        try {
            listeningPort = Integer.parseInt(args[0]);
            ownIp = InetAddress.getLocalHost().getHostAddress();
            hostname = InetAddress.getLocalHost().getHostName();
            System.out.println("INFO : Discovery node listening on - " + hostname + Constants.DELIMITORS.TRACEPATH_DELIM + ownIp + ":" + listeningPort);
            //start listening
            NodeListeningThread discoverNodeListeningThread = new NodeListeningThread(this);
            Thread listeningThread = new Thread(discoverNodeListeningThread);
            listeningThread.start();
            //starting command thread
            ConsoleCommands consoleCommands = new ConsoleCommands(this);
            Thread consoleCommandThread = new Thread(consoleCommands);
            consoleCommandThread.start();
            //Start worker thread
            worker = new Worker(this);
            Thread workerThread = new Thread(worker);
            workerThread.start();
            //Start worker to track completion of creation of routing tables
        } catch (UnknownHostException ex) {
            logger.log(Level.SEVERE, "Unknown host : {0}", ownIp);
        }
    }

    @Override
    public void processRequest(byte[] bytes, Socket socket) {
        byte type = bytes[0];
        Random random = new Random();
        switch (type) {
            case Constants.MESSAGES.PEER_SENDS_REGISTRATION_REQUEST:
                PeerSendsRegistrationRequest peerSendsRegistrationRequest = new PeerSendsRegistrationRequest(bytes);
                System.out.println("INFO : Adding JOIN request for " + peerSendsRegistrationRequest.getId() + " in queue.");
                synchronized (regReqs) {
                    regReqs.add(peerSendsRegistrationRequest);
                }
                synchronized (worker.initial) {
                    worker.initial.notify();
                }
//                String peerId = peerSendsRegistrationRequest.getId();
//                String peerIpPort = peerSendsRegistrationRequest.getIpPort();
//                String peerNickname = peerSendsRegistrationRequest.getNickname();
//                synchronized (registeredPeers) {
//                    try {
//                        if (registeredPeers.containsKey(peerId)) {
//                            DNRegistrationResponse regResponse = new DNRegistrationResponse();
//                            regResponse.setRegistered(false);
//                            TCPSender sender = new TCPSender(socket);
//                            sender.sendData(regResponse.getBytes());
//                        } else {
//                            DNRegistrationResponse regResponse = new DNRegistrationResponse();
//                            List<String> keys = new ArrayList(registeredPeers.keySet());
//                            if (registeredPeers.isEmpty()) {
//                                regResponse.setRandomPeerIpPortLen(0);
//                                regResponse.setRandomPeerIdLen(0);
//                            } else {
//                                int randomPeerIndex = random.nextInt(registeredPeers.size());
//                                String randomPeerId = keys.get(randomPeerIndex);
//                                PeerInfo peerInfo = registeredPeers.get(randomPeerId);
//                                String randomIpPortString = peerInfo.getIp() + Constants.DELIMITORS.IP_PORT_DELIM + peerInfo.getPort();
//                                regResponse.setRandomPeerIpPort(randomIpPortString);
//                                regResponse.setRandomPeerIpPortLen(randomIpPortString.length());
//                                regResponse.setRandomPeerId(peerInfo.getId());
//                                regResponse.setRandomPeerIdLen(peerInfo.getId().length());
//                            }
//                            regResponse.setRegistered(true);
//                            String[] split = peerIpPort.split("" + Constants.DELIMITORS.IP_PORT_DELIM);
//                            registeredPeers.put(peerId, new PeerInfo(peerId, split[0], Integer.parseInt(split[1]), peerNickname));
//                            logger.log(Level.INFO, "Registered : {0}-{1}", new Object[]{peerId, peerIpPort});
//                            TCPSender sender = new TCPSender(socket);
//                            sender.sendData(regResponse.getBytes());
//                        }
//                    } catch (IOException ex) {
//                        logger.severe("Sending registration response to Peer.");
//                    }
//                }
                break;
            case Constants.MESSAGES.PEER_SENDS_RE_REGISTRATION_REQUEST:
                PeerSendsReRegistrationRequest peerSendsReRegistrationRequest = new PeerSendsReRegistrationRequest(bytes);
                String peerId = peerSendsReRegistrationRequest.getId();
                String peerIpPort = peerSendsReRegistrationRequest.getIpPort();
                String peerNickname = peerSendsReRegistrationRequest.getNickname();
                synchronized (registeredPeers) {
                    try {
                        if (registeredPeers.containsKey(peerId)) {
                            DNRegistrationResponse regResponse = new DNRegistrationResponse();
                            regResponse.setRegistered(false);
                            TCPSender sender = new TCPSender(socket);
                            sender.sendData(regResponse.getBytes());
                        } else {
                            DNRegistrationResponse regResponse = new DNRegistrationResponse();
                            List<String> keys = new ArrayList(registeredPeers.keySet());
                            if (registeredPeers.isEmpty()) {
                                regResponse.setRandomPeerIpPortLen(0);
                                regResponse.setRandomPeerIdLen(0);
                            } else {
                                int randomPeerIndex = random.nextInt(registeredPeers.size());
                                String randomPeerId = keys.get(randomPeerIndex);
                                PeerInfo peerInfo = registeredPeers.get(randomPeerId);
                                String randomIpPortString = peerInfo.getIp() + Constants.DELIMITORS.IP_PORT_DELIM + peerInfo.getPort();
                                regResponse.setRandomPeerIpPort(randomIpPortString);
                                regResponse.setRandomPeerIpPortLen(randomIpPortString.length());
                                regResponse.setRandomPeerId(peerInfo.getId());
                                regResponse.setRandomPeerIdLen(peerInfo.getId().length());
                            }
                            regResponse.setRegistered(true);
//                            String[] split = peerIpPort.split("" + Constants.DELIMITORS.IP_PORT_DELIM);
//                            registeredPeers.put(peerId, new PeerInfo(peerId, split[0], Integer.parseInt(split[1]), peerNickname));
//                            System.out.println("Registered : " + peerId + "-" + peerIpPort);
                            TCPSender sender = new TCPSender(socket);
                            sender.sendData(regResponse.getBytes());
                        }
                    } catch (IOException ex) {
                        logger.severe("Sending registration response to Peer.");
                    }
                }
                break;
            case Constants.MESSAGES.CLIENT_TO_DN_REQUEST_RANDOM_PEER:
                ClientToDNRequestRandomPeer reqRandomPeer = new ClientToDNRequestRandomPeer(bytes);
                String clientIp = reqRandomPeer.getClientIp();
                int clientPort = reqRandomPeer.getClientPort();
                DNSendsRandomPeerToClient randomPeerToClient = new DNSendsRandomPeerToClient();
                if (registeredPeers != null && !registeredPeers.isEmpty()) {
                    synchronized (registeredPeers) {
                        List<String> keys = new ArrayList(registeredPeers.keySet());
                        int randomPeerIndex = random.nextInt(registeredPeers.size());
                        String randomPeerId = keys.get(randomPeerIndex);
                        PeerInfo peerInfo = registeredPeers.get(randomPeerId);
                        randomPeerToClient.setRandomPeerId(peerInfo.getId());
                        randomPeerToClient.setRandomPeerIp(peerInfo.getIp());
                        randomPeerToClient.setRandomPeerPort(peerInfo.getPort());
                    }
                } else {
                    //No peers registered yet
                    randomPeerToClient.setRandomPeerId(Constants.NO_IDS_REGISTERED_STRING);
                    randomPeerToClient.setRandomPeerIp(Constants.NO_IDS_REGISTERED_STRING);
                    randomPeerToClient.setRandomPeerPort(Constants.NO_IDS_REGISTERED_INT);
                }
                TCPSender sender = new TCPSender(socket);
                 {
                    try {
                        sender.sendData(randomPeerToClient.getBytes());
                    } catch (IOException ex) {
                        System.err.println("ERROR : Sending random peer to client.");
                    }
                }
                break;
            case Constants.MESSAGES.PEER_LEAVES_PASTRY:
                PeerLeavesPastryToDN peerLeavesPastryToDN = new PeerLeavesPastryToDN(bytes);
                String leavingPeerId = peerLeavesPastryToDN.getId();
                System.out.println("INFO : " + leavingPeerId + " is leaving.");
                synchronized (registeredPeers) {
                    registeredPeers.remove(leavingPeerId);
                }
                synchronized (worker.response) {
                    worker.response.notify();
                }
                break;
            case Constants.MESSAGES.PEER_SENDS_ACK_REGISTRATION:
                PeerSendsAckToDN ackToDN = new PeerSendsAckToDN(bytes);
                String idToRegister = ackToDN.getId();
                String ipPort = ackToDN.getIpPort();
                String nickname = ackToDN.getNickname();
                String[] split = ipPort.split("" + Constants.DELIMITORS.IP_PORT_DELIM);
                synchronized (registeredPeers) {
                    registeredPeers.put(idToRegister, new PeerInfo(idToRegister, split[0], Integer.parseInt(split[1]), nickname));
                }
                synchronized (worker.response) {
                    worker.response.notify();
                }
                System.out.println("INFO : Registered Peer : " + idToRegister + "," + ipPort);
                break;
            default:
                System.out.println("WARNING : Default case, type received " + type);
        }
    }

    @Override
    public int retrieveListeningPort() {
        return listeningPort;
    }

    public void printingPeers() {
        System.out.println("INFO : Printing peers");
        synchronized (registeredPeers) {
            if (!registeredPeers.isEmpty()) {
                for (Map.Entry<String, PeerInfo> entrySet : registeredPeers.entrySet()) {
                    String peerId = entrySet.getKey();
                    PeerInfo peerInfo = entrySet.getValue();
                    System.out.println(peerId + " = " + peerInfo.getIp() + ":" + peerInfo.getPort() + ", " + peerInfo.getNickname());
                }
            } else {
                System.out.println("INFO : No peers registered yet.");
            }
        }
    }
}
