package proj.pastry.peer;

import proj.pastry.discoverynode.NodeListeningThread;
import proj.pastry.transport.Node;
import proj.pastry.transport.TCPConnection;
import proj.pastry.transport.TCPSender;
import proj.pastry.util.ConsoleCommands;
import proj.pastry.util.Constants;
import proj.pastry.util.DistanceDirection;
import proj.pastry.util.SystemFunctionUtil;
import proj.patry.wireformats.DNRegistrationResponse;
import proj.patry.wireformats.DestinationSendsJoinResponse;
import proj.patry.wireformats.DestinationSendsLookupResponseForRetrieve;
import proj.patry.wireformats.DestinationSendsLookupResponseForStore;
import proj.patry.wireformats.FileDetailsToSend;
import proj.patry.wireformats.LeafSendsFileToNewPeer;
import proj.patry.wireformats.LookupDestinationForFileKey;
import proj.patry.wireformats.PeerLeavesPastryToDN;
import proj.patry.wireformats.PeerRequestsFilesWhileJoining;
import proj.patry.wireformats.PeerSendsAckToDN;
import proj.patry.wireformats.PeerSendsRegistrationRequest;
import proj.patry.wireformats.PeerSendsJoinRequest;
import proj.patry.wireformats.PeerSendsReRegistrationRequest;
import proj.patry.wireformats.PeerSendsUpdateLeaf;
import proj.patry.wireformats.PeerSendsUpdateRT;
import proj.patry.wireformats.StoreFile;
import proj.patry.wireformats.StoreFileFromLeavingPeer;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class Peer implements Node {

    int listeningPort;
    String ownIp;
    String hostname;
    String discoveryNodeIp;
    int discoveryNodeListeningPort;
    PeerInfo selfInfo;
    public String id;
    //<row(Starting from 0), <column, <peerId, peerinfo>>
    Map<Integer, Map<Character, Map<String, PeerInfo>>> routingTable = new HashMap<>();
    //<postion(L/R), peerinfo>
    Map<Character, PeerInfo> leafNodes = new HashMap<>();
    //<fileKey, completefilePath>
    Map<String, String> fileDetails = new HashMap<>();
    Logger logger = Logger.getLogger(getClass().getName());

    public static void main(String[] args) {
        //args[0] - own listening port
        //args[1] - DN ip address
        //args[2] - DN listenig port
        //args[3] - own ID
        new Peer().start(args);
    }

    public void start(String[] args) {
        try {
            listeningPort = Integer.parseInt(args[0]);
            ownIp = InetAddress.getLocalHost().getHostAddress();
            hostname = InetAddress.getLocalHost().getHostName();
            discoveryNodeIp = args[1];
            discoveryNodeListeningPort = Integer.parseInt(args[2]);
            logger.info(hostname + " listening on - " + ownIp + " : " + listeningPort);
            //start listening
            NodeListeningThread peerListeningThread = new NodeListeningThread(this);
            Thread listeningThread = new Thread(peerListeningThread);
            listeningThread.start();
            //starting command thread
            ConsoleCommands consoleCommands = new ConsoleCommands(this);
            Thread consoleCommandThread = new Thread(consoleCommands);
            consoleCommandThread.start();
            //Generate/Receive id and send registration request to DN
            if (args.length == 3) {
                Long currentTime = System.nanoTime();
                String hex = Long.toHexString(currentTime);
                id = hex.substring(hex.length() - 4);
                logger.info("Generated ID : " + id);
            } else if (args.length == 4) {
                id = args[3].toLowerCase().trim();
                logger.info("Received ID : " + id);
            }
            PeerSendsRegistrationRequest regReq = new PeerSendsRegistrationRequest();
            regReq.setId(id);
            regReq.setIpPort(ownIp + Constants.DELIMITORS.IP_PORT_DELIM + listeningPort);
            regReq.setNickname(hostname);
            try {
//                TCPSender sender = new TCPSender(new Socket(discoveryNodeIp, discoveryNodeListeningPort));
//                sender.sendData(regReq.getBytes());
                TCPConnection connection = new TCPConnection(new Socket(discoveryNodeIp, discoveryNodeListeningPort), this);
//                connection.startReceiverThread(this);
                connection.getSender().sendData(regReq.getBytes());
            } catch (IOException ex) {
                logger.severe("Connecting to Discovery node.");
            }
        } catch (UnknownHostException ex) {
            logger.severe("Unknown host : " + ownIp);
        }
    }

    @Override
    public void processRequest(byte[] bytes, Socket socket) {
        try {
            byte type = bytes[0];
            Random random = new Random();
            switch (type) {
                case Constants.MESSAGES.DN_SENDS_REGISTRATION_RESPONSE:
                    DNRegistrationResponse dNRegistrationResponse = new DNRegistrationResponse(bytes);
                    boolean registered = dNRegistrationResponse.isRegistered();
                    if (!registered) {
                        //Re-generate id and send request again
                        Long currentTime = System.nanoTime();
                        String hex = Long.toHexString(currentTime);
                        id = hex.substring(hex.length() - 4);
                        System.out.println("INFO : Re-generated ID : " + id);
                        PeerSendsReRegistrationRequest regReq = new PeerSendsReRegistrationRequest();
                        regReq.setId(id);
                        regReq.setIpPort(ownIp + Constants.DELIMITORS.IP_PORT_DELIM + listeningPort);
                        regReq.setNickname(hostname);
                        try {
//                        TCPSender sender = new TCPSender(new Socket(discoveryNodeIp, discoveryNodeListeningPort));
//                        sender.sendData(regReq.getBytes());
                            TCPConnection connection = new TCPConnection(new Socket(discoveryNodeIp, discoveryNodeListeningPort), this);
                            connection.startReceiverThread(this);
                            connection.getSender().sendData(regReq.getBytes());
                        } catch (IOException ex) {
                            logger.severe("Connecting to Discovery node.");
                        }
                    } else {
                        selfInfo = new PeerInfo(id, ownIp, listeningPort, hostname);
                        try {
                            logger.info("Registered");
                            //Send request to random port for own RT creation
                            int randomNodeIpPortLen = dNRegistrationResponse.getRandomPeerIpPortLen();
                            int randomPeerIdLen = dNRegistrationResponse.getRandomPeerIdLen();
                            if (randomNodeIpPortLen > 0 && randomPeerIdLen > 0) {
                                String randomPeerIpPort = dNRegistrationResponse.getRandomPeerIpPort();
                                String randomPeerId = dNRegistrationResponse.getRandomPeerId();
                                System.out.println("INFO : Random peer id : " + randomPeerId);
                                String[] ipPortArray = randomPeerIpPort.split("" + Constants.DELIMITORS.IP_PORT_DELIM);
                                PeerSendsJoinRequest joinRequest = new PeerSendsJoinRequest();
                                joinRequest.setId(id);
                                joinRequest.setIpPort(ownIp + Constants.DELIMITORS.IP_PORT_DELIM + listeningPort);
                                joinRequest.setLastPeerId(id);
                                joinRequest.setHopCount(0);
                                joinRequest.setTracepath(Constants.NO_IDS_REGISTERED_STRING);
                                //creating routing table
                                int idLength = id.length();
                                Map<Integer, Map<Character, Map<String, PeerInfo>>> initialRoutingTable = new HashMap<>();
                                joinRequest.setRoutingTable(initialRoutingTable);
//                                logger.info("Initial routing table : ");
                                for (int i = 0; i < idLength; i++) {
                                    Map<Character, Map<String, PeerInfo>> columns = new HashMap<>();
                                    initialRoutingTable.put(i, columns);
                                    for (int j = 0; j < 16; j++) {
//                                        if (id.charAt(i) != Integer.toHexString(j).charAt(0)) {
                                        columns.put(Integer.toHexString(j).charAt(0), new HashMap<String, PeerInfo>());
//                                        }
                                    }
//                                    logger.log(Level.INFO, "{0} = {1}", new Object[]{i, columns});
//                                    System.out.println(i + " = " + columns);
                                }
                                joinRequest.setRoutingTableLength(joinRequest.getRoutingTable().size());

//                            joinRequest.setRoutingTableLength(0);
                                System.out.println("INFO : sending JOIN request to : " + randomPeerId);
                                TCPSender sender = new TCPSender(new Socket(ipPortArray[0], Integer.parseInt(ipPortArray[1])));
                                sender.sendData(joinRequest.getBytes());
//                            TCPConnection connection = new TCPConnection(new Socket(discoveryNodeIp, discoveryNodeListeningPort), this);
//                            connection.startReceiverThread(this);
//                            connection.getSender().sendData(regReq.getBytes());
                            } else {
                                logger.info("First registered.");
                                logger.info("Routing table : ");
                                //creating empty routing table
                                int idLength = id.length();
                                synchronized (routingTable) {
                                    for (int i = 0; i < idLength; i++) {
                                        Map<Character, Map<String, PeerInfo>> columns = new TreeMap<>();
                                        routingTable.put(i, columns);
                                        for (int j = 0; j < 16; j++) {
                                            if (id.charAt(i) != Integer.toHexString(j).charAt(0)) {
                                                columns.put(Integer.toHexString(j).charAt(0), new TreeMap<String, PeerInfo>());
                                            }
                                        }
                                        System.out.println(i + " = " + columns);
                                    }
                                }
                                //send ACK to DN
                                PeerSendsAckToDN ackToDN = new PeerSendsAckToDN();
                                ackToDN.setId(id);
                                ackToDN.setIpPort(ownIp + Constants.DELIMITORS.IP_PORT_DELIM + listeningPort);
                                ackToDN.setNickname(hostname);
                                TCPSender ackSender = new TCPSender(new Socket(discoveryNodeIp, discoveryNodeListeningPort));
                                ackSender.sendData(ackToDN.getBytes());
                                System.out.println("INFO : Completed Registration");
                            }
                        } catch (IOException ex) {
                            logger.severe("Sending RT creation request from new peer");
                        }
                    }
                    break;
                case Constants.MESSAGES.PEER_SENDS_JOIN_REQUEST:
                    PeerSendsJoinRequest joinRequest = new PeerSendsJoinRequest(bytes);
                    String newPeerId = joinRequest.getId();
                    String newPeerIpPort = joinRequest.getIpPort();
                    String lastPeerId = joinRequest.getLastPeerId();
                    String[] ipPort = newPeerIpPort.split(Constants.DELIMITORS.IP_PORT_DELIM + "");
                    int hopCount = joinRequest.getHopCount();
                    hopCount += 1;
                    String tracepath = joinRequest.getTracepath();

                    int routingTableLength = joinRequest.getRoutingTableLength();
                    Map<Integer, Map<Character, Map<String, PeerInfo>>> tempRoutingTable;
                    if (routingTableLength > 0) {
                        tempRoutingTable = joinRequest.getRoutingTable();
                    } else {
                        tempRoutingTable = new TreeMap<>();
                    }
                    if (newPeerId.equals(lastPeerId)) {
                        System.out.println("INFO : Received join request from newly added  node : " + newPeerId);
                        tracepath = id;
                    } else {
                        System.out.println("INFO : Received join request from : " + lastPeerId + " for node : " + newPeerId);
                        tracepath += Constants.DELIMITORS.TRACEPATH_DELIM + id;
                    }
                    System.out.println("INFO : Current hop count : " + hopCount);
                    System.out.println("INFO : Current path : " + tracepath);
                    //Copy unmatch row from current peer to new peer's routing table
                    int unmatchPrefix = SystemFunctionUtil.matchPrefix(newPeerId, id);
                    synchronized (routingTable) {
                        for (int i = 0; i <= unmatchPrefix; i++) {
                            Map<Character, Map<String, PeerInfo>> unmatchedRow = routingTable.get(i);
                            Map<Character, Map<String, PeerInfo>> unmatchedRowInTRT = tempRoutingTable.get(i);
                            for (Map.Entry<Character, Map<String, PeerInfo>> entrySet : unmatchedRow.entrySet()) {
                                Character column = entrySet.getKey();
                                Map<String, PeerInfo> peerHandle = entrySet.getValue();
                                Map<String, PeerInfo> peerHandleInTRT = unmatchedRowInTRT.get(column);
                                if (peerHandle != null && !peerHandle.isEmpty()) {
                                    if (peerHandleInTRT == null || peerHandleInTRT.isEmpty()) {
                                        //appropriate cell is null or empty, so replace with current RT's entry
                                        unmatchedRowInTRT.put(column, peerHandle);
//                                } else {
//                                    //check whether current RT's entry is better than TRT's entry
//
                                    }
                                } else {
                                    //Current RT have null/empty entry, so don't do anything
                                }
                            }
//                        //Put entry for current peer at appropriate row and column
                            Map<String, PeerInfo> columnIndexOfCurrentPeer = unmatchedRowInTRT.get(id.charAt(i));
                            if (columnIndexOfCurrentPeer == null || columnIndexOfCurrentPeer.isEmpty()) {
                                Map<String, PeerInfo> peerHandle = new HashMap<>();
                                peerHandle.put(id, selfInfo);
                                unmatchedRowInTRT.put(id.charAt(i), peerHandle);
                            }
                        }
                    }
                    //check whether this peer is destination or not
                    //check whether new peer is in between leaf nodes and self.
                    PeerInfo leftLeaf;
                    PeerInfo rightLeaf;
                    synchronized (leafNodes) {
                        leftLeaf = leafNodes.get(Constants.DIRECTION.LEFT);
                        rightLeaf = leafNodes.get(Constants.DIRECTION.RIGHT);
                    }
                    String nextPeer = "";
                    PeerInfo nextPeerInfo = null;
                    if (leftLeaf != null) {
                        //more than one peer are in pastry
                        //find next peer
                        nextPeerInfo = findNextPeer(newPeerId, leftLeaf, rightLeaf);
                        nextPeer = nextPeerInfo.getId();
//                        System.out.println("Next Peer: " + nextPeer);
//                        System.out.println(" newPeerId: " + newPeerId);
//                        System.out.println("nextPeerInfo.getIp(): " + nextPeerInfo.getIp());
//                        System.out.println("extPeerInfo.getPort(): " + nextPeerInfo.getPort());
//                        System.out.println("ipPort[0" + ipPort[0]);
//                        System.out.println("ipPort[1" + ipPort[1]);

                        if (nextPeer.equals(newPeerId) || ((nextPeerInfo.getIp().equals(ipPort[0]) && nextPeerInfo.getPort() == Integer.parseInt(ipPort[1])))) {
                            //nextPeer was removed and rejoined on same port
                            //Remove entry from routing table
//                            System.out.println("INFO : Trying to remove " + nextPeer);
                            synchronized (routingTable) {
                                boolean found = false;
                                for (Iterator<Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>>> it = routingTable.entrySet().iterator(); it.hasNext();) {
                                    Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet = it.next();
                                    Integer row = entrySet.getKey();
                                    Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
                                    for (Iterator<Map.Entry<Character, Map<String, PeerInfo>>> it1 = columns.entrySet().iterator(); it1.hasNext();) {
                                        Map.Entry<Character, Map<String, PeerInfo>> entrySet1 = it1.next();
                                        Character column = entrySet1.getKey();
                                        Map<String, PeerInfo> peerHandle = entrySet1.getValue();
                                        if (peerHandle != null && !peerHandle.isEmpty()) {
                                            String key = (String) new ArrayList(peerHandle.keySet()).get(0);
                                            PeerInfo info = peerHandle.get(key);
                                            if (key.equals(nextPeer)) {
//                                                System.out.println("INFO : Removing key equal : " + key);
                                                peerHandle.clear();
//                                                peerHandle.remove(newPeerId);                                                
                                                found = true;
                                                break;
                                            } else if (info.getIp().equals(nextPeerInfo.getIp()) && info.getPort() == nextPeerInfo.getPort()) {
//                                                System.out.println("INFO : Removing ipPort equal : " + key);
                                                peerHandle.clear();
//                                                peerHandle.remove(newPeerId);                                                
                                                found = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (found) {
                                        break;
                                    }
                                }
                            }
                            nextPeerInfo = findNextPeer(newPeerId, leftLeaf, rightLeaf);
                            nextPeer = nextPeerInfo.getId();
                        }
                        //send request to next peer if its not self.
                        //If nextPeer is self, then self is destination
                        if (nextPeer.equals(id)) {
                            try {
                                //self is destination,so send response to newly added peer
                                DistanceDirection distanceAndDirectionFromSelf = SystemFunctionUtil.getDistanceAndDirection(id, newPeerId);
                                System.out.println("INFO : " + id + " is destination, responding back to : " + newPeerId);
                                DestinationSendsJoinResponse joinResponse = new DestinationSendsJoinResponse();
                                joinResponse.setDestId(id);
                                joinResponse.setHopCount(hopCount);
                                joinResponse.setTracepath(tracepath);
                                joinResponse.setRoutingTable(tempRoutingTable);
                                joinResponse.setRoutingTableLength(tempRoutingTable.size());
                                char direction = distanceAndDirectionFromSelf.getDirection();
                                if (leftLeaf != null) {
                                    if (direction == Constants.DIRECTION.RIGHT) {
                                        //send right leaf as new peer's right leaf
                                        joinResponse.setLeafNode(rightLeaf);
                                        joinResponse.setLeafNodeLen(rightLeaf.getBytes().length);
                                        joinResponse.setLeafNodeDirection(Constants.DIRECTION.RIGHT);
                                        joinResponse.setDestNode(selfInfo);
                                        joinResponse.setDestNodeLen(selfInfo.getBytes().length);
                                        joinResponse.setDestNodeDirection(Constants.DIRECTION.LEFT);
                                    } else {
                                        //send left leaf as new peer's left leaf
                                        joinResponse.setLeafNode(leftLeaf);
                                        joinResponse.setLeafNodeLen(leftLeaf.getBytes().length);
                                        joinResponse.setLeafNodeDirection(Constants.DIRECTION.LEFT);
                                        joinResponse.setDestNode(selfInfo);
                                        joinResponse.setDestNodeLen(selfInfo.getBytes().length);
                                        joinResponse.setDestNodeDirection(Constants.DIRECTION.RIGHT);
                                    }
                                } else {
                                    joinResponse.setLeafNode(selfInfo);
                                    joinResponse.setLeafNodeDirection(Constants.DIRECTION.RIGHT);
                                    joinResponse.setLeafNodeLen(selfInfo.getBytes().length);
                                    joinResponse.setDestNode(selfInfo);
                                    joinResponse.setDestNodeLen(selfInfo.getBytes().length);
                                    joinResponse.setDestNodeDirection(Constants.DIRECTION.LEFT);
                                }
//                            joinResponse.setRoutingTableLength(0);

                                TCPSender sender = new TCPSender(new Socket(ipPort[0], Integer.parseInt(ipPort[1])));
                                sender.sendData(joinResponse.getBytes());
                            } catch (IOException ex) {
                                logger.log(Level.SEVERE, "Sending response to newly added node : {0}", newPeerId);
                            }
                        } else {
                            try {
                                //forward to closer peer
                                System.out.println("INFO : Forwarding JOIN request to : " + nextPeer);
                                PeerSendsJoinRequest forwardedRequest = new PeerSendsJoinRequest();
                                forwardedRequest.setId(newPeerId);
                                forwardedRequest.setIpPort(newPeerIpPort);
                                forwardedRequest.setLastPeerId(id);
                                forwardedRequest.setHopCount(hopCount);
                                forwardedRequest.setTracepath(tracepath);
                                forwardedRequest.setRoutingTableLength(tempRoutingTable.size());
                                forwardedRequest.setRoutingTable(tempRoutingTable);
                                TCPSender sender = new TCPSender(new Socket(nextPeerInfo.getIp(), nextPeerInfo.getPort()));
                                sender.sendData(forwardedRequest.getBytes());
                            } catch (IOException ex) {
                                logger.log(Level.SEVERE, "Forwarding JOIN request to : {0}", nextPeerInfo.getId());
                            }
                        }
                    } else {
                        try {
                            //Only one peer in pastry, excluding newly added peer
                            nextPeer = id;
                            System.out.println("INFO : " + id + " is the only peer in pastry, so its the destination.");
                            DestinationSendsJoinResponse joinResponse = new DestinationSendsJoinResponse();
                            joinResponse.setDestId(id);
                            joinResponse.setHopCount(hopCount);
                            joinResponse.setTracepath(tracepath);
                            joinResponse.setDestNode(selfInfo);
                            joinResponse.setDestNodeDirection(Constants.DIRECTION.RIGHT);
                            joinResponse.setDestNodeLen(selfInfo.getBytes().length);
                            joinResponse.setLeafNode(selfInfo);
                            joinResponse.setLeafNodeDirection(Constants.DIRECTION.LEFT);
                            joinResponse.setLeafNodeLen(selfInfo.getBytes().length);
                            joinResponse.setRoutingTable(tempRoutingTable);
                            joinResponse.setRoutingTableLength(tempRoutingTable.size());
                            TCPSender sender = new TCPSender(new Socket(ipPort[0], Integer.parseInt(ipPort[1])));
                            sender.sendData(joinResponse.getBytes());
                        } catch (IOException ex) {
                            logger.log(Level.SEVERE, "Sending response to newly added node : {0}", newPeerId);
                        }
                    }
                    break;
                case Constants.MESSAGES.DESTINATION_SENDS_JOIN_RESPONSE:
                    DestinationSendsJoinResponse joinResponse = new DestinationSendsJoinResponse(bytes);
                    String destId = joinResponse.getDestId();
                    int hopCountRcvd = joinResponse.getHopCount();
                    String tracepathRcvd = joinResponse.getTracepath();
                    PeerInfo destNode = joinResponse.getDestNode();
                    char destNodeDirection = joinResponse.getDestNodeDirection();
                    PeerInfo leafNode = joinResponse.getLeafNode();
                    char leafNodeDirection = joinResponse.getLeafNodeDirection();
                    System.out.println("INFO : Received JOIN response from : " + destId);
                    System.out.println("INFO : Hop count : " + hopCountRcvd);
                    System.out.println("INFO : Path : " + tracepathRcvd);
                    //storing leaf nodes
                    System.out.println("INFO : Leaf nodes");
                    synchronized (leafNodes) {
                        leafNodes.put(destNodeDirection, destNode);
                        leafNodes.put(leafNodeDirection, leafNode);
                        System.out.println(destNodeDirection + " = " + destNode.getId());
                        System.out.println(leafNodeDirection + " = " + leafNode.getId());
                    }
                    //store routing table
                    int routingTableLength1 = joinResponse.getRoutingTableLength();
                    if (routingTableLength1 > 0) {
                        Map<Integer, Map<Character, Map<String, PeerInfo>>> routingTableToStore = joinResponse.getRoutingTable();
                        for (Iterator<Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>>> iterator = routingTableToStore.entrySet().iterator(); iterator.hasNext();) {
                            Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> row = iterator.next();
                            Map<Character, Map<String, PeerInfo>> columns = row.getValue();
                            columns.remove(id.charAt(row.getKey()));
                        }
                        synchronized (routingTable) {
//                            logger.info("Final routing table : ");
                            routingTable = routingTableToStore;
                        }
                    }
                    //Update others about newly created node
                    //Send update to leaf nodes.
                    //Update left leaf of right leaf, set it to newly added peer                    
                    PeerInfo rightLeafToUpdate;
                    PeerInfo leftLeafToUpdate;
                    synchronized (leafNodes) {
                        rightLeafToUpdate = leafNodes.get(Constants.DIRECTION.RIGHT);
                        leftLeafToUpdate = leafNodes.get(Constants.DIRECTION.LEFT);
                    }
                    PeerSendsUpdateLeaf updateRightLeaf = new PeerSendsUpdateLeaf();
                    updateRightLeaf.setDirection(Constants.DIRECTION.LEFT);
                    updateRightLeaf.setSendingPeerId(id);
                    updateRightLeaf.setLeafInfo(selfInfo);
                    updateRightLeaf.setLeafInfoLen(selfInfo.getBytes().length);
                    System.out.println("INFO : Sending left leaf update to : " + rightLeafToUpdate.getId());
                    TCPSender rightLeafUpdateSender = new TCPSender(new Socket(rightLeafToUpdate.getIp(), rightLeafToUpdate.getPort()));
                    rightLeafUpdateSender.sendData(updateRightLeaf.getBytes());
                    //Update right leaf of left leaf, set it to newly added peer
                    PeerSendsUpdateLeaf updateLeftLeaf = new PeerSendsUpdateLeaf();
                    updateLeftLeaf.setDirection(Constants.DIRECTION.RIGHT);
                    updateLeftLeaf.setSendingPeerId(id);
                    updateLeftLeaf.setLeafInfo(selfInfo);
                    updateLeftLeaf.setLeafInfoLen(selfInfo.getBytes().length);
                    System.out.println("INFO : Sending right leaf update to : " + leftLeafToUpdate.getId());
                    TCPSender leftLeafUpdateSender = new TCPSender(new Socket(leftLeafToUpdate.getIp(), leftLeafToUpdate.getPort()));
                    leftLeafUpdateSender.sendData(updateLeftLeaf.getBytes());
                    //Send update to routing table entries
                    synchronized (routingTable) {
                        for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                            Integer row = entrySet.getKey();
                            Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
                            for (Map.Entry<Character, Map<String, PeerInfo>> entrySet1 : columns.entrySet()) {
                                Character column = entrySet1.getKey();
                                Map<String, PeerInfo> peerHandle = entrySet1.getValue();
                                if (peerHandle != null && !peerHandle.isEmpty()) {
                                    for (Iterator<Map.Entry<String, PeerInfo>> it = peerHandle.entrySet().iterator(); it.hasNext();) {
                                        Map.Entry<String, PeerInfo> entrySet2 = it.next();
                                        String peerHandleId = entrySet2.getKey();
                                        PeerInfo peerHandleInfo = entrySet2.getValue();
                                        PeerSendsUpdateRT updateRT = new PeerSendsUpdateRT();
                                        updateRT.setLeafInfo(selfInfo);
                                        updateRT.setLeafInfoLen(selfInfo.getBytes().length);
                                        System.out.println("INFO : Sending routing table update to : " + peerHandleId);
                                        try {
                                            Socket updateRTSocket = new Socket(peerHandleInfo.getIp(), peerHandleInfo.getPort());
                                            TCPSender updateRTSender = new TCPSender(updateRTSocket);
                                            updateRTSender.sendData(updateRT.getBytes());
                                        } catch (ConnectException ex) {
                                            System.out.println("INFO : " + peerHandleId + " is already down, so removing from routin table.");
                                            it.remove();
                                        }
                                    }
                                }
                            }
//                            System.out.println(row + " = " + columns);
                        }
                        System.out.println("INFO : Final routing table.");
                        for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                            Integer row = entrySet.getKey();
                            Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
//                                logger.log(Level.INFO, "{0} = {1}", new Object[]{row, columns});
                            System.out.println(row + " = " + columns);
                        }
                    }
                    //request files from leaves
                    System.out.println("INFO : Requesting files from leaves");
                    if (!rightLeafToUpdate.getId().equals(leftLeafToUpdate.getId())) {
                        //Both leaves are different, send 2 requests
                        PeerRequestsFilesWhileJoining toLeftPeer = new PeerRequestsFilesWhileJoining();
                        toLeftPeer.setId(id);
                        toLeftPeer.setIp(ownIp);
                        toLeftPeer.setPort(listeningPort);
                        System.out.println("INFO : Requesting files from left leaf : " + leftLeafToUpdate.getId());
                        Socket leftLeafSocket = new Socket(leftLeafToUpdate.getIp(), leftLeafToUpdate.getPort());
                        TCPSender leftLeafSender = new TCPSender(leftLeafSocket);
                        leftLeafSender.sendData(toLeftPeer.getBytes());
                        try ( //waiting for response
                                DataInputStream din = new DataInputStream(leftLeafSocket.getInputStream())) {
                            int dataLength = din.readInt();
                            byte[] data = new byte[dataLength];
                            din.readFully(data, 0, dataLength);
                            byte leftLeafType = data[0];
                            if (leftLeafType == Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER) {
                                this.storeReceivedFiles(data);
                            } else {
                                System.err.println("ERROR : Waiting for message type " + Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER + " but get " + type);
                            }
                            din.close();
                        }
                        PeerRequestsFilesWhileJoining toRightPeer = new PeerRequestsFilesWhileJoining();
                        toRightPeer.setId(id);
                        toRightPeer.setIp(ownIp);
                        toRightPeer.setPort(listeningPort);
                        System.out.println("INFO : Requesting files from right leaf : " + rightLeafToUpdate.getId());
                        Socket rightLeafSocket = new Socket(rightLeafToUpdate.getIp(), rightLeafToUpdate.getPort());
                        TCPSender rightLeafSender = new TCPSender(rightLeafSocket);
                        rightLeafSender.sendData(toRightPeer.getBytes());
                        try ( //waiting for response
                                DataInputStream din = new DataInputStream(rightLeafSocket.getInputStream())) {
                            int dataLength = din.readInt();
                            byte[] data = new byte[dataLength];
                            din.readFully(data, 0, dataLength);
                            byte leftLeafType = data[0];
                            if (leftLeafType == Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER) {
                                this.storeReceivedFiles(data);
                            } else {
                                System.err.println("ERROR : Waiting for message type " + Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER + " but get " + type);
                            }
                            din.close();
                        }
                    } else {
                        //Both leaves are same, send only 1 request
                        PeerRequestsFilesWhileJoining toLeftPeer = new PeerRequestsFilesWhileJoining();
                        toLeftPeer.setId(id);
                        toLeftPeer.setIp(ownIp);
                        toLeftPeer.setPort(listeningPort);
                        System.out.println("INFO : Requesting files from leaf : " + leftLeafToUpdate.getId());
                        Socket leftLeafSocket = new Socket(leftLeafToUpdate.getIp(), leftLeafToUpdate.getPort());
                        TCPSender leftLeafSender = new TCPSender(leftLeafSocket);
                        leftLeafSender.sendData(toLeftPeer.getBytes());
                        try ( //waiting for response
                                DataInputStream din = new DataInputStream(leftLeafSocket.getInputStream())) {
                            int dataLength = din.readInt();
                            byte[] data = new byte[dataLength];
                            din.readFully(data, 0, dataLength);
                            byte leftLeafType = data[0];
                            if (leftLeafType == Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER) {
                                this.storeReceivedFiles(data);
                            } else {
                                System.err.println("ERROR : Waiting for message type " + Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER + " but get " + type);
                            }
                            din.close();
                        }
                    }
                    //send ACK to DN
                    System.out.println("INFO : Sending ACK to Discovery node.");
                    PeerSendsAckToDN ackToDN = new PeerSendsAckToDN();
                    ackToDN.setId(id);
                    ackToDN.setIpPort(ownIp + Constants.DELIMITORS.IP_PORT_DELIM + listeningPort);
                    ackToDN.setNickname(hostname);
                    TCPSender ackSender = new TCPSender(new Socket(discoveryNodeIp, discoveryNodeListeningPort));
                    ackSender.sendData(ackToDN.getBytes());
                    System.out.println("INFO : Completed Registration");
                    break;
                case Constants.MESSAGES.PEER_SENDS_UPDATE_LEAF:
                    PeerSendsUpdateLeaf updateLeaf = new PeerSendsUpdateLeaf(bytes);
                    char direction = updateLeaf.getDirection();
                    int leafInfoLen = updateLeaf.getLeafInfoLen();
                    String sendingPeerId = updateLeaf.getSendingPeerId();
                    if (leafInfoLen > 0) {
                        PeerInfo leafInfo = updateLeaf.getLeafInfo();
//                        logger.log(Level.INFO, "Receieved {0} leaf update from : {1}", new Object[]{direction, leafInfo.getId()});
                        System.out.println("INFO : Received " + direction + " leaf update from " + sendingPeerId);
                        synchronized (leafNodes) {
//                            System.out.println("INFO : Adding entry in leaf node : " + direction + Constants.DELIMITORS.TRACEPATH_DELIM + leafInfo.getId());
                            leafNodes.put(direction, leafInfo);
                        }
                    } else {
                        //Set leaf to null
                        synchronized (leafNodes) {
                            leafNodes.put(direction, null);
                        }
                    }
                    System.out.println("INFO : Leaf nodes : ");
                    synchronized (leafNodes) {
                        for (Map.Entry<Character, PeerInfo> entrySet : leafNodes.entrySet()) {
                            Character dir = entrySet.getKey();
                            PeerInfo leafInfo = entrySet.getValue();
//                            logger.log(Level.INFO, "{0} = {1}", new Object[]{dir, leafInfo.getId()});
                            if (leafInfo != null) {
                                System.out.println(dir + " = " + leafInfo.getId());
                            } else {
                                System.out.println(dir + " = null");
                            }
                        }
                    }
                    break;
                case Constants.MESSAGES.PEER_SENDS_UPDATE_RT:
                    PeerSendsUpdateRT updateRT = new PeerSendsUpdateRT(bytes);
                    int leafLen = updateRT.getLeafInfoLen();
                    if (leafLen > 0) {
                        PeerInfo leafInfo = updateRT.getLeafInfo();
                        String newlyAddedPeerId = leafInfo.getId();
//                        logger.log(Level.INFO, "Received RT update from : {0}", newlyAddedPeerId);
                        System.out.println("INFO : Received RT update from : " + newlyAddedPeerId);
                        //update routing table for this entry
                        int prefixToupdate = SystemFunctionUtil.matchPrefix(newlyAddedPeerId, id);
                        synchronized (routingTable) {
//                            for (int i = 0; i <= prefixToupdate; i++) {
                            Map<Character, Map<String, PeerInfo>> unmatchedRow = routingTable.get(prefixToupdate);
                            Map<String, PeerInfo> peerHandle = unmatchedRow.get(newlyAddedPeerId.charAt(prefixToupdate));
//                            if (peerHandle == null || peerHandle.isEmpty()) {
                            //appropriate cell is null or empty, so replace with current RT's entry
//                                logger.log(Level.INFO, "Updating {0} row and {1} column", new Object[]{prefixToupdate, newlyAddedPeerId.charAt(prefixToupdate)});
//                                System.out.println("INFO : Updating " + prefixToupdate + "th row and " + newlyAddedPeerId.charAt(prefixToupdate) + "th column");
                            peerHandle = new HashMap<>();
                            peerHandle.put(newlyAddedPeerId, leafInfo);
                            unmatchedRow.put(newlyAddedPeerId.charAt(prefixToupdate), peerHandle);
//                            }
                        }

                        System.out.println("INFO : Routing table after update from : " + newlyAddedPeerId);
                        synchronized (routingTable) {
                            for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                                Integer row = entrySet.getKey();
                                Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
                                System.out.println(row + " = " + columns);
                            }
                        }
                    }
                    break;
                case Constants.MESSAGES.PEER_REQUESTS_FILES_WHILE_JOINING:
                    PeerRequestsFilesWhileJoining fileReq = new PeerRequestsFilesWhileJoining(bytes);
                    String newPeer = fileReq.getId();
                    String newPeerIp = fileReq.getIp();
                    int newPeerPort = fileReq.getPort();
                    System.out.println("INFO : Received file transfer requests from newly added peer : " + newPeer);
                    LeafSendsFileToNewPeer filesToTransfer = new LeafSendsFileToNewPeer();
                    filesToTransfer.setLeafId(id);
                    List<FileDetailsToSend> fileDetailsToSend = new ArrayList<>();
                    synchronized (fileDetails) {
                        if (!fileDetails.isEmpty()) {
                            for (Map.Entry<String, String> entrySet : fileDetails.entrySet()) {
                                String fileKey = entrySet.getKey();
                                String filePath = entrySet.getValue();
                                DistanceDirection currentState = SystemFunctionUtil.getDistanceAndDirection(fileKey, id);
                                DistanceDirection nextState = SystemFunctionUtil.getDistanceAndDirection(fileKey, newPeer);
                                if (currentState.getDistance() > nextState.getDistance()) {
                                    //transfer file
                                    FileDetailsToSend fileToTransfer = new FileDetailsToSend();
                                    fileToTransfer.setFileKey(fileKey);
                                    fileToTransfer.setFilepath(filePath);
                                    fileDetailsToSend.add(fileToTransfer);
                                } else if (currentState.getDistance() == nextState.getDistance()) {
                                    if (Integer.parseInt(newPeer, 16) > Integer.parseInt(id, 16)) {
                                        //transfer file
                                        FileDetailsToSend fileToTransfer = new FileDetailsToSend();
                                        fileToTransfer.setFileKey(fileKey);
                                        fileToTransfer.setFilepath(filePath);
                                        fileDetailsToSend.add(fileToTransfer);
                                    }
                                }
                            }
                        }
                    }
                    filesToTransfer.setNoOfFiles(fileDetailsToSend.size());
                    filesToTransfer.setFileDetailsToSend(fileDetailsToSend);
                    for (FileDetailsToSend filepathsToTransfer1 : fileDetailsToSend) {
//                            filepathsToTransfer1.setLeafId(id);
                        File file = new File(filepathsToTransfer1.getFilepath());
//                        TCPSender fileSender = new TCPSender(new Socket(newPeerIp, newPeerPort));
                        try (FileInputStream fin = new FileInputStream(file)) {
                            int fileLength = (int) file.length();
                            if (fileLength > 0) {
                                byte[] data = new byte[fileLength];
                                fin.read(data);
                                filepathsToTransfer1.setDataLen(fileLength);
                                filepathsToTransfer1.setData(data);
                                System.out.println("INFO : Sending " + filepathsToTransfer1.getFilepath() + " with key : " + filepathsToTransfer1.getFileKey() + " to " + newPeer);
//                                fileSender.sendData(filepathsToTransfer1.getBytes());
                                synchronized (fileDetails) {
                                    fileDetails.remove(filepathsToTransfer1.getFileKey());
                                }
                            }
                            fin.close();
                        }
                    }
                    TCPSender fileSender = new TCPSender(socket);
                    fileSender.sendData(filesToTransfer.getBytes());
//                    if (!fileDetailsToSend.isEmpty()) {
//                        for (FileDetailsToSend filepathsToTransfer1 : fileDetailsToSend) {
////                            filepathsToTransfer1.setLeafId(id);
//                            File file = new File(filepathsToTransfer1.getFilepath());
//                            TCPSender fileSender = new TCPSender(new Socket(newPeerIp, newPeerPort));
//                            try (FileInputStream fin = new FileInputStream(file)) {
//                                int fileLength = (int) file.length();
//                                if (fileLength > 0) {
//                                    byte[] data = new byte[fileLength];
//                                    fin.read(data);
//                                    filepathsToTransfer1.setDataLen(fileLength);
//                                    filepathsToTransfer1.setData(data);
//                                    System.out.println("INFO : Sending " + filepathsToTransfer1.getFilepath() + " with key : " + filepathsToTransfer1.getFileKey() + " to " + newPeer);
//                                    fileSender.sendData(filepathsToTransfer1.getBytes());
//                                    synchronized (fileDetails) {
//                                        fileDetails.remove(filepathsToTransfer1.getFileKey());
//                                    }
//                                }
//                                fin.close();
//                            }
//                        }
//                    } else {
//                        LeafSendsFileToNewPeer leafSendsFileToNewPeer = new LeafSendsFileToNewPeer();
//                        leafSendsFileToNewPeer.setLeafId(id);
//                        leafSendsFileToNewPeer.setNoOfFiles(0);
//                        TCPSender fileSender = new TCPSender(new Socket(newPeerIp, newPeerPort));
//                        fileSender.sendData(leafSendsFileToNewPeer.getBytes());
//                    }
                    break;
//                case Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER:
//                    LeafSendsFileToNewPeer filesReceived = new LeafSendsFileToNewPeer(bytes);
//                    String leafId = filesReceived.getLeafId();
//                    int noOfFilesRcvd = filesReceived.getNoOfFiles();
////                    if(noOfFilesRcvd > 0){
//                    if (noOfFilesRcvd > 0) {
//                        List<FileDetailsToSend> fileDetailsToStore = filesReceived.getFileDetailsToSend();
//                        for (FileDetailsToSend fileDetailToStore : fileDetailsToStore) {
//                            String fileKeyRcvd = fileDetailToStore.getFileKey();
//                            String filepathRcvd = fileDetailToStore.getFilepath();
//                            System.out.println("INFO : " + filepathRcvd + " with " + fileKeyRcvd + " key received from " + leafId);
//                            byte[] data = fileDetailToStore.getData();
//                            String folders = filepathRcvd.substring(0, filepathRcvd.lastIndexOf("/"));
//                            File foldersObj = new File(folders);
//                            if (!foldersObj.exists()) {
//                                foldersObj.mkdirs();
//                            }
//                            File f = new File(filepathRcvd);
//                            try (FileOutputStream fOut = new FileOutputStream(f)) {
//                                fOut.write(data);
//                                fOut.close();
//                                synchronized (fileDetails) {
//                                    fileDetails.put(fileKeyRcvd, filepathRcvd);
//                                }
//                            } catch (IOException ex) {
//                                System.err.println("ERROR : Writing data to file");
//                            }
//                        }
//                    } else {
//                        System.out.println("INFO : No files received from " + leafId);
//                    }
//                    break;
                case Constants.MESSAGES.LOOKUP_DESTINATION_FOR_FILE_KEY_STORE:
                    LookupDestinationForFileKey lookup = new LookupDestinationForFileKey(bytes);
                    char operation = lookup.getOperation();
                    String clientIp = lookup.getClientIp();
                    int clientPort = lookup.getClientPort();
                    String fileKey = lookup.getFileKey();
                    String filepath = lookup.getFilepath();
                    String lastPeerId1 = lookup.getLastPeerId();
                    int hopCountLookup = lookup.getHopCount();
                    hopCountLookup += 1;
                    String tracepathLookup = lookup.getTracepath();
                    if (lastPeerId1.equals(Constants.NO_IDS_REGISTERED_STRING)) {
                        System.out.println("INFO : Received lookup request from client for key : " + fileKey);
                        tracepathLookup = id;
                    } else {
                        System.out.println("INFO : Received lookup request from peer : " + lastPeerId1 + " for key : " + fileKey);
                        tracepathLookup += Constants.DELIMITORS.TRACEPATH_DELIM + id;
                    }
                    //check whether this peer is destination or not
                    if (!fileKey.equals(id)) {
                        //check whether new peer is in between leaf nodes and self.
                        PeerInfo leftLeafForLookup;
                        PeerInfo rightLeafForLookup;
                        synchronized (leafNodes) {
                            leftLeafForLookup = leafNodes.get(Constants.DIRECTION.LEFT);
                            rightLeafForLookup = leafNodes.get(Constants.DIRECTION.RIGHT);
                        }
                        String nextPeerForLookup = "";
                        PeerInfo nextPeerInfoForLookup = null;
                        if (leftLeafForLookup != null) {
                            //more than one peer are in pastry
                            //find next peer
                            nextPeerInfoForLookup = findNextPeer(fileKey, leftLeafForLookup, rightLeafForLookup);
                            nextPeerForLookup = nextPeerInfoForLookup.getId();
                            //send request to next peer if its not self.
                            //If nextPeer is self, then self is destination
                            if (nextPeerForLookup.equals(id)) {
                                if (operation == Constants.OPERATION.STORE) {
                                    System.out.println("INFO : " + id + " is destination for storing key : " + fileKey);
                                    //send lookup response to client
                                    DestinationSendsLookupResponseForStore lookupResponse = new DestinationSendsLookupResponseForStore();
                                    lookupResponse.setDestId(id);
                                    lookupResponse.setDestIp(ownIp);
                                    lookupResponse.setDestPort(listeningPort);
                                    lookupResponse.setFileKey(fileKey);
                                    lookupResponse.setFilepath(filepath);
                                    lookupResponse.setHopCount(hopCountLookup);
                                    lookupResponse.setTracepath(tracepathLookup);
                                    System.out.println("INFO : Sending lookup resonse to client.");
                                    TCPSender lookupResponseSender = new TCPSender(new Socket(clientIp, clientPort));
                                    lookupResponseSender.sendData(lookupResponse.getBytes());
                                } else if (operation == Constants.OPERATION.RETRIEVE) {
                                    //confirm destination by checking filekeys and respond
                                    DestinationSendsLookupResponseForRetrieve lookupResponseRetrieve = new DestinationSendsLookupResponseForRetrieve();
                                    lookupResponseRetrieve.setDestId(id);
                                    lookupResponseRetrieve.setFileKey(fileKey);
                                    lookupResponseRetrieve.setHopCount(hopCountLookup);
                                    lookupResponseRetrieve.setTracepath(tracepathLookup);
                                    String filePath = "";
                                    synchronized (fileDetails) {
                                        filePath = fileDetails.get(fileKey);
                                    }
                                    if (filePath != null && !filePath.isEmpty()) {
                                        File fileToRead = new File(filePath);
                                        if (fileToRead.exists()) {
                                            FileInputStream fin = new FileInputStream(fileToRead);
                                            int fileLen = (int) fileToRead.length();
                                            byte[] filedata = new byte[fileLen];
                                            fin.read(filedata);
                                            lookupResponseRetrieve.setFilePath(filePath);
                                            lookupResponseRetrieve.setDataLen(fileLen);
                                            lookupResponseRetrieve.setData(filedata);
                                        } else {
                                            System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                            lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                            lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                                            //remove file from map
                                            synchronized (fileDetails) {
                                                if (fileDetails.containsKey(fileKey)) {
                                                    fileDetails.remove(fileKey);
                                                }
                                            }
                                        }
                                    } else {
                                        //file does not exist
                                        System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                        lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                        lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                                    }
                                    TCPSender retrieveFileSender = new TCPSender(new Socket(clientIp, clientPort));
                                    retrieveFileSender.sendData(lookupResponseRetrieve.getBytes());
                                }
                            } else {
                                //forward request to closest peer
                                LookupDestinationForFileKey lookupForward = new LookupDestinationForFileKey();
                                lookupForward.setOperation(operation);
                                lookupForward.setClientIp(clientIp);
                                lookupForward.setClientPort(clientPort);
                                lookupForward.setFileKey(fileKey);
                                lookupForward.setFilepath(filepath);
                                lookupForward.setLastPeerId(id);
                                lookupForward.setHopCount(hopCountLookup);
                                lookupForward.setTracepath(tracepathLookup);
                                System.out.println("INFO : Forwarding lookup request for :" + fileKey + " to : " + nextPeerForLookup);
                                TCPSender lookupForwardSender = new TCPSender(new Socket(nextPeerInfoForLookup.getIp(), nextPeerInfoForLookup.getPort()));
                                lookupForwardSender.sendData(lookupForward.getBytes());
                            }
                        } else {
                            if (operation == Constants.OPERATION.STORE) {
                                //This peer is only one, send lookup response to client
                                System.out.println("INFO : " + id + " is only peer in pastry so it's the destination for storing key : " + fileKey);
                                DestinationSendsLookupResponseForStore lookupResponse = new DestinationSendsLookupResponseForStore();
                                lookupResponse.setDestId(id);
                                lookupResponse.setDestIp(ownIp);
                                lookupResponse.setDestPort(listeningPort);
                                lookupResponse.setFileKey(fileKey);
                                lookupResponse.setFilepath(filepath);
                                lookupResponse.setHopCount(hopCountLookup);
                                lookupResponse.setTracepath(tracepathLookup);
                                System.out.println("INFO : Sending lookup resonse to client.");
                                TCPSender lookupResponseSender = new TCPSender(new Socket(clientIp, clientPort));
                                lookupResponseSender.sendData(lookupResponse.getBytes());
                            } else if (operation == Constants.OPERATION.RETRIEVE) {
                                //confirm destination by checking filekeys and respond
                                DestinationSendsLookupResponseForRetrieve lookupResponseRetrieve = new DestinationSendsLookupResponseForRetrieve();
                                lookupResponseRetrieve.setDestId(id);
                                lookupResponseRetrieve.setFileKey(fileKey);
                                lookupResponseRetrieve.setHopCount(hopCountLookup);
                                lookupResponseRetrieve.setTracepath(tracepathLookup);
                                String filePath = "";
                                synchronized (fileDetails) {
                                    filePath = fileDetails.get(fileKey);
                                }
                                if (filePath != null && !filePath.isEmpty()) {
                                    File fileToRead = new File(filePath);
                                    if (fileToRead.exists()) {
                                        FileInputStream fin = new FileInputStream(fileToRead);
                                        int fileLen = (int) fileToRead.length();
                                        byte[] filedata = new byte[fileLen];
                                        fin.read(filedata);
                                        lookupResponseRetrieve.setFilePath(filePath);
                                        lookupResponseRetrieve.setDataLen(fileLen);
                                        lookupResponseRetrieve.setData(filedata);
                                    } else {
                                        System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                        lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                        lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                                        //remove file from map
                                        synchronized (fileDetails) {
                                            if (fileDetails.containsKey(fileKey)) {
                                                fileDetails.remove(fileKey);
                                            }
                                        }
                                    }
                                } else {
                                    //file does not exist
                                    System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                    lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                    lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                                }
                                TCPSender retrieveFileSender = new TCPSender(new Socket(clientIp, clientPort));
                                retrieveFileSender.sendData(lookupResponseRetrieve.getBytes());
                            }
                        }
                    } else {
                        System.out.println("INFO : Filekey " + fileKey + " is same as peer Id : " + id);
                        if (operation == Constants.OPERATION.STORE) {
                            //filekey is same as ownId, so send lookup response to client                            
                            DestinationSendsLookupResponseForStore lookupResponse = new DestinationSendsLookupResponseForStore();
                            lookupResponse.setDestId(id);
                            lookupResponse.setDestIp(ownIp);
                            lookupResponse.setDestPort(listeningPort);
                            lookupResponse.setFileKey(fileKey);
                            lookupResponse.setFilepath(filepath);
                            lookupResponse.setHopCount(hopCountLookup);
                            lookupResponse.setTracepath(tracepathLookup);
                            System.out.println("INFO : Sending lookup resonse to client.");
                            TCPSender lookupresponseSender = new TCPSender(new Socket(clientIp, clientPort));
                            lookupresponseSender.sendData(lookupResponse.getBytes());
                        } else if (operation == Constants.OPERATION.RETRIEVE) {
                            //confirm destination by checking filekeys and respond
                            DestinationSendsLookupResponseForRetrieve lookupResponseRetrieve = new DestinationSendsLookupResponseForRetrieve();
                            lookupResponseRetrieve.setDestId(id);
                            lookupResponseRetrieve.setFileKey(fileKey);
                            lookupResponseRetrieve.setHopCount(hopCountLookup);
                            lookupResponseRetrieve.setTracepath(tracepathLookup);
                            String filePath = "";
                            synchronized (fileDetails) {
                                filePath = fileDetails.get(fileKey);
                            }
                            if (filePath != null && !filePath.isEmpty()) {
                                File fileToRead = new File(filePath);
                                if (fileToRead.exists()) {
                                    FileInputStream fin = new FileInputStream(fileToRead);
                                    int fileLen = (int) fileToRead.length();
                                    byte[] filedata = new byte[fileLen];
                                    fin.read(filedata);
                                    lookupResponseRetrieve.setFilePath(filePath);
                                    lookupResponseRetrieve.setDataLen(fileLen);
                                    lookupResponseRetrieve.setData(filedata);
                                } else {
                                    System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                    lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                    lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                                    //remove file from map
                                    synchronized (fileDetails) {
                                        if (fileDetails.containsKey(fileKey)) {
                                            fileDetails.remove(fileKey);
                                        }
                                    }
                                }
                            } else {
                                //file does not exist
                                System.out.println("INFO : " + filePath + " does not exist, responding back to client.");
                                lookupResponseRetrieve.setFilePath(Constants.NO_IDS_REGISTERED_STRING);
                                lookupResponseRetrieve.setDataLen(Constants.NO_IDS_REGISTERED_INT);
                            }
                            TCPSender retrieveFileSender = new TCPSender(new Socket(clientIp, clientPort));
                            retrieveFileSender.sendData(lookupResponseRetrieve.getBytes());
                        }
                    }
                    break;
                case Constants.MESSAGES.STORE_FILE:
                    StoreFile storeFile = new StoreFile(bytes);
                    String fileKeyToStore = storeFile.getFileKey();
                    int dataLen = storeFile.getDataLen();
                    String filePath = storeFile.getFilePath();
                    byte[] filedata = storeFile.getData();
                    System.out.println("INFO : Store file request from client with file key : " + fileKeyToStore);
                    String completeFilePath = Constants.FILE_STORE_LOCATION + filePath;
                    String folders = completeFilePath.substring(0, completeFilePath.lastIndexOf("/"));
                    File foldersObj = new File(folders);
                    if (!foldersObj.exists()) {
                        foldersObj.mkdirs();
                    }
                    File f = new File(completeFilePath);
                    try (FileOutputStream fOut = new FileOutputStream(f)) {
                        fOut.write(filedata);
                        fOut.close();
                        synchronized (fileDetails) {
                            fileDetails.put(fileKeyToStore, completeFilePath);
                        }
                    } catch (IOException ex) {
                        System.err.println("ERROR : Writing data to file");
                    }
                    break;
                case Constants.MESSAGES.STORE_FILE_FROM_LEAVING_PEER:
                    StoreFileFromLeavingPeer storeFileFromLeavingPeer = new StoreFileFromLeavingPeer(bytes);
                    String fileKey1 = storeFileFromLeavingPeer.getFileKey();
                    int dataLen1 = storeFileFromLeavingPeer.getDataLen();
                    String filepath1 = storeFileFromLeavingPeer.getFilepath();
                    String leavingPeerId = storeFileFromLeavingPeer.getLeavingPeerId();
                    if (dataLen1 > 0 && !fileKey1.equals(Constants.NO_IDS_REGISTERED_STRING) && !filepath1.equals(Constants.NO_IDS_REGISTERED_STRING)) {
                        byte[] data = storeFileFromLeavingPeer.getData();
                        System.out.println("INFO : Reeived " + filepath1 + " with key " + fileKey1 + " from leaving peer " + leavingPeerId);
                        String foldersToCreate = filepath1.substring(0, filepath1.lastIndexOf("/"));
                        File foldersObjToCreate = new File(foldersToCreate);
                        if (!foldersObjToCreate.exists()) {
                            foldersObjToCreate.mkdirs();
                        }
                        File file = new File(filepath1);
                        try (FileOutputStream fOut = new FileOutputStream(file)) {
                            fOut.write(data);
                            fOut.close();
                            synchronized (fileDetails) {
                                fileDetails.put(fileKey1, filepath1);
                            }
                        } catch (IOException ex) {
                            System.err.println("ERROR : Writing data to file");
                        }
                    } else {
                        System.err.println("ERROR : Invalid file OR file is removed");
                    }
                    break;
                default:
                    logger.log(Level.WARNING, "Default case : {0}", new String(bytes));

            }
        } catch (IOException ex) {
            logger.severe("Sending update message.");
        }
    }

//    public PeerInfo findNextPeer(String newPeerId, PeerInfo leftLeaf, PeerInfo rightLeaf) {
//        PeerInfo nextPeerInfo = selfInfo;
//        //Distance between self and newPeerId referencing from self
//        DistanceDirection distanceAndDirectionFromSelf = SystemFunctionUtil.getDistanceAndDirection(id, newPeerId);
//        if (!leftLeaf.getId().equals(rightLeaf.getId())) {
//            //left and right leaves are different
//            //Distance between left leaf node and newPeerId referencing from leftleaf
//            DistanceDirection distanceAndDirectionFromLeft = SystemFunctionUtil.getDistanceAndDirection(leftLeaf.getId(), newPeerId);
//            //Distance between right leaf node and newPeerId referencing from rightleaf
//            DistanceDirection distanceAndDirectionFromRight = SystemFunctionUtil.getDistanceAndDirection(rightLeaf.getId(), newPeerId);
//            if (distanceAndDirectionFromLeft.getDirection() == Constants.DIRECTION.RIGHT && distanceAndDirectionFromSelf.getDirection() == Constants.DIRECTION.LEFT) {
//                //new peer is between left leaf and self
//                //check the closest
//                if (distanceAndDirectionFromLeft.getDistance() < distanceAndDirectionFromSelf.getDistance()) {
//                    //left leaf is closest
////                    nextPeer = leftLeaf.getId();
//                    nextPeerInfo = leftLeaf;
//                } else {
//                    //self is closest
////                    nextPeer = id;
//                    nextPeerInfo = selfInfo;
//                }
//                System.out.println("INFO : Found between left leaf and self.");
//            } else if (distanceAndDirectionFromRight.getDirection() == Constants.DIRECTION.LEFT && distanceAndDirectionFromSelf.getDirection() == Constants.DIRECTION.RIGHT) {
//                //new peer is between right leaf and self
//                //check the closest
//                if (distanceAndDirectionFromSelf.getDistance() < distanceAndDirectionFromRight.getDistance()) {
//                    //self is closest
////                    nextPeer = id;
//                    nextPeerInfo = selfInfo;
//                } else {
//                    //right leaf is closest
////                    nextPeer = rightLeaf.getId();
//                    nextPeerInfo = rightLeaf;
//                }
//                System.out.println("INFO : Found between right leaf and self.");
//            } else {
//                //Not in between leaves and self    
//                System.out.println("INFO : Not between leaves, so finding in routing table and leaves.");
//                int matchedPrefix = SystemFunctionUtil.matchPrefix(newPeerId, id);
//                char unmatchedLetter = newPeerId.charAt(matchedPrefix);
//                //Select next peer from routing table and leaf nodes
//                int minDistance = distanceAndDirectionFromSelf.getDistance();
//                synchronized (routingTable) {
//                    Map<Character, Map<String, PeerInfo>> row = routingTable.get(matchedPrefix);
//                    if (row != null && !row.isEmpty()) {
//                        Map<String, PeerInfo> nextHop = row.get(unmatchedLetter);
//                        if (nextHop == null || nextHop.isEmpty()) {
//                            //No peer found at that cell, check every cell from routing table to select closest
//                            for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
//                                Integer selfRow = entrySet.getKey();
//                                Map<Character, Map<String, PeerInfo>> rowColumn = entrySet.getValue();
//                                if (rowColumn != null && !rowColumn.isEmpty()) {
//                                    for (Map.Entry<Character, Map<String, PeerInfo>> entrySet1 : rowColumn.entrySet()) {
//                                        Character column = entrySet1.getKey();
//                                        Map<String, PeerInfo> peerHandle = entrySet1.getValue();
//                                        if (peerHandle != null && !peerHandle.isEmpty()) {
//                                            for (Map.Entry<String, PeerInfo> entrySet2 : peerHandle.entrySet()) {
//                                                String handleId = entrySet2.getKey();
//                                                PeerInfo handlePeerInfo = entrySet2.getValue();
//                                                DistanceDirection distanceAndDirection = SystemFunctionUtil.getDistanceAndDirection(newPeerId, handleId);
//                                                if (distanceAndDirection.getDistance() < minDistance) {
//                                                    minDistance = distanceAndDirection.getDistance();
////                                                    nextPeer = handleId;
//                                                    nextPeerInfo = handlePeerInfo;
//                                                } else if (distanceAndDirection.getDistance() == minDistance) {
//                                                    if (Integer.parseInt(handleId, 16) > Integer.parseInt(nextPeerInfo.getId(), 16)) {
////                                                        nextPeer = handleId;
//                                                        nextPeerInfo = handlePeerInfo;
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        } else {
//                            //set next peer
//                            String nextPeer = (String) new ArrayList(nextHop.keySet()).get(0);
//                            nextPeerInfo = nextHop.get(nextPeer);
//                            return nextPeerInfo;
//                        }
//                    }
//                }
//                //check leaf nodes
//                synchronized (leafNodes) {
//                    if (leafNodes != null && !leafNodes.isEmpty()) {
//                        for (Map.Entry<Character, PeerInfo> entrySet : leafNodes.entrySet()) {
//                            Character direction = entrySet.getKey();
//                            PeerInfo leafNodeInfo = entrySet.getValue();
//                            String leafNodeId = leafNodeInfo.getId();
//                            DistanceDirection distanceAndDirection = SystemFunctionUtil.getDistanceAndDirection(newPeerId, leafNodeId);
//                            if (distanceAndDirection.getDistance() < minDistance) {
//                                minDistance = distanceAndDirection.getDistance();
////                                nextPeer = leafNodeId;
//                                nextPeerInfo = leafNodeInfo;
//                            } else if (distanceAndDirection.getDistance() == minDistance) {
//                                if (Integer.parseInt(leafNodeId, 16) > Integer.parseInt(nextPeerInfo.getId(), 16)) {
////                                    nextPeer = leafNodeId;
//                                    nextPeerInfo = leafNodeInfo;
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        } else {
//            //left leaf and right leaf are same, so only two peers are in pastry excluding newly added peer
//            //Distance between right leaf node and newPeerId referencing from rightleaf
//            DistanceDirection distanceAndDirectionFromRight = SystemFunctionUtil.getDistanceAndDirection(rightLeaf.getId(), newPeerId);
//            if (distanceAndDirectionFromRight.getDistance() < distanceAndDirectionFromSelf.getDistance()) {
//                //right/left leaf is closest, so its the destination
////                nextPeer = rightLeaf.getId();
//                nextPeerInfo = rightLeaf;
//            } else if (distanceAndDirectionFromRight.getDistance() > distanceAndDirectionFromSelf.getDistance()) {
//                //self is closest, so its the destination
////                nextPeer = id;
//                nextPeerInfo = selfInfo;
//            } else {
//                //both are same, so select the peer which is right side
//                if (distanceAndDirectionFromRight.getDirection() == Constants.DIRECTION.RIGHT) {
////                    nextPeer = rightLeaf.getId();
//                    nextPeerInfo = rightLeaf;
//                } else {
////                    nextPeer = id;
//                    nextPeerInfo = selfInfo;
//                }
//            }
//        }
//        return nextPeerInfo;
//    }
    public PeerInfo findNextPeer(String newPeerId, PeerInfo leftLeaf, PeerInfo rightLeaf) {
//        System.out.println("-Fine next peer called-");
        PeerInfo nextPeerInfo = selfInfo;
        if (newPeerId.equals(id)) {
            nextPeerInfo = selfInfo;
        } else {
            //Distance between self and newPeerId referencing from self
            DistanceDirection distanceAndDirectionFromSelf = SystemFunctionUtil.getDistanceAndDirection(id, newPeerId);
            String leftId = leftLeaf.getId();
            String rightId = rightLeaf.getId();
            if (!leftLeaf.getId().equals(rightLeaf.getId())) {
                //left and right leaves are different
                if (leftId.equals(newPeerId)) {
                    nextPeerInfo = leftLeaf;
                } else if (rightId.equals(newPeerId)) {
                    nextPeerInfo = rightLeaf;
                } else {
                    //Clockwise Distance between left leaf node and newPeerId
                    int clockwiseDistanceFromLeftToNew = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.RIGHT, leftId, newPeerId);
//            System.out.println("-clockwiseDistanceFromLeftToNew-" + clockwiseDistanceFromLeftToNew);
                    //AntiClockwise Distance between left leaf node and newPeerId
                    int antiClockwiseDistanceFromSelfToLeft = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.LEFT, id, leftId);
//            System.out.println("-antiClockwiseDistanceFromSelfToLeft-" + antiClockwiseDistanceFromSelfToLeft);
                    //Clockwise Distance between left leaf node and newPeerId
                    int clockwiseDistanceFromSelfToNew = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.RIGHT, id, newPeerId);
//            System.out.println("-clockwiseDistanceFromSelfToNew-" + clockwiseDistanceFromSelfToNew);
                    //AntiClockwise Distance between left leaf node and newPeerId
                    int antiClockwiseDistanceFromRightToSelf = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.LEFT, rightId, id);
//            System.out.println("-antiClockwiseDistanceFromRightToSelf-" + antiClockwiseDistanceFromRightToSelf);
                    //Distance between right leaf node and newPeerId referencing from rightleaf
//            DistanceDirection distanceAndDirectionFromRight = SystemFunctionUtil.getDistanceAndDirection(rightLeaf.getId(), newPeerId);
                    if (clockwiseDistanceFromLeftToNew < antiClockwiseDistanceFromSelfToLeft) {
                        //new peer is between left leaf and self
                        //check the closest
                        int antiClockwiseDistanceFromSelfToNew = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.LEFT, id, newPeerId);
                        if (clockwiseDistanceFromLeftToNew < antiClockwiseDistanceFromSelfToNew) {
                            //left leaf is closest
//                    nextPeer = leftLeaf.getId();
                            nextPeerInfo = leftLeaf;
                        } else if (clockwiseDistanceFromLeftToNew > antiClockwiseDistanceFromSelfToNew) {
                            //self is closest
//                    nextPeer = id;
                            nextPeerInfo = selfInfo;
                        } else {
                            //Distance from both are same so store to node with higher id
                            if (Integer.parseInt(id, 16) > Integer.parseInt(leftId, 16)) {
                                //self id is higher then left
                                nextPeerInfo = selfInfo;
                            } else {
                                //Left leaf's id is higher
                                nextPeerInfo = leftLeaf;
                            }
                        }
                        System.out.println("INFO : Found between left leaf and self.");
                    } else if (clockwiseDistanceFromSelfToNew < antiClockwiseDistanceFromRightToSelf) {
                        //new peer is between right leaf and self
                        //check the closest
                        int antiClockwiseDistanceFromRightToNew = SystemFunctionUtil.countDistanceInDirection(Constants.DIRECTION.LEFT, rightId, newPeerId);
                        if (clockwiseDistanceFromSelfToNew < antiClockwiseDistanceFromRightToNew) {
                            //self is closest
//                    nextPeer = id;
                            nextPeerInfo = selfInfo;
                        } else if (clockwiseDistanceFromSelfToNew > antiClockwiseDistanceFromRightToNew) {
                            //right leaf is closest
//                    nextPeer = rightLeaf.getId();
                            nextPeerInfo = rightLeaf;
                        } else {
                            //Both are at same distance so select peer with higher id
                            if (Integer.parseInt(id, 16) > Integer.parseInt(rightId, 16)) {
                                //self id is higher then left
                                nextPeerInfo = selfInfo;
                            } else {
                                //Left leaf's id is higher
                                nextPeerInfo = rightLeaf;
                            }
                        }
                        System.out.println("INFO : Found between right leaf and self.");
                    } else {
                        //Not in between leaves and self    
                        System.out.println("INFO : Not between leaves, so finding in routing table and leaves.");
                        int matchedPrefix = SystemFunctionUtil.matchPrefix(newPeerId, id);
                        char unmatchedLetter = newPeerId.charAt(matchedPrefix);
                        //Select next peer from routing table and leaf nodes
                        int minDistance = distanceAndDirectionFromSelf.getDistance();
                        boolean foundAtCell = false;
                        synchronized (routingTable) {
                            Map<Character, Map<String, PeerInfo>> row = routingTable.get(matchedPrefix);
                            if (row != null && !row.isEmpty()) {
                                Map<String, PeerInfo> nextHop = row.get(unmatchedLetter);
                                if (nextHop == null || nextHop.isEmpty()) {
                                    //No peer found at that cell, check every cell from routing table to select closest
//                                    System.out.println("INFO : No peer found at cell : " + matchedPrefix + ":" + unmatchedLetter);
                                    for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                                        Integer selfRow = entrySet.getKey();
                                        Map<Character, Map<String, PeerInfo>> rowColumn = entrySet.getValue();
                                        if (rowColumn != null && !rowColumn.isEmpty()) {
                                            for (Map.Entry<Character, Map<String, PeerInfo>> entrySet1 : rowColumn.entrySet()) {
                                                Character column = entrySet1.getKey();
                                                Map<String, PeerInfo> peerHandle = entrySet1.getValue();
                                                if (peerHandle != null && !peerHandle.isEmpty()) {
                                                    for (Map.Entry<String, PeerInfo> entrySet2 : peerHandle.entrySet()) {
                                                        String handleId = entrySet2.getKey();
                                                        PeerInfo handlePeerInfo = entrySet2.getValue();
                                                        DistanceDirection distanceAndDirection = SystemFunctionUtil.getDistanceAndDirection(newPeerId, handleId);
                                                        if (distanceAndDirection.getDistance() < minDistance) {
                                                            minDistance = distanceAndDirection.getDistance();
//                                                    nextPeer = handleId;
                                                            nextPeerInfo = handlePeerInfo;
                                                        } else if (distanceAndDirection.getDistance() == minDistance) {
                                                            if (Integer.parseInt(handleId, 16) > Integer.parseInt(nextPeerInfo.getId(), 16)) {
//                                                        nextPeer = handleId;
                                                                nextPeerInfo = handlePeerInfo;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    //set next peer
                                    String nextPeer = (String) new ArrayList(nextHop.keySet()).get(0);
                                    nextPeerInfo = nextHop.get(nextPeer);
                                    foundAtCell = true;
//                                    return nextPeerInfo;
                                }
                            }
                        }
                        //check leaf nodes
                        if (!foundAtCell) {
                            synchronized (leafNodes) {
                                if (leafNodes != null && !leafNodes.isEmpty()) {
                                    for (Map.Entry<Character, PeerInfo> entrySet : leafNodes.entrySet()) {
                                        Character direction = entrySet.getKey();
                                        PeerInfo leafNodeInfo = entrySet.getValue();
                                        String leafNodeId = leafNodeInfo.getId();
                                        DistanceDirection distanceAndDirection = SystemFunctionUtil.getDistanceAndDirection(newPeerId, leafNodeId);
                                        if (distanceAndDirection.getDistance() < minDistance) {
                                            minDistance = distanceAndDirection.getDistance();
//                                nextPeer = leafNodeId;
                                            nextPeerInfo = leafNodeInfo;
                                        } else if (distanceAndDirection.getDistance() == minDistance) {
                                            if (Integer.parseInt(leafNodeId, 16) > Integer.parseInt(nextPeerInfo.getId(), 16)) {
//                                    nextPeer = leafNodeId;
                                                nextPeerInfo = leafNodeInfo;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                //left leaf and right leaf are same, so only two peers are in pastry excluding newly added peer
                if (rightId.equals(newPeerId)) {
                    nextPeerInfo = rightLeaf;
                } else {
                    //Distance between right leaf node and newPeerId referencing from rightleaf
                    DistanceDirection distanceAndDirectionFromRight = SystemFunctionUtil.getDistanceAndDirection(rightLeaf.getId(), newPeerId);
                    if (distanceAndDirectionFromRight.getDistance() < distanceAndDirectionFromSelf.getDistance()) {
                        //right/left leaf is closest, so its the destination
//                nextPeer = rightLeaf.getId();
                        nextPeerInfo = rightLeaf;
                    } else if (distanceAndDirectionFromRight.getDistance() > distanceAndDirectionFromSelf.getDistance()) {
                        //self is closest, so its the destination
//                nextPeer = id;
                        nextPeerInfo = selfInfo;
                    } else {
                        //both are same, so select the peer with higher id
                        if (Integer.parseInt(rightLeaf.getId(), 16) > Integer.parseInt(id, 16)) {
//                    nextPeer = rightLeaf.getId();
                            nextPeerInfo = rightLeaf;
                        } else {
//                    nextPeer = id;
                            nextPeerInfo = selfInfo;
                        }
                    }
                }
            }
//            System.out.println("INFO : Next peer - " + nextPeerInfo.getId() + "-" + nextPeerInfo.getIp() + ":" + nextPeerInfo.getPort());
            if (!nextPeerInfo.getId().equals(selfInfo.getId())) {
                try {
                    Socket tempSocket = new Socket(nextPeerInfo.getIp(), nextPeerInfo.getPort());
                    tempSocket.close();
                } catch (IOException ex) {
                    System.out.println("WARNING : Peer " + nextPeerInfo.getId() + " might be down, so finding new peer for key : " + newPeerId);
                    //Remove entry from routing table
                    synchronized (routingTable) {
                        boolean found = false;
                        for (Iterator<Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>>> it = routingTable.entrySet().iterator(); it.hasNext();) {
                            Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet = it.next();
                            Integer row = entrySet.getKey();
                            Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
                            for (Iterator<Map.Entry<Character, Map<String, PeerInfo>>> it1 = columns.entrySet().iterator(); it1.hasNext();) {
                                Map.Entry<Character, Map<String, PeerInfo>> entrySet1 = it1.next();
                                Character column = entrySet1.getKey();
                                Map<String, PeerInfo> peerHandle = entrySet1.getValue();
                                if (peerHandle != null && !peerHandle.isEmpty()) {
                                    String key = (String) new ArrayList(peerHandle.keySet()).get(0);
                                    if (key.equals(nextPeerInfo.getId())) {
                                        peerHandle.remove(nextPeerInfo.getId());
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if (found) {
                                break;
                            }
                        }
                    }
                    //request again for next peer
                    nextPeerInfo = this.findNextPeer(newPeerId, leftLeaf, rightLeaf);
                }
            }
        }
        return nextPeerInfo;
    }

    @Override
    public int retrieveListeningPort() {
        return listeningPort;
    }

    public void printSelfInfo() {
        System.out.println("ID : " + id);
        System.out.println("Listening on : " + ownIp + ":" + listeningPort);
//        System.out.println("INFO : Printing routing information");
        System.out.println("INFO : Leaf nodes");
        synchronized (leafNodes) {
            if (!leafNodes.isEmpty()) {
                for (Map.Entry<Character, PeerInfo> entrySet : leafNodes.entrySet()) {
                    Character direction = entrySet.getKey();
                    PeerInfo peerInfo = entrySet.getValue();
                    System.out.println(direction + " : " + peerInfo);
                }
            } else {
                System.out.println("INFO : No leaf nodes");
            }
        }
        System.out.println("INFO : Routing table");
        synchronized (routingTable) {
            for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                Integer row = entrySet.getKey();
                Map<Character, Map<String, PeerInfo>> columns = entrySet.getValue();
                System.out.println(row + " = " + columns);
            }
        }
    }

    public void listFiles() {
        System.out.println("INFO : Listing files ");
        synchronized (fileDetails) {
            if (!fileDetails.isEmpty()) {
                for (Map.Entry<String, String> entrySet : fileDetails.entrySet()) {
                    String fileKey = entrySet.getKey();
                    String filePath = entrySet.getValue();
                    System.out.println(fileKey + " : " + filePath);
                }
            } else {
                System.out.println("INFO : No files found.");
            }
        }
    }

    public void leavePastry() {
        try {
            System.out.println("INFO : Leaving pastry");
            //Send message to DN
            PeerLeavesPastryToDN peerLeavesPastryToDN = new PeerLeavesPastryToDN();
            peerLeavesPastryToDN.setId(id);
            TCPSender sender = new TCPSender(new Socket(discoveryNodeIp, discoveryNodeListeningPort));
            sender.sendData(peerLeavesPastryToDN.getBytes());
            //Tell leaves to update their leaf set
            PeerInfo leftLeaf;
            PeerInfo rightLeaf;
            synchronized (leafNodes) {
                leftLeaf = leafNodes.get(Constants.DIRECTION.LEFT);
                rightLeaf = leafNodes.get(Constants.DIRECTION.RIGHT);
            }
            if (leftLeaf != null && rightLeaf != null) {
                //Its not last peer to be removed
                if (!leftLeaf.getId().equals(rightLeaf.getId())) {
                    //Atleast 2 peers will be in pastry after removal of this.
                    PeerSendsUpdateLeaf updateLeftLeaf = new PeerSendsUpdateLeaf();
                    updateLeftLeaf.setDirection(Constants.DIRECTION.RIGHT);
                    updateLeftLeaf.setSendingPeerId(id);
                    updateLeftLeaf.setLeafInfo(rightLeaf);
                    updateLeftLeaf.setLeafInfoLen(rightLeaf.getBytes().length);
                    System.out.println("INFO : Sending right leaf upfate to " + leftLeaf.getId());
                    TCPSender leftLeafUpdateSender = new TCPSender(new Socket(leftLeaf.getIp(), leftLeaf.getPort()));
                    leftLeafUpdateSender.sendData(updateLeftLeaf.getBytes());
                    PeerSendsUpdateLeaf updateRightLeaf = new PeerSendsUpdateLeaf();
                    updateRightLeaf.setDirection(Constants.DIRECTION.LEFT);
                    updateRightLeaf.setSendingPeerId(id);
                    updateRightLeaf.setLeafInfo(leftLeaf);
                    updateRightLeaf.setLeafInfoLen(leftLeaf.getBytes().length);
                    System.out.println("INFO : Sending left leaf upfate to " + rightLeaf.getId());
                    TCPSender rightLeafUpdateSender = new TCPSender(new Socket(rightLeaf.getIp(), rightLeaf.getPort()));
                    rightLeafUpdateSender.sendData(updateRightLeaf.getBytes());
                    //Sending files
                    synchronized (fileDetails) {
                        for (Iterator<Map.Entry<String, String>> it = fileDetails.entrySet().iterator(); it.hasNext();) {
                            Map.Entry<String, String> entrySet = it.next();
                            String fileKey = entrySet.getKey();
                            String filepath = entrySet.getValue();
                            StoreFileFromLeavingPeer fileFromLeavingPeer = new StoreFileFromLeavingPeer();
                            fileFromLeavingPeer.setFileKey(fileKey);
                            fileFromLeavingPeer.setFilepath(filepath);
                            fileFromLeavingPeer.setLeavingPeerId(id);
                            File file = new File(filepath);
                            if (file.exists()) {
                                try (FileInputStream fin = new FileInputStream(file)) {
                                    int fileLength = (int) file.length();
                                    if (fileLength > 0) {
                                        byte[] data = new byte[fileLength];
                                        fin.read(data);
                                        fileFromLeavingPeer.setDataLen(fileLength);
                                        fileFromLeavingPeer.setData(data);
                                    }
                                    fin.close();
                                }
                                PeerInfo destPeer = null;
                                DistanceDirection distanceAndDirectionFromLeftLeaf = SystemFunctionUtil.getDistanceAndDirection(fileKey, leftLeaf.getId());
                                DistanceDirection distanceAndDirectionFromRightLeaf = SystemFunctionUtil.getDistanceAndDirection(fileKey, rightLeaf.getId());
                                if (distanceAndDirectionFromLeftLeaf.getDistance() > distanceAndDirectionFromRightLeaf.getDistance()) {
                                    //fileKey is closer to right leaf
                                    destPeer = rightLeaf;
                                } else if (distanceAndDirectionFromLeftLeaf.getDistance() < distanceAndDirectionFromRightLeaf.getDistance()) {
                                    //fileKey is closer to left leaf
                                    destPeer = leftLeaf;
                                } else {
                                    //check higher id
                                    if (Integer.parseInt(leftLeaf.getId(), 16) > Integer.parseInt(rightLeaf.getId(), 16)) {
                                        //left leaf id is higher
                                        destPeer = leftLeaf;
                                    } else {
                                        //right leaf id is higher
                                        destPeer = leftLeaf;
                                    }
                                }
                                System.out.println("INFO : Sending " + filepath + " with key : " + fileKey + " to " + destPeer.getId());
                                TCPSender filesender = new TCPSender(new Socket(destPeer.getIp(), destPeer.getPort()));
                                filesender.sendData(fileFromLeavingPeer.getBytes());
                            }
                            it.remove();
                        }
                    }
                } else {
                    //Only one peer will be there after this nodes removal
                    PeerSendsUpdateLeaf updateLeftLeaf = new PeerSendsUpdateLeaf();
                    updateLeftLeaf.setSendingPeerId(id);
                    updateLeftLeaf.setDirection(Constants.DIRECTION.RIGHT);
//                    updateLeftLeaf.setLeafInfo(rightLeaf);
                    updateLeftLeaf.setLeafInfoLen(0);
                    TCPSender leafUpdateSender = new TCPSender(new Socket(leftLeaf.getIp(), leftLeaf.getPort()));
                    leafUpdateSender.sendData(updateLeftLeaf.getBytes());
                    PeerSendsUpdateLeaf updateRightLeaf = new PeerSendsUpdateLeaf();
                    updateRightLeaf.setSendingPeerId(id);
                    updateRightLeaf.setDirection(Constants.DIRECTION.LEFT);
//                    updateRightLeaf.setLeafInfo(leftLeaf);
                    updateRightLeaf.setLeafInfoLen(0);
                    leafUpdateSender = new TCPSender(new Socket(leftLeaf.getIp(), leftLeaf.getPort()));
                    leafUpdateSender.sendData(updateRightLeaf.getBytes());
                    synchronized (fileDetails) {
                        for (Iterator<Map.Entry<String, String>> it = fileDetails.entrySet().iterator(); it.hasNext();) {
                            Map.Entry<String, String> entrySet = it.next();
                            String fileKey = entrySet.getKey();
                            String filepath = entrySet.getValue();
                            StoreFileFromLeavingPeer storeFileFromLeavingPeer = new StoreFileFromLeavingPeer();
                            storeFileFromLeavingPeer.setFileKey(fileKey);
                            storeFileFromLeavingPeer.setFilepath(filepath);
                            storeFileFromLeavingPeer.setLeavingPeerId(id);
                            File file = new File(filepath);
                            if (file.exists()) {
                                try (FileInputStream fin = new FileInputStream(file)) {
                                    int fileLength = (int) file.length();
                                    if (fileLength > 0) {
                                        byte[] data = new byte[fileLength];
                                        fin.read(data);
                                        storeFileFromLeavingPeer.setDataLen(fileLength);
                                        storeFileFromLeavingPeer.setData(data);
                                        System.out.println("INFO : Sending " + filepath + " with key : " + fileKey + " to " + leftLeaf.getId());
                                        leafUpdateSender = new TCPSender(new Socket(leftLeaf.getIp(), leftLeaf.getPort()));
                                        leafUpdateSender.sendData(storeFileFromLeavingPeer.getBytes());
                                    }
                                    fin.close();
                                }
                            }
                            it.remove();
                        }
                    }
                }
            } else {
                //Last peer to be removed
                System.out.println("INFO : Last node to be removed.");
            }
            System.exit(0);
            //Send files
        } catch (IOException ex) {
            System.err.println("ERROR : Sending leave message to DN");
        }
    }

    public void storeReceivedFiles(byte[] bytes) {
        LeafSendsFileToNewPeer filesReceived = new LeafSendsFileToNewPeer(bytes);
        String leafId = filesReceived.getLeafId();
        int noOfFilesRcvd = filesReceived.getNoOfFiles();
//                    if(noOfFilesRcvd > 0){
        if (noOfFilesRcvd > 0) {
            List<FileDetailsToSend> fileDetailsToStore = filesReceived.getFileDetailsToSend();
            for (FileDetailsToSend fileDetailToStore : fileDetailsToStore) {
                String fileKeyRcvd = fileDetailToStore.getFileKey();
                String filepathRcvd = fileDetailToStore.getFilepath();
                System.out.println("INFO : " + filepathRcvd + " with " + fileKeyRcvd + " key received from " + leafId);
                byte[] data = fileDetailToStore.getData();
                String folders = filepathRcvd.substring(0, filepathRcvd.lastIndexOf("/"));
                File foldersObj = new File(folders);
                if (!foldersObj.exists()) {
                    foldersObj.mkdirs();
                }
                File f = new File(filepathRcvd);
                try (FileOutputStream fOut = new FileOutputStream(f)) {
                    fOut.write(data);
                    fOut.close();
                    synchronized (fileDetails) {
                        fileDetails.put(fileKeyRcvd, filepathRcvd);
                    }
                } catch (IOException ex) {
                    System.err.println("ERROR : Writing data to file");
                }
            }
        } else {
            System.out.println("INFO : No files received from " + leafId);
        }
    }
}
