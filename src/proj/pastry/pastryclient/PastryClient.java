package proj.pastry.pastryclient;

import proj.pastry.discoverynode.NodeListeningThread;
import proj.pastry.transport.Node;
import proj.pastry.transport.TCPConnection;
import proj.pastry.transport.TCPSender;
import proj.pastry.util.ConsoleCommands;
import proj.pastry.util.Constants;
import proj.patry.wireformats.ClientToDNRequestRandomPeer;
import proj.patry.wireformats.DNSendsRandomPeerToClient;
import proj.patry.wireformats.DestinationSendsLookupResponseForRetrieve;
import proj.patry.wireformats.DestinationSendsLookupResponseForStore;
import proj.patry.wireformats.LookupDestinationForFileKey;
import proj.patry.wireformats.StoreFile;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class PastryClient implements Node {

    int listeningPort;
    String ownIp;
    String discoveryNodeIp;
    int discoveryNodePort;
    Logger logger = Logger.getLogger(getClass().getName());
    MessageDigest messageDigest;
    //<filename, fileKey>
    public Map<String, String> filesWithCustomKey = new HashMap<>();
    public static final String readFileLoc = "/s/chopin/l/grad/namanrs/retrievedFiles/";

    public static void main(String[] args) {
        //args[0] - own listening port
        //args[1] - DN ip address
        //args[2] - DN listenig port
        new PastryClient().start(args);
    }

    public void start(String[] args) {
        try {
            listeningPort = Integer.parseInt(args[0]);
            ownIp = InetAddress.getLocalHost().getHostAddress();
            discoveryNodeIp = args[1];
            discoveryNodePort = Integer.parseInt(args[2]);
            logger.log(Level.INFO, "Client listening on - {0} : {1}", new Object[]{ownIp, listeningPort});
            //start listening
            NodeListeningThread clientListeningThread = new NodeListeningThread(this);
            Thread listeningThread = new Thread(clientListeningThread);
            listeningThread.start();
            //starting command thread
            ConsoleCommands consoleCommands = new ConsoleCommands(this);
            Thread consoleCommandThread = new Thread(consoleCommands);
            consoleCommandThread.start();
            //create folders for retrived files
            File folders = new File(readFileLoc);
            if (!folders.exists()) {
                folders.mkdirs();
            }
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (UnknownHostException ex) {
            logger.log(Level.SEVERE, "Unknown host : {0}", ownIp);
        } catch (NoSuchAlgorithmException ex) {
            System.err.println("ERROR : No algorithm found.");
        }
    }

    @Override
    public void processRequest(byte[] bytes, Socket socket) {
        byte type = bytes[0];
        switch (type) {
            case Constants.MESSAGES.DESTINATION_SENDS_LOOKUP_RESPONSE_FOR_RETRIEVE:
                DestinationSendsLookupResponseForRetrieve responseForRetrieve = new DestinationSendsLookupResponseForRetrieve(bytes);
                String destId = responseForRetrieve.getDestId();
                String fileKey = responseForRetrieve.getFileKey();
                String filePath = responseForRetrieve.getFilePath();
                int hopCountRtr = responseForRetrieve.getHopCount();
                String tracepathRtr = responseForRetrieve.getTracepath();
                int dataLen = responseForRetrieve.getDataLen();
                if (!filePath.equals(Constants.NO_IDS_REGISTERED_STRING) && dataLen > 0) {
                    byte[] filedata = responseForRetrieve.getData();
                    System.out.println("INFO : File retrieve response from - " + destId + " for\n" + filePath + " with fileKey " + fileKey);
                    System.out.println("INFO : Hop count : " + hopCountRtr);
                    System.out.println("INFO : Path : " + tracepathRtr);
                    //Store file locally
                    String filename = filePath.substring(filePath.lastIndexOf("/") + 1);
                    File fileToStore = new File(readFileLoc + filename);
                    if (!fileToStore.exists()) {
                        try {
                            fileToStore.createNewFile();
                            try (FileOutputStream fOut = new FileOutputStream(fileToStore)) {
                                fOut.write(filedata);
                                fOut.close();
                                System.out.println("INFO : Stored file successfully.");
                                socket.close();
                            } catch (IOException ex) {
                                System.err.println("ERROR : Writing data to file");
                            }
                        } catch (IOException ex) {
                            System.err.println("ERROR : Creating new file");
                        }

                    }
                } else {
                    //Error in reading file
                    System.err.println("ERROR : Either file does not exists or its empty.");
                }
                break;
            case Constants.MESSAGES.LOOKUP_DESTINATION_FOR_FILE_KEY_STORE:
                DestinationSendsLookupResponseForStore lookupResponse = new DestinationSendsLookupResponseForStore(bytes);
                String destIp = lookupResponse.getDestIp();
                int destPort = lookupResponse.getDestPort();
                String destIdToStore = lookupResponse.getDestId();
                String fileKey1 = lookupResponse.getFileKey();
                String filepath = lookupResponse.getFilepath();
                int hopCount = lookupResponse.getHopCount();
                String tracepath = lookupResponse.getTracepath();
                System.out.println("INFO : Destination for filekey : " + fileKey1 + " is : " + destIdToStore);
                System.out.println("INFO : Hop count : " + hopCount);
                System.out.println("INFO : Path : " + tracepath);
                //send file data to random peer
                File fileToStore = new File(filepath);
                if (fileToStore.exists()) {
                    FileInputStream fin = null;
                    try {
                        StoreFile storeFile = new StoreFile();
                        storeFile.setFileKey(fileKey1);
                        storeFile.setFilePath(filepath);
                        fin = new FileInputStream(fileToStore);
                        int fileLength = (int) fileToStore.length();
                        byte[] fileData = new byte[fileLength];
                        fin.read(fileData);
                        storeFile.setDataLen(fileLength);
                        storeFile.setData(fileData);
                        System.out.println("INFO : Sending file data to : " + destIdToStore + Constants.DELIMITORS.TRACEPATH_DELIM + destIp + ":" + destPort);
                        TCPSender fileSender = new TCPSender(new Socket(destIp, destPort));
                        fileSender.sendData(storeFile.getBytes());
                    } catch (FileNotFoundException ex) {
                        System.err.println("ERROR : File not found : " + filepath);
                    } catch (IOException ex) {
                        System.err.println("ERROR : Reading file : " + filepath);
                    } finally {
                        try {
                            fin.close();
                            socket.close();
                        } catch (IOException ex) {
                            System.err.println("ERROR : Closing input stream.");
                        }
                    }
                }
                break;
            default:
                System.out.println("WARNING : Default case, message type : " + type);
        }
    }

    @Override
    public int retrieveListeningPort() {
        return listeningPort;
    }

    public void storeFile(String filePath, String key) {
        File fileToStore = new File(filePath);
        if (fileToStore.exists()) {
            try {
                //ask discoverynode for random peer
                ClientToDNRequestRandomPeer reqRandomPeer = new ClientToDNRequestRandomPeer();
                reqRandomPeer.setClientIp(ownIp);
                reqRandomPeer.setClientPort(listeningPort);
                DataInputStream din;
                int dataLength;
                byte[] data;
                try (Socket dnSocket = new Socket(discoveryNodeIp, discoveryNodePort)) {
                    TCPSender sender = new TCPSender(dnSocket);
                    sender.sendData(reqRandomPeer.getBytes());
                    //waiting for response
                    din = new DataInputStream(dnSocket.getInputStream());
                    dataLength = din.readInt();
                    data = new byte[dataLength];
                    din.readFully(data, 0, dataLength);
                    din.close();
                    dnSocket.close();
                }
                byte type = data[0];
                if (type == Constants.MESSAGES.DN_SENDS_RANDOM_PEER_TO_CLIENT) {
                    DNSendsRandomPeerToClient randomPeer = new DNSendsRandomPeerToClient(data);
                    String randomPeerId = randomPeer.getRandomPeerId();
                    String randomPeerIp = randomPeer.getRandomPeerIp();
                    int randomPeerPort = randomPeer.getRandomPeerPort();
                    System.out.println("INFO : Random peer returned for store : " + randomPeerId);
                    System.out.println("INFO : Random peer connection details - " + randomPeerIp + ":" + randomPeerPort);
                    if (!randomPeerId.equals(Constants.NO_IDS_REGISTERED_STRING)) {
                        if (key == null) {
                            //compute 16-bit key for file
                            byte[] digest = messageDigest.digest(filePath.getBytes());
                            StringBuffer sb = new StringBuffer("");
                            for (int i = 0; i < digest.length; i++) {
                                sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
                            }
                            key = sb.substring(sb.length() - 4);
                        }
                        System.out.println("INFO : File key to store : " + key);
                        //Ask random peer for file destination
                        if (!key.equals(randomPeerId)) {
                            LookupDestinationForFileKey lookup = new LookupDestinationForFileKey();
                            lookup.setOperation(Constants.OPERATION.STORE);
                            lookup.setClientIp(ownIp);
                            lookup.setClientPort(listeningPort);
                            lookup.setFileKey(key);
                            lookup.setFilepath(filePath);
                            lookup.setLastPeerId(Constants.NO_IDS_REGISTERED_STRING);
                            lookup.setHopCount(0);
                            lookup.setTracepath(Constants.NO_IDS_REGISTERED_STRING);
                            System.out.println("INFO : Sending lookup request to : " + randomPeerId);
                            try {
                                Socket lookupSocket = new Socket(randomPeerIp, randomPeerPort);
                                TCPSender lookupSender = new TCPSender(lookupSocket);
                                lookupSender.sendData(lookup.getBytes());
                                //waiting for response
//                                din = new DataInputStream(lookupSocket.getInputStream());
//                                dataLength = din.readInt();
//                                data = new byte[dataLength];
//                                din.readFully(data, 0, dataLength);
//                                type = data[0];
//                                if (type == Constants.MESSAGES.DESTINATION_SENDS_LOOKUP_RESPONSE_FOR_STORE) {
//                                    DestinationSendsLookupResponseForStore lookupResponse = new DestinationSendsLookupResponseForStore(data);
//                                    String destIp = lookupResponse.getDestIp();
//                                    int destPort = lookupResponse.getDestPort();
//                                    String destId = lookupResponse.getDestId();
//                                    System.out.println("INFO : Destination for filekey : " + fileKey + " is : " + destId);
//                                    //send file data to random peer
//                                    StoreFile storeFile = new StoreFile();
//                                    storeFile.setFileKey(fileKey);
//                                    storeFile.setFilePath(filePath);
//                                    FileInputStream fin = new FileInputStream(fileToStore);
//                                    int fileLength = (int) fileToStore.length();
//                                    byte[] fileData = new byte[fileLength];
//                                    fin.read(fileData);
//                                    storeFile.setDataLen(fileLength);
//                                    storeFile.setData(fileData);
//                                    System.out.println("INFO : Sending file data to : " + destId + Constants.DELIMITORS.TRACEPATH_DELIM + destIp + ":" + destPort);
//                                    TCPSender fileSender = new TCPSender(new Socket(destIp, destPort));
//                                    fileSender.sendData(storeFile.getBytes());
//                                    storeFile.set
//                                } else {
//                                    System.out.println("ERROR : Invalid message type : " + type + ", Waiting for lookup response.");
//                                }
                            } catch (ConnectException ex) {
                                System.err.println("ERROR : Connecting to destination for storing file.");
                            }
                        } else {
                            //filekey is same as randomPeerId, don't go for lookup and directly store the file
                            System.out.println("INFO : File key is same as random peer, directly sending the file.");
                            System.out.println("INFO : Destination for filekey : " + key + " is : " + randomPeerId);
                            StoreFile storeFile = new StoreFile();
                            storeFile.setFileKey(key);
                            storeFile.setFilePath(filePath);
                            FileInputStream fin = new FileInputStream(fileToStore);
                            int fileLength = (int) fileToStore.length();
                            byte[] fileData = new byte[fileLength];
                            fin.read(fileData);
                            storeFile.setDataLen(fileLength);
                            storeFile.setData(fileData);
                            System.out.println("INFO : Sending file data to : " + randomPeerId + Constants.DELIMITORS.TRACEPATH_DELIM + randomPeerIp + ":" + randomPeerPort);
                            TCPSender fileSender = new TCPSender(new Socket(randomPeerIp, randomPeerPort));
                            fileSender.sendData(storeFile.getBytes());
                        }
                    } else {
                        System.out.println("INFO : No peers in pastry yet, try after sometime.");
                    }
                } else {
                    System.out.println("ERROR : Invalid message type : " + type + ", wating for random peer from DiscoveryNode. ");
                }
            } catch (IOException ex) {
                System.err.println("ERROR : Sending request to discovery node");
            }
        } else {
            System.err.println("ERROR : File does not exist, please try again.");
        }
    }

    public void retrieveFile(String filePath) {
        System.out.println("INFO : Retrieve request for : " + filePath);
        //ask discoverynode for random peer
        try {
            ClientToDNRequestRandomPeer reqRandomPeer = new ClientToDNRequestRandomPeer();
            reqRandomPeer.setClientIp(ownIp);
            reqRandomPeer.setClientPort(listeningPort);
            DataInputStream din;
            int dataLength;
            byte[] data;
            try (Socket dnSocket = new Socket(discoveryNodeIp, discoveryNodePort)) {
                TCPSender sender = new TCPSender(dnSocket);
                sender.sendData(reqRandomPeer.getBytes());
                //waiting for response
                din = new DataInputStream(dnSocket.getInputStream());
                dataLength = din.readInt();
                data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                din.close();
                dnSocket.close();
            }
            byte type = data[0];
            if (type == Constants.MESSAGES.DN_SENDS_RANDOM_PEER_TO_CLIENT) {
                DNSendsRandomPeerToClient randomPeer = new DNSendsRandomPeerToClient(data);
                String randomPeerId = randomPeer.getRandomPeerId();
                String randomPeerIp = randomPeer.getRandomPeerIp();
                int randomPeerPort = randomPeer.getRandomPeerPort();
                System.out.println("INFO : Random peer returned for retrieveal : " + randomPeerId);
                System.out.println("INFO : Random peer connection details - " + randomPeerIp + ":" + randomPeerPort);
                if (!randomPeerId.equals(Constants.NO_IDS_REGISTERED_STRING)) {
                    String fileKey = "";
                    synchronized (filesWithCustomKey) {
                        fileKey = filesWithCustomKey.get(filePath);
                    }
                    if (fileKey == null) {
                        //compute 16-bit key for file
                        byte[] digest = messageDigest.digest(filePath.getBytes());
                        StringBuffer sb = new StringBuffer("");
                        for (int i = 0; i < digest.length; i++) {
                            sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
                        }
                        fileKey = sb.substring(sb.length() - 4);
                    } else {
                        System.out.println("INFO : This file was stored with custom key.");
                    }
                    System.out.println("INFO : File key to read : " + fileKey);
                    LookupDestinationForFileKey lookup = new LookupDestinationForFileKey();
                    lookup.setOperation(Constants.OPERATION.RETRIEVE);
                    lookup.setClientIp(ownIp);
                    lookup.setClientPort(listeningPort);
                    lookup.setFileKey(fileKey);
                    lookup.setFilepath(filePath);
                    lookup.setLastPeerId(Constants.NO_IDS_REGISTERED_STRING);
                    lookup.setHopCount(0);
                    lookup.setTracepath(Constants.NO_IDS_REGISTERED_STRING);
                    System.out.println("INFO : Sending lookup request to : " + randomPeerId);
                    try {
                        Socket lookupSocket = new Socket(randomPeerIp, randomPeerPort);
                        TCPConnection lookupConnection = new TCPConnection(lookupSocket, this);
                        lookupConnection.startReceiverThread(this);
                        lookupConnection.getSender().sendData(lookup.getBytes());
                    } catch (IOException ex) {
                        System.err.println("ERROR : Connecting to random peer.");
                    }
                } else {
                    System.out.println("INFO : No peers in pastry yet, try after sometime.");
                }
            } else {
                System.out.println("ERROR : Invalid message type : " + type + ", wating for random peer from DiscoveryNode. ");
            }
        } catch (IOException ex) {
            System.err.println("ERROR : Sending request to discovery node");
        }
    }
}
