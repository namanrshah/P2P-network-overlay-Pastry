package proj.patry.wireformats;

import proj.pastry.peer.PeerInfo;
import proj.pastry.util.Constants;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class PeerSendsJoinRequest {

    byte type;
    String id;
    String ipPort;
    String lastPeerId;
    int hopCount;
    String tracepath;
    int routingTableLength;
    Map<Integer, Map<Character, Map<String, PeerInfo>>> routingTable;

    public PeerSendsJoinRequest() {
        this.type = Constants.MESSAGES.PEER_SENDS_JOIN_REQUEST;
    }

    public PeerSendsJoinRequest(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int len = din.readInt();
            byte[] info = new byte[len];
            din.readFully(info);
            id = new String(info);
            int ipPortLen = din.readInt();
            byte[] ipPortBytes = new byte[ipPortLen];
            din.readFully(ipPortBytes);
            ipPort = new String(ipPortBytes);
            int lastPeerIdLen = din.readInt();
            byte[] lastPeerIdBytes = new byte[lastPeerIdLen];
            din.readFully(lastPeerIdBytes);
            lastPeerId = new String(lastPeerIdBytes);
            hopCount = din.readInt();
            int tracepathLen = din.readInt();
            byte[] tracepathBytes = new byte[tracepathLen];
            din.readFully(tracepathBytes);
            tracepath = new String(tracepathBytes);
            routingTableLength = din.readInt();
            routingTable = new TreeMap<>();
            if (routingTableLength > 0) {
                for (int i = 0; i < routingTableLength; i++) {
                    //rows
                    int row = din.readInt();
                    Map<Character, Map<String, PeerInfo>> rowMap = new TreeMap<>();
                    routingTable.put(row, rowMap);
                    int columnCount = din.readInt();
                    if (columnCount > -1) {
                        for (int j = 0; j < columnCount; j++) {
                            //columns
                            char columnHeader = din.readChar();
                            Map<String, PeerInfo> peerHandles = new HashMap<>();
                            rowMap.put(columnHeader, peerHandles);
                            int peerHandleSize = din.readInt();
                            if (peerHandleSize > -1) {
                                for (int k = 0; k < peerHandleSize; k++) {
                                    int idLen = din.readInt();
                                    byte[] idBytes = new byte[idLen];
                                    din.readFully(idBytes);
                                    String handleId = new String(idBytes);
                                    int peerInfoLen = din.readInt();
                                    byte[] peerInfoBytes = new byte[peerInfoLen];
                                    din.readFully(peerInfoBytes);
                                    PeerInfo peerInfo = new PeerInfo(peerInfoBytes);
                                    peerHandles.put(handleId, peerInfo);
                                }
                            } else {
                                //No columns
                            }
                        }
                    } else {
                        //No rows
                    }
                }
            }
            baInputStream.close();
            din.close();
        } catch (IOException ex) {
            System.err.println("ERROR : exception in deserializing." + this.getClass());
        }
    }

    public byte[] getBytes() {
        try {
            byte[] marshalledBytes = null;
            ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
            dout.write(type);
            dout.writeInt(id.length());
            dout.write(id.getBytes());
            dout.writeInt(ipPort.length());
            dout.write(ipPort.getBytes());
            dout.writeInt(lastPeerId.length());
            dout.write(lastPeerId.getBytes());
            dout.writeInt(hopCount);
            dout.writeInt(tracepath.length());
            dout.write(tracepath.getBytes());
            //rows size
            dout.writeInt(routingTableLength);
            if (routingTableLength > 0) {
                for (Map.Entry<Integer, Map<Character, Map<String, PeerInfo>>> entrySet : routingTable.entrySet()) {
                    //row
                    Integer row = entrySet.getKey();
                    Map<Character, Map<String, PeerInfo>> value = entrySet.getValue();
                    dout.writeInt(row);
                    if (value != null) {
                        //column size
                        dout.writeInt(value.size());
                        for (Map.Entry<Character, Map<String, PeerInfo>> entrySet1 : value.entrySet()) {
                            //column
                            Character column = entrySet1.getKey();
                            Map<String, PeerInfo> peerHandle = entrySet1.getValue();
                            dout.writeChar(column);
                            if (peerHandle != null) {
                                //peer handle size, most probably 1
                                dout.writeInt(peerHandle.size());
                                for (Map.Entry<String, PeerInfo> entrySet2 : peerHandle.entrySet()) {
                                    String peerHandleId = entrySet2.getKey();
                                    PeerInfo peerInfo = entrySet2.getValue();
                                    dout.writeInt(peerHandleId.length());
                                    dout.write(peerHandleId.getBytes());
                                    byte[] peerInfoBytes = peerInfo.getBytes();
                                    dout.writeInt(peerInfoBytes.length);
                                    dout.write(peerInfoBytes);
                                }
                            } else {
                                //no peer handle
                                dout.writeInt(-1);
                            }
                        }
                    } else {
                        //no columns
                        dout.writeInt(-1);
                    }
                }
            }
            dout.flush();
            marshalledBytes = baOutputStream.toByteArray();
            baOutputStream.close();
            dout.close();
            return marshalledBytes;
        } catch (IOException ex) {
            Logger.getLogger("ERROR : error in marshalling" + this.getClass());
        }
        return null;
    }

    public int getHopCount() {
        return hopCount;
    }

    public void setHopCount(int hopCount) {
        this.hopCount = hopCount;
    }

    public String getTracepath() {
        return tracepath;
    }

    public void setTracepath(String tracepath) {
        this.tracepath = tracepath;
    }

    public int getRoutingTableLength() {
        return routingTableLength;
    }

    public void setRoutingTableLength(int routingTableLength) {
        this.routingTableLength = routingTableLength;
    }

    public Map<Integer, Map<Character, Map<String, PeerInfo>>> getRoutingTable() {
        return routingTable;
    }

    public void setRoutingTable(Map<Integer, Map<Character, Map<String, PeerInfo>>> routingTable) {
        this.routingTable = routingTable;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIpPort() {
        return ipPort;
    }

    public void setIpPort(String ipPort) {
        this.ipPort = ipPort;
    }

    public String getLastPeerId() {
        return lastPeerId;
    }

    public void setLastPeerId(String lastPeerId) {
        this.lastPeerId = lastPeerId;
    }

    @Override
    public String toString() {
        return "RoutingTableCreationRequest{" + "type=" + type + ", id=" + id + ", ipPort=" + ipPort + '}';
    }

}
