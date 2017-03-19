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
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class DestinationSendsJoinResponse {

    byte type;
    String destId;
    int destNodeLen;
    PeerInfo destNode;
    char destNodeDirection;
    int leafNodeLen;
    PeerInfo leafNode;
    char leafNodeDirection;
    int hopCount;
    String tracepath;
    int routingTableLength;
    Map<Integer, Map<Character, Map<String, PeerInfo>>> routingTable;

    public DestinationSendsJoinResponse() {
        this.type = Constants.MESSAGES.DESTINATION_SENDS_JOIN_RESPONSE;
    }

    public DestinationSendsJoinResponse(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int len = din.readInt();
            byte[] info = new byte[len];
            din.readFully(info);
            destId = new String(info);
            destNodeLen = din.readInt();
            if (destNodeLen > 0) {
                byte[] destNodeBytes = new byte[destNodeLen];
                din.readFully(destNodeBytes);
                destNode = new PeerInfo(destNodeBytes);
                destNodeDirection = din.readChar();
            }
            leafNodeLen = din.readInt();
            if (leafNodeLen > 0) {
                byte[] leafNodeBytes = new byte[leafNodeLen];
                din.readFully(leafNodeBytes);
                leafNode = new PeerInfo(leafNodeBytes);
                leafNodeDirection = din.readChar();
            }
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
                            Map<String, PeerInfo> peerHandles = new TreeMap<>();
                            rowMap.put(columnHeader, peerHandles);
                            int peerHandleSize = din.readInt();
                            if (peerHandleSize > -1) {
                                for (int k = 0; k < peerHandleSize; k++) {
                                    int idLen = din.readInt();
//                                    System.out.println("-len-" + idLen + " for " + j + "th row and " + k + "th column.");
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
            dout.writeInt(destId.length());
            dout.write(destId.getBytes());
            //leaf node info            
            dout.writeInt(destNodeLen);
            if (destNodeLen > 0) {
                dout.write(destNode.getBytes());
                dout.writeChar(destNodeDirection);
            }
            dout.writeInt(leafNodeLen);
            if (leafNodeLen > 0) {
                dout.write(leafNode.getBytes());
                dout.writeChar(leafNodeDirection);
            }
            dout.writeInt(hopCount);
            dout.writeInt(tracepath.length());
            dout.write(tracepath.getBytes());
//            dout.writeInt(ipPort.length());
//            dout.write(ipPort.getBytes());
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

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
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

    public int getDestNodeLen() {
        return destNodeLen;
    }

    public void setDestNodeLen(int destNodeLen) {
        this.destNodeLen = destNodeLen;
    }

    public PeerInfo getDestNode() {
        return destNode;
    }

    public void setDestNode(PeerInfo destNode) {
        this.destNode = destNode;
    }

    public char getDestNodeDirection() {
        return destNodeDirection;
    }

    public void setDestNodeDirection(char destNodeDirection) {
        this.destNodeDirection = destNodeDirection;
    }

    public int getLeafNodeLen() {
        return leafNodeLen;
    }

    public void setLeafNodeLen(int leafNodeLen) {
        this.leafNodeLen = leafNodeLen;
    }

    public PeerInfo getLeafNode() {
        return leafNode;
    }

    public void setLeafNode(PeerInfo leafNode) {
        this.leafNode = leafNode;
    }

    public char getLeafNodeDirection() {
        return leafNodeDirection;
    }

    public void setLeafNodeDirection(char leafNodeDirection) {
        this.leafNodeDirection = leafNodeDirection;
    }

    public String getDestId() {
        return destId;
    }

    public void setDestId(String destId) {
        this.destId = destId;
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

}
