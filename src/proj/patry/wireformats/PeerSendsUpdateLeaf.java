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
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class PeerSendsUpdateLeaf {

    byte type;
    int leafInfoLen;
    PeerInfo leafInfo;
    String sendingPeerId;
    char direction;

    public PeerSendsUpdateLeaf() {
        this.type = Constants.MESSAGES.PEER_SENDS_UPDATE_LEAF;
    }

    public PeerSendsUpdateLeaf(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int sendingPeerIdLen = din.readInt();
            byte[] sendingPeerIdBytes = new byte[sendingPeerIdLen];
            din.readFully(sendingPeerIdBytes);
            sendingPeerId = new String(sendingPeerIdBytes);
            leafInfoLen = din.readInt();
            if (leafInfoLen > 0) {
                byte[] leafInfoBytes = new byte[leafInfoLen];
                din.readFully(leafInfoBytes);
                leafInfo = new PeerInfo(leafInfoBytes);
            }
            direction = din.readChar();
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
            dout.writeInt(sendingPeerId.length());
            dout.write(sendingPeerId.getBytes());
            dout.writeInt(leafInfoLen);
            if (leafInfoLen > 0) {
                dout.write(leafInfo.getBytes());
            }
            dout.writeChar(direction);
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

    public String getSendingPeerId() {
        return sendingPeerId;
    }

    public void setSendingPeerId(String sendingPeerId) {
        this.sendingPeerId = sendingPeerId;
    }

    public int getLeafInfoLen() {
        return leafInfoLen;
    }

    public void setLeafInfoLen(int leafInfoLen) {
        this.leafInfoLen = leafInfoLen;
    }

    public PeerInfo getLeafInfo() {
        return leafInfo;
    }

    public void setLeafInfo(PeerInfo leafInfo) {
        this.leafInfo = leafInfo;
    }

    public char getDirection() {
        return direction;
    }

    public void setDirection(char direction) {
        this.direction = direction;
    }

    @Override
    public String toString() {
        return "PeerSendsUpdateLeaf{" + "type=" + type + ", leafInfoLen=" + leafInfoLen + ", leafInfo=" + leafInfo + ", direction=" + direction + '}';
    }

}
