package proj.patry.wireformats;

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
public class DNSendsRandomPeerToClient {

    byte type;
    String randomPeerId;
    String randomPeerIp;
    int randomPeerPort;

    public DNSendsRandomPeerToClient() {
        this.type = Constants.MESSAGES.DN_SENDS_RANDOM_PEER_TO_CLIENT;
    }

    public DNSendsRandomPeerToClient(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int idLen = din.readInt();
            byte[] idBytes = new byte[idLen];
            din.readFully(idBytes);
            randomPeerId = new String(idBytes);
            int ipLen = din.readInt();
            byte[] ipBytes = new byte[ipLen];
            din.readFully(ipBytes);
            randomPeerIp = new String(ipBytes);
            randomPeerPort = din.readInt();
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
            dout.writeInt(randomPeerId.length());
            dout.write(randomPeerId.getBytes());
            dout.writeInt(randomPeerIp.length());
            dout.write(randomPeerIp.getBytes());
            dout.writeInt(randomPeerPort);
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

    public String getRandomPeerId() {
        return randomPeerId;
    }

    public void setRandomPeerId(String randomPeerId) {
        this.randomPeerId = randomPeerId;
    }

    public String getRandomPeerIp() {
        return randomPeerIp;
    }

    public void setRandomPeerIp(String randomPeerIp) {
        this.randomPeerIp = randomPeerIp;
    }

    public int getRandomPeerPort() {
        return randomPeerPort;
    }

    public void setRandomPeerPort(int randomPeerPort) {
        this.randomPeerPort = randomPeerPort;
    }

    @Override
    public String toString() {
        return "DNSendsRandomPeerToClient{" + "type=" + type + ", randomPeerId=" + randomPeerId + ", randomPeerIp=" + randomPeerIp + ", randomPeerPort=" + randomPeerPort + '}';
    }

}
