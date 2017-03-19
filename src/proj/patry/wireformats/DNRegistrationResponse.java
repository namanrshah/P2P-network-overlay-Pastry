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
public class DNRegistrationResponse {

    byte type;
    boolean registered;
    int randomPeerIdLen;
    String randomPeerId;
    int randomPeerIpPortLen;
    String randomPeerIpPort;

    public DNRegistrationResponse() {
        this.type = Constants.MESSAGES.DN_SENDS_REGISTRATION_RESPONSE;
    }

    public DNRegistrationResponse(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            registered = din.readBoolean();
            if (registered) {
                randomPeerIdLen = din.readInt();
                if (randomPeerIdLen > 0) {
                    byte[] id = new byte[randomPeerIdLen];
                    din.readFully(id);
                    randomPeerId = new String(id);
                }
                randomPeerIpPortLen = din.readInt();
                if (randomPeerIpPortLen > 0) {
                    byte[] info = new byte[randomPeerIpPortLen];
                    din.readFully(info);
                    randomPeerIpPort = new String(info);
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
            dout.writeBoolean(registered);
            if (registered) {
                dout.writeInt(randomPeerIdLen);
                if (randomPeerIdLen > 0) {
                    dout.write(randomPeerId.getBytes());
                }
                dout.writeInt(randomPeerIpPortLen);
                if (randomPeerIpPortLen > 0) {
                    dout.write(randomPeerIpPort.getBytes());
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

    public int getRandomPeerIdLen() {
        return randomPeerIdLen;
    }

    public void setRandomPeerIdLen(int randomPeerIdLen) {
        this.randomPeerIdLen = randomPeerIdLen;
    }

    public String getRandomPeerId() {
        return randomPeerId;
    }

    public void setRandomPeerId(String randomPeerId) {
        this.randomPeerId = randomPeerId;
    }

    public int getRandomPeerIpPortLen() {
        return randomPeerIpPortLen;
    }

    public void setRandomPeerIpPortLen(int randomPeerIpPortLen) {
        this.randomPeerIpPortLen = randomPeerIpPortLen;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }

    public String getRandomPeerIpPort() {
        return randomPeerIpPort;
    }

    public void setRandomPeerIpPort(String randomPeerIpPort) {
        this.randomPeerIpPort = randomPeerIpPort;
    }

    @Override
    public String toString() {
        return "DNRegistrationResponse{" + "type=" + type + ", registered=" + registered + ", randomNodeIpPort=" + randomPeerIpPort + '}';
    }

}
