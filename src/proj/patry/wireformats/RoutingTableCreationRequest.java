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
public class RoutingTableCreationRequest {

    byte type;
    String id;
    String ipPort;

    public RoutingTableCreationRequest() {
        this.type = Constants.MESSAGES.PEER_SENDS_JOIN_REQUEST;
    }

    public RoutingTableCreationRequest(byte[] marshalledBytes) {
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

    @Override
    public String toString() {
        return "RoutingTableCreationRequest{" + "type=" + type + ", id=" + id + ", ipPort=" + ipPort + '}';
    }

}
