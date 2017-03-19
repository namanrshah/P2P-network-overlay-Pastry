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
 * @author Rajiv
 */
public class PeerRequestsFilesWhileJoining {

    byte type;
    String id;
    String ip;
    int port;

    public PeerRequestsFilesWhileJoining() {
        this.type = Constants.MESSAGES.PEER_REQUESTS_FILES_WHILE_JOINING;
    }

    public PeerRequestsFilesWhileJoining(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int idLen = din.readInt();
            byte[] idBytes = new byte[idLen];
            din.readFully(idBytes);
            id = new String(idBytes);
            int ipLen = din.readInt();
            byte[] ipBytes = new byte[ipLen];
            din.readFully(ipBytes);
            ip = new String(ipBytes);
            port = din.readInt();

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
            dout.writeInt(ip.length());
            dout.write(ip.getBytes());
            dout.writeInt(port);
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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "PeerRequestsFilesWhileJoining{" + "type=" + type + ", id=" + id + ", ip=" + ip + ", port=" + port + '}';
    }

}
