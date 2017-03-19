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
public class DestinationSendsLookupResponseForStore {

    byte type;
    String destIp;
    int destPort;
    String destId;
    String fileKey;
    String filepath;
    int hopCount;
    String tracepath;

    public DestinationSendsLookupResponseForStore() {
        this.type = Constants.MESSAGES.LOOKUP_DESTINATION_FOR_FILE_KEY_STORE;
    }

    public DestinationSendsLookupResponseForStore(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int ipLen = din.readInt();
            byte[] ipBytes = new byte[ipLen];
            din.readFully(ipBytes);
            destIp = new String(ipBytes);
            destPort = din.readInt();
            int destIdLen = din.readInt();
            byte[] destIdBytes = new byte[destIdLen];
            din.readFully(destIdBytes);
            destId = new String(destIdBytes);
            int keyLen = din.readInt();
            byte[] keyBytes = new byte[keyLen];
            din.readFully(keyBytes);
            fileKey = new String(keyBytes);
            int pathLen = din.readInt();
            byte[] pathBytes = new byte[pathLen];
            din.readFully(pathBytes);
            filepath = new String(pathBytes);
            hopCount = din.readInt();
            int tracepathLen = din.readInt();
            byte[] tracepathBytes = new byte[tracepathLen];
            din.readFully(tracepathBytes);
            tracepath = new String(tracepathBytes);
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
            dout.writeInt(destIp.length());
            dout.write(destIp.getBytes());
            dout.writeInt(destPort);
            dout.writeInt(destId.length());
            dout.write(destId.getBytes());
            dout.writeInt(fileKey.length());
            dout.write(fileKey.getBytes());
            dout.writeInt(filepath.length());
            dout.write(filepath.getBytes());
            dout.writeInt(hopCount);
            dout.writeInt(tracepath.length());
            dout.write(tracepath.getBytes());
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

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getDestIp() {
        return destIp;
    }

    public void setDestIp(String destIp) {
        this.destIp = destIp;
    }

    public int getDestPort() {
        return destPort;
    }

    public void setDestPort(int destPort) {
        this.destPort = destPort;
    }

    public String getDestId() {
        return destId;
    }

    public void setDestId(String destId) {
        this.destId = destId;
    }

    @Override
    public String toString() {
        return "DestinationSendsLookupResponse{" + "type=" + type + ", destIp=" + destIp + ", destPort=" + destPort + ", destId=" + destId + '}';
    }

}
