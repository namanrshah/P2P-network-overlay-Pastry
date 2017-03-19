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
public class LookupDestinationForFileKey {

    byte type;
    char operation;
    String clientIp;
    int clientPort;
    String fileKey;
    String filepath;
    String lastPeerId;
    int hopCount;
    String tracepath;

    public LookupDestinationForFileKey() {
        this.type = Constants.MESSAGES.LOOKUP_DESTINATION_FOR_FILE_KEY_STORE;
    }

    public LookupDestinationForFileKey(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            operation = din.readChar();
            int ipLen = din.readInt();
            byte[] ipBytes = new byte[ipLen];
            din.readFully(ipBytes);
            clientIp = new String(ipBytes);
            clientPort = din.readInt();
            int keyLen = din.readInt();
            byte[] keyBytes = new byte[keyLen];
            din.readFully(keyBytes);
            fileKey = new String(keyBytes);
            int pathLen = din.readInt();
            byte[] pathBytes = new byte[pathLen];
            din.readFully(pathBytes);
            filepath = new String(pathBytes);
            int lastPeerIdLen = din.readInt();
            byte[] lastPeerBytes = new byte[lastPeerIdLen];
            din.readFully(lastPeerBytes);
            lastPeerId = new String(lastPeerBytes);
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
            dout.writeChar(operation);
            dout.writeInt(clientIp.length());
            dout.write(clientIp.getBytes());
            dout.writeInt(clientPort);
            dout.writeInt(fileKey.length());
            dout.write(fileKey.getBytes());
            dout.writeInt(filepath.length());
            dout.write(filepath.getBytes());
            dout.writeInt(lastPeerId.length());
            dout.write(lastPeerId.getBytes());
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

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getLastPeerId() {
        return lastPeerId;
    }

    public void setLastPeerId(String lastPeerId) {
        this.lastPeerId = lastPeerId;
    }

    public char getOperation() {
        return operation;
    }

    public void setOperation(char operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "LookupDestinationForFileKey{" + "type=" + type + ", clientIp=" + clientIp + ", clientPort=" + clientPort + ", fileKey=" + fileKey + ", lastPeerId=" + lastPeerId + '}';
    }

}
