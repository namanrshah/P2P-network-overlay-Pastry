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
public class DestinationSendsLookupResponseForRetrieve {

    byte type;
    String destId;
    String fileKey;
    String filePath;
    int dataLen;
    byte[] data;
    int hopCount;
    String tracepath;

    public DestinationSendsLookupResponseForRetrieve() {
        this.type = Constants.MESSAGES.DESTINATION_SENDS_LOOKUP_RESPONSE_FOR_RETRIEVE;
    }

    public DestinationSendsLookupResponseForRetrieve(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int destIdLen = din.readInt();
            byte[] destIdBytes = new byte[destIdLen];
            din.readFully(destIdBytes);
            destId = new String(destIdBytes);
            int fileKeyLen = din.readInt();
            byte[] fileKeyBytes = new byte[fileKeyLen];
            din.readFully(fileKeyBytes);
            fileKey = new String(fileKeyBytes);
            int filePathLen = din.readInt();
            byte[] filePathBytes = new byte[filePathLen];
            din.readFully(filePathBytes);
            filePath = new String(filePathBytes);
            dataLen = din.readInt();
            if (dataLen > 0) {
                data = new byte[dataLen];
                din.readFully(data);
            }
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
            dout.writeInt(destId.length());
            dout.write(destId.getBytes());
            dout.writeInt(fileKey.length());
            dout.write(fileKey.getBytes());
            dout.writeInt(filePath.length());
            dout.write(filePath.getBytes());
            dout.writeInt(dataLen);
            if (dataLen > 0) {
                dout.write(data);
            }
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

    public String getDestId() {
        return destId;
    }

    public void setDestId(String destId) {
        this.destId = destId;
    }

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "DestinationSendsLookupResponseForRetrieve{" + "type=" + type + ", destId=" + destId + ", fileKey=" + fileKey + ", filePath=" + filePath + ", dataLen=" + dataLen + '}';
    }

}
