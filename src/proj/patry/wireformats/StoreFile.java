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
public class StoreFile {

    byte type;
    String fileKey;
    String filePath;
    int dataLen;
    byte[] data;

    public StoreFile() {
        this.type = Constants.MESSAGES.STORE_FILE;
    }

    public StoreFile(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
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
            dout.writeInt(fileKey.length());
            dout.write(fileKey.getBytes());
            dout.writeInt(filePath.length());
            dout.write(filePath.getBytes());
            dout.writeInt(dataLen);
            if (dataLen > 0) {
                dout.write(data);
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

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
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
        return "StoreFile{" + "type=" + type + ", fileKey=" + fileKey + ", dataLen=" + dataLen + '}';
    }

}
