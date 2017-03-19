package proj.patry.wireformats;

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
public class FileDetailsToSend {

    String fileKey;
    String filepath;
    int dataLen;
    byte[] data;

    public FileDetailsToSend() {
    }

    public FileDetailsToSend(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            int fileKeyLen = din.readInt();
            byte[] fileKeyBytes = new byte[fileKeyLen];
            din.readFully(fileKeyBytes);
            fileKey = new String(fileKeyBytes);
            int filePathLen = din.readInt();
            byte[] filePathBytes = new byte[filePathLen];
            din.readFully(filePathBytes);
            filepath = new String(filePathBytes);
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
            dout.writeInt(fileKey.length());
            dout.write(fileKey.getBytes());
            dout.writeInt(filepath.length());
            dout.write(filepath.getBytes());
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

    
}
