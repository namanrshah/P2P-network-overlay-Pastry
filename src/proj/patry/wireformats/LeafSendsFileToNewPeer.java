package proj.patry.wireformats;

import proj.pastry.util.Constants;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author Rajiv
 */
public class LeafSendsFileToNewPeer {

    byte type;
    String leafId;
    int noOfFiles;
    List<FileDetailsToSend> fileDetailsToSend;

    public LeafSendsFileToNewPeer() {
        this.type = Constants.MESSAGES.LEAF_SENDS_FILE_TO_NEW_PEER;
    }

    public LeafSendsFileToNewPeer(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            type = din.readByte();
            int destIdLen = din.readInt();
            byte[] destIdBytes = new byte[destIdLen];
            din.readFully(destIdBytes);
            leafId = new String(destIdBytes);
            noOfFiles = din.readInt();
            fileDetailsToSend = new ArrayList<>();
            if (noOfFiles > 0) {
                for (int i = 0; i < noOfFiles; i++) {
                    int filebytesLen = din.readInt();
                    byte[] filebytes = new byte[filebytesLen];
                    din.readFully(filebytes);
                    fileDetailsToSend.add(new FileDetailsToSend(filebytes));
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
            dout.writeInt(leafId.length());
            dout.write(leafId.getBytes());
            dout.writeInt(noOfFiles);
            if (noOfFiles > 0) {
                for (int i = 0; i < noOfFiles; i++) {
                    byte[] filebytes = fileDetailsToSend.get(i).getBytes();
                    dout.writeInt(filebytes.length);
                    dout.write(filebytes);
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

    public String getLeafId() {
        return leafId;
    }

    public void setLeafId(String leafId) {
        this.leafId = leafId;
    }

    public int getNoOfFiles() {
        return noOfFiles;
    }

    public void setNoOfFiles(int noOfFiles) {
        this.noOfFiles = noOfFiles;
    }

    public List<FileDetailsToSend> getFileDetailsToSend() {
        return fileDetailsToSend;
    }

    public void setFileDetailsToSend(List<FileDetailsToSend> fileDetailsToSend) {
        this.fileDetailsToSend = fileDetailsToSend;
    }

    @Override
    public String toString() {
        return "LeafSendsFileToNewPeer{" + "type=" + type + ", leafId=" + leafId + '}';
    }

}
