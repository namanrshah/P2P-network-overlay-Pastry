package proj.pastry.peer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class PeerInfo {

    String id;
    String ip;
    int port;
    String nickname;

    public PeerInfo(String id, String ip, int port, String nickname) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.nickname = nickname;
    }

    public PeerInfo(byte[] marshalledBytes) {
        try {
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
            int idLen = din.readInt();
            byte[] idBytes = new byte[idLen];
            din.readFully(idBytes);
            id = new String(idBytes);
            int ipLen = din.readInt();
            byte[] ipBytes = new byte[ipLen];
            din.readFully(ipBytes);
            ip = new String(ipBytes);
            port = din.readInt();
            int nicknameLen = din.readInt();
            byte[] nicknameBytes = new byte[nicknameLen];
            din.readFully(nicknameBytes);
            nickname = new String(nicknameBytes);
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
            dout.writeInt(id.length());
            dout.write(id.getBytes());
            dout.writeInt(ip.length());
            dout.write(ip.getBytes());
            dout.writeInt(port);
            dout.writeInt(nickname.length());
            dout.write(nickname.getBytes());
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

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    @Override
    public String toString() {
        return "PeerInfo{" + "id=" + id + ", ip=" + ip + ", port=" + port + ", nickname=" + nickname + '}';
    }

}
