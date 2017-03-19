package proj.pastry.util;

/**
 *
 * @author namanrs
 */
public class Constants {

    public static final String NO_IDS_REGISTERED_STRING = "-1";
    public static final int NO_IDS_REGISTERED_INT = -1;
    public static final String FILE_STORE_LOCATION = "/tmp/cs555_namanrs";

    public static class MESSAGES {

        public static final byte PEER_SENDS_REGISTRATION_REQUEST = 1;
        public static final byte DN_SENDS_REGISTRATION_RESPONSE = 2;
        public static final byte PEER_SENDS_JOIN_REQUEST = 3;
        public static final byte DESTINATION_SENDS_JOIN_RESPONSE = 4;
        public static final byte PEER_SENDS_UPDATE_LEAF = 5;
        public static final byte PEER_SENDS_UPDATE_RT = 6;
        public static final byte CLIENT_TO_DN_REQUEST_RANDOM_PEER = 7;
        public static final byte DN_SENDS_RANDOM_PEER_TO_CLIENT = 8;
        public static final byte LOOKUP_DESTINATION_FOR_FILE_KEY_STORE = 9;
        public static final byte DESTINATION_SENDS_LOOKUP_RESPONSE_FOR_STORE = 10;
        public static final byte STORE_FILE = 11;
        public static final byte DESTINATION_SENDS_LOOKUP_RESPONSE_FOR_RETRIEVE = 12;
        public static final byte PEER_REQUESTS_FILES_WHILE_JOINING = 13;
        public static final byte LEAF_SENDS_FILE_TO_NEW_PEER = 14;
        public static final byte PEER_LEAVES_PASTRY = 15;
        public static final byte STORE_FILE_FROM_LEAVING_PEER = 16;
        public static final byte PEER_SENDS_ACK_REGISTRATION = 17;
        public static final byte PEER_SENDS_RE_REGISTRATION_REQUEST = 18;
    }

    public static class DELIMITORS {

        public static final char IP_PORT_DELIM = ':';
        public static final String TRACEPATH_DELIM = "-";
    }

    public static class DIRECTION {

        public static final char RIGHT = 'R';
        public static final char LEFT = 'L';
    }

    public static class OPERATION {

        public static final char STORE = 'S';
        public static final char RETRIEVE = 'R';
    }

    public static class COMMANDS {

        public static class PEER {

            public static final String SELF_INFO = "printinfo";
            public static final String LEAVE_PASTRY = "leave";
            public static final String LIST_FILES = "listfiles";
        }

        public static class DISCOVERY_NODE {

            public static final String LIST_PEERS = "listpeers";
        }

        public static class STORE_CLINET {

            public static final String STORE_FILE = "store";
            public static final String RETRIEVE_FILE = "retrieve";
        }
    }
}
