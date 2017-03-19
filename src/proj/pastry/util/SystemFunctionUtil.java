package proj.pastry.util;

/**
 *
 * @author namanrs
 */
public class SystemFunctionUtil {

    //Returns 
    //distance =  distance between peer1 and peer2
    //direction = peer2's direction with reference from peer1
    public static DistanceDirection getDistanceAndDirection(String peer1Id, String peer2Id) {
        int d1 = Integer.parseInt(peer1Id, 16);
//        System.out.println(d1);
        int d2 = Integer.parseInt(peer2Id, 16);
//        System.out.println(d2);
        int diff1 = d1 - d2;
        int absDiff = Math.abs(diff1);
//        System.out.println(absDiff);
//        System.out.println((65536 - absDiff));
        char dir;
        int ans = 0;
        if (d1 < d2) {
            if (absDiff < (65536 - absDiff)) {
                dir = Constants.DIRECTION.RIGHT;
                ans = absDiff;
            } else {
                dir = Constants.DIRECTION.LEFT;
                ans = 65536 - absDiff;
            }
        } else {
            if (absDiff < (65536 - absDiff)) {
                dir = Constants.DIRECTION.LEFT;
                ans = absDiff;
            } else {
                dir = Constants.DIRECTION.RIGHT;
                ans = 65536 - absDiff;
            }
        }
        return new DistanceDirection(dir, ans);
    }

//    private static String nearerNode(String peer, String comparePeer1, String comparePeer2) {
//        DistanceDirection dd1 = IDManipulation.getDistanceAndDirection(peer, comparePeer1);
//        DistanceDirection dd2 = IDManipulation.getDistanceAndDirection(peer, comparePeer2);
//
//        int d1 = dd1.getDistance();
//        int d2 = dd2.getDistance();
//
//        String nearerNode = (d1 < d2) ? comparePeer1 : comparePeer2;
//        if (d1 == d2) {
//            if (dd1.getDirection() == Protocol.DIRECTION.RIGHT) {
//                nearerNode = comparePeer1;
//            } else if (dd2.getDirection() == Protocol.DIRECTION.RIGHT) {
//                nearerNode = comparePeer2;
//            }
//        }
//        return nearerNode;
//    }
    public static int matchPrefix(String peerId1, String peerId2) {
        int indexToReturn = 0;
        int peerId1Length = peerId1.length();
        int peerId2Length = peerId2.length();
        int maxItrCount = peerId1Length < peerId2Length ? peerId1Length : peerId2Length;
        for (int i = 0; i < maxItrCount; i++) {
            if (peerId1.charAt(i) != peerId2.charAt(i)) {
                indexToReturn = i;
                break;
            }
        }
        return indexToReturn;
    }

    public static int countDistanceInDirection(char direction, String node1Id, String node2Id) {
        int distance = 0;
        int d1 = Integer.parseInt(node1Id, 16);
        int d2 = Integer.parseInt(node2Id, 16);
        if (direction == Constants.DIRECTION.RIGHT) {
            if (d1 < d2) {
                distance = d2 - d1;
            } else {
                distance = 65536 - d1 + d2;
            }
        } else if (direction == Constants.DIRECTION.LEFT) {
            if (d1 < d2) {
                distance = 65536 - d2 + d1;
            } else {
                distance = d1 - d2;
            }
        }
        return distance;
    }
}
