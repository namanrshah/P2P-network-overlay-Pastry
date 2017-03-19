
package proj.pastry.util;

/**
 *
 * @author namanrs
 */
public class DistanceDirection {

    private char direction;
    private int distance;

    public char getDirection() {
        return direction;
    }

    public DistanceDirection(char direction, int distance) {
        this.direction = direction;
        this.distance = distance;
    }

    public void setDirection(char direction) {
        this.direction = direction;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "DistanceDirection{" + "direction=" + direction + ", distance=" + distance + '}';
    }

}
