package proj.pastry.discoverynode;

import java.util.logging.Logger;

/**
 *
 * @author namanrs
 */
public class NodeRegistrationTracker implements Runnable {

    DiscoveryNode DiscoveryNode;
    Logger logger = Logger.getLogger(getClass().getName());

    public NodeRegistrationTracker(DiscoveryNode DiscoveryNode) {
        this.DiscoveryNode = DiscoveryNode;
    }

    @Override
    public void run() {

    }

}
