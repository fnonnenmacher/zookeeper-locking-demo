@Grab('org.apache.zookeeper:zookeeper:3.5.4-beta')
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL
import static org.apache.zookeeper.CreateMode.PERSISTENT
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE

ResourceLockingClient client = new ResourceLockingClient('localhost:2181', '/_lockparent_')

client.lock();
println 'Resource is locked'

println 'Press enter to unlock resource'
System.in.newReader().readLine()

client.unlock();
println 'Resource unlocked'

/**
 * Client class which is used to lock or unlock a resource
 */
class ResourceLockingClient {
    private final String name;
    private final ZooKeeper zk
    private String myLockNode;

    ResourceLockingClient(String connectionString, String resourcename) {

        def watcher = new WaitingWatcher();
        zk = new ZooKeeper(connectionString, 1000, watcher);
        watcher.waitForWatchEvent();

        if (!zk.exists(resourcename, null)) {
            zk.create(resourcename, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
        }

        this.name = resourcename;
    }

    void lock() {

        def res = zk.create("$name/lock-", new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
        println("\tCreated znode '$res'")

        myLockNode = res.split('/').last();

        while (true) {

            List<String> children = zk.getChildren(name, false);

            //sort, so that all nodes are sorted with increasing sequential numbers
            children.sort()

            //check if client has current lock
            if (children.get(0).equals(myLockNode)) {
                println("\tLock received! (with znode '$name/$myLockNode')")
                return;
            }
            println("\tResource is already locked. Wait until its released!");

            //find predecessor to watch its node
            int ourIndex = children.findIndexOf { it.equals(myLockNode) }
            String predecessor = children.get(ourIndex - 1)
            println("\tPredecessor znode identified: '$name/$predecessor'")

            def watcher = new WaitingWatcher();
            boolean predecessorExists = zk.exists("$name/$predecessor", watcher)

            if (predecessorExists) {

                println("\tWatcher on predecessor znode '$name/$predecessor' created.")

                watcher.waitForWatchEvent();

                println("\tWatch event for predecessor znode '$name/$predecessor' received.")
            }
        }
    }

    void unlock() {
        zk.delete("$name/$myLockNode", -1)
        println("\tznode '$name/$myLockNode' deleted.")
        myLockNode = null;
    }
}

/**
 * Pseudo watcher which helps to make asynchronous calls synchronous. (Blocks until watch event has been received)
 */
class WaitingWatcher implements Watcher {
    private boolean received = false;

    @Override
    void process(WatchedEvent watchedEvent) {
        received = true;
    }

    void waitForWatchEvent() {
        while (!received) {
            sleep(50);
        }
    }
}

