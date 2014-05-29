package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

public class ResponseMsg extends Message {

    /**
     *
     */
    private static final long serialVersionUID = -2871499945624179185L;
    private final boolean success;
    private final long requsetID;
    private final int queueSize;
    
    private final long startTime;
    

    public ResponseMsg(Address source, Address destination, boolean success, int queueSize, long requsetID, long startTime) {
        super(source, destination);
        this.success = success;
        this.requsetID = requsetID;
        this.queueSize = queueSize;
        
        this.startTime = startTime;
    }

    public long getRequsetID() {
        return requsetID;
    }

    

    public int getQueueSize() {
        return queueSize;
    }

    public boolean isSuccess() {
        return success;
    }

    

    public long getStartTime() {
        return startTime;
    }
    
    
}
