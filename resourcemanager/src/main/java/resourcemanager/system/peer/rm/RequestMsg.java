package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

public class RequestMsg extends Message {

    private static final long serialVersionUID = -1719815843936782731L;
    private final int numCpus;
    private final int amountMemInMb;
    private final long requestID;
    
    private final long startTime;

    public RequestMsg(Address source, Address destination, int numCpus, int amountMemInMb, long requestID, long startTime) {
        super(source, destination);
        this.numCpus = numCpus;
        this.amountMemInMb = amountMemInMb;
        this.requestID = requestID;
        
        this.startTime = startTime;
    }

    public int getAmountMemInMb() {
        return amountMemInMb;
    }

    public int getNumCpus() {
        return numCpus;
    }

    public long getRequestID() {
        return requestID;
    }

    public long getStartTime() {
        return startTime;
    }
    
    
}
