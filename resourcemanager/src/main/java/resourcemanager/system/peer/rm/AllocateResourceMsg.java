package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

public class AllocateResourceMsg extends Message {

    private static final long serialVersionUID = 6196336565685562525L;
    private final int numCPU;
    private final int amountMem;
    private final int time;
    private final long requestID;

    private final long startTime;

    public AllocateResourceMsg(Address source, Address destination, int numCPU, int amountMem, int time, long requestID, long startTime) {
        super(source, destination);
        this.numCPU = numCPU;
        this.amountMem = amountMem;
        this.time = time;
        this.requestID = requestID;

        this.startTime = startTime;
    }

    public int getNumCPU() {
        return numCPU;
    }

    public int getAmountMem() {
        return amountMem;
    }

    public int getTime() {
        return time;
    }

    public long getRequestID() {
        return requestID;
    }

    public long getStartTime() {
        return startTime;
    }

}
