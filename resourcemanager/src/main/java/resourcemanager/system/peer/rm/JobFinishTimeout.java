package resourcemanager.system.peer.rm;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class JobFinishTimeout extends Timeout {

    private int numCPU;
    private int amountMem;

    protected JobFinishTimeout(ScheduleTimeout request, int numCPU, int amountMem) {
        super(request);
        this.numCPU = numCPU;
        this.amountMem = amountMem;
    }

    public int getNumCPU() {
        return numCPU;
    }

    public int getAmountMem() {
        return amountMem;
    }

}
