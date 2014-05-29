package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * User: jdowling
 */
public class RequestResources {

    private int numCpus;
    private int amountMem;
    private int time;
    int waitingResponses;
    ResponseMsg bestResponse = null;

    ArrayList<ResponseMsg> responses;

    public RequestResources(int numCpus, int amountMem, int time, int waitingResponses) {
        this.numCpus = numCpus;
        this.amountMem = amountMem;
        this.time = time;
        this.waitingResponses = waitingResponses;

        responses = new ArrayList<ResponseMsg>();
    }

    public int getNumCpus() {
        return numCpus;
    }

    public int getAmountMem() {
        return amountMem;
    }

    public int getTime() {
        return time;
    }

    public ArrayList<ResponseMsg> getResponses() {
        return responses;
    }

    public ArrayList<ResponseMsg> restoreResponses(ResponseMsg res) {
        responses.add(res);
        return responses;
    }

    public ArrayList<ResponseMsg> sortResponses() {
        Collections.sort(responses, new ComparatorByQueueSize());
        return responses;
    }

    public ResponseMsg findBestResponse(ResponseMsg res) {
        if (bestResponse == null) {
            bestResponse = res;
        } else if (!bestResponse.isSuccess() && res.isSuccess()) {
            bestResponse = res;
        } else if (res.getQueueSize() < bestResponse.getQueueSize()) {
            bestResponse = res;
        }

        return bestResponse;
    }

    public static class RequestTimeout extends Timeout {

        private final Address destination;

        RequestTimeout(ScheduleTimeout st, Address destination) {
            super(st);
            this.destination = destination;
        }

        public Address getDestination() {
            return destination;
        }
    }
}
