package common.peer;

import common.simulation.PeerJoin;
import se.sics.kompics.Event;

public class JoinPeer extends Event {

    private final Long peerId;
    private final int numFreeCpus;
    private final int freeMemoryInMbs;

    public JoinPeer(PeerJoin pj) {
        this.peerId = pj.getPeerId();
        this.numFreeCpus = pj.getNumFreeCpus();
        this.freeMemoryInMbs = pj.getFreeMemoryInMbs();
    }

    public int getFreeMemoryInMbs() {
        return freeMemoryInMbs;
    }

    public int getNumFreeCpus() {
        return numFreeCpus;
    }

    public Long getPeerId() {
        return this.peerId;
    }
}
