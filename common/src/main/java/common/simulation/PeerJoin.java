package common.simulation;

import java.io.Serializable;
import se.sics.kompics.Event;

public final class PeerJoin extends Event implements Serializable {

    private final Long peerId;
    private final int numFreeCpus;
    private final int freeMemoryInMbs;

    public PeerJoin(Long peerId, int numFreeCpus, int freeMemoryInMbs) {
        this.peerId = peerId;
        this.numFreeCpus = numFreeCpus;
        this.freeMemoryInMbs = freeMemoryInMbs;
    }

    public int getNumFreeCpus() {
        return numFreeCpus;
    }

    public int getFreeMemoryInMbs() {
        return freeMemoryInMbs;
    }

    public Long getPeerId() {
        return this.peerId;
    }
}
