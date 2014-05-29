package simulator.snapshot;

import common.peer.AvailableResources;

import java.util.ArrayList;

import cyclon.system.peer.cyclon.PeerDescriptor;
import se.sics.kompics.address.Address;

public class PeerInfo {

    private ArrayList<Address> neighbours;
    private ArrayList<PeerDescriptor> tmanPartners;
    
    private final AvailableResources availableResources;

    public PeerInfo(AvailableResources availableResources) {
        this.neighbours = new ArrayList<Address>();
        this.availableResources = availableResources;
    }

    public int getNumFreeCpus() {
        return availableResources.getNumFreeCpus();
    }
    
    public int getFreeMemInMbs() {
        return availableResources.getFreeMemInMbs();
    }
    
    public synchronized void setNeighbours(ArrayList<Address> partners) {
        this.neighbours = partners;
    }

    public synchronized ArrayList<Address> getNeighbours() {
        return new ArrayList<Address>(neighbours);
    }

	public ArrayList<PeerDescriptor> getTmanPartners() {
		return tmanPartners;
	}

	public void setTmanPartners(ArrayList<PeerDescriptor> tmanPartners) {
		this.tmanPartners = tmanPartners;
	}

	public AvailableResources getAvailableResources() {
		return availableResources;
	}
}