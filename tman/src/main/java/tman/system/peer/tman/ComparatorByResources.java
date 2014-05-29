package tman.system.peer.tman;

import java.util.Comparator;

import cyclon.system.peer.cyclon.PeerDescriptor;

public class ComparatorByResources implements Comparator<PeerDescriptor> {
	

	private PeerDescriptor self;

	public ComparatorByResources(PeerDescriptor self) {
		this.self = self;
	}

	@Override
	public int compare(PeerDescriptor p1, PeerDescriptor p2) {
		
		
		int resource1 = p1.getAv().getFreeMemInMbs() * p1.getAv().getNumFreeCpus();
		int resource2 = p2.getAv().getFreeMemInMbs() * p2.getAv().getNumFreeCpus();
		int selfResource = self.getAv().getFreeMemInMbs() * self.getAv().getNumFreeCpus();

		if (resource1 < selfResource && resource2 > selfResource) {
			return 1;
		} else if (resource1 > selfResource && resource2 < selfResource) {
			return -1;
		} else if (Math.abs(resource1 - selfResource) < Math.abs(resource2 - selfResource)) {
			return -1;
		} else if (Math.abs(resource1 - selfResource) > Math.abs(resource2 - selfResource)) {
			return 1;
		}
		
		return 0;
	}

}
