package cyclon.system.peer.cyclon;

import java.util.LinkedList;


import se.sics.kompics.Event;
import se.sics.kompics.address.Address;


public class CyclonSample extends Event {
	LinkedList<PeerDescriptor> nodes = new LinkedList<PeerDescriptor>();


	public CyclonSample(LinkedList<PeerDescriptor> nodes) {
		this.nodes = nodes;
	}
        
	public CyclonSample() {
	}


	public LinkedList<PeerDescriptor> getSample() {
		return this.nodes;
	}
}
