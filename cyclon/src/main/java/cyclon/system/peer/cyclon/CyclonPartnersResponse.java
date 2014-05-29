package cyclon.system.peer.cyclon;

import java.util.LinkedList;


import se.sics.kompics.Event;
import se.sics.kompics.address.Address;


public class CyclonPartnersResponse extends Event {
	LinkedList<PeerDescriptor> partners = new LinkedList<PeerDescriptor>();


	public CyclonPartnersResponse(LinkedList<PeerDescriptor> partners) {
		this.partners = partners;
	}
        
	public CyclonPartnersResponse() {
	}


	public LinkedList<PeerDescriptor> getPartners() {
		return this.partners;
	}
}
