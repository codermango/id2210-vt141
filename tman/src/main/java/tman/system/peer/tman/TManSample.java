package tman.system.peer.tman;

import java.util.LinkedList;



import cyclon.system.peer.cyclon.PeerDescriptor;
import se.sics.kompics.Event;


public class TManSample extends Event {
	LinkedList<PeerDescriptor> partnersByRes = new LinkedList<PeerDescriptor>();
	LinkedList<PeerDescriptor> partnersByCpu = new LinkedList<PeerDescriptor>();
	LinkedList<PeerDescriptor> partnersByMem = new LinkedList<PeerDescriptor>();

	public TManSample(LinkedList<PeerDescriptor> partnersByRes, LinkedList<PeerDescriptor> partnersByCpu,
			LinkedList<PeerDescriptor> partnersByMem) {
		this.partnersByRes= partnersByRes;
		this.partnersByCpu = partnersByCpu;
		this.partnersByMem = partnersByMem;
	}
        
	public LinkedList<PeerDescriptor> getPartnersByRes() {
		return partnersByRes;
	}

	public LinkedList<PeerDescriptor> getPartnersByCpu() {
		return partnersByCpu;
	}

	public LinkedList<PeerDescriptor> getPartnersByMem() {
		return partnersByMem;
	}

}
