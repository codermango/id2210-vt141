package tman.simulator.snapshot;

import java.util.LinkedList;

import cyclon.system.peer.cyclon.PeerDescriptor;
import se.sics.kompics.address.Address;


public class PeerInfo {
	private LinkedList<PeerDescriptor> tmanPartnersByRes;
	private LinkedList<PeerDescriptor> tmanPartnersByCpu;
	private LinkedList<PeerDescriptor> tmanPartnersByMem;
	private LinkedList<Address> cyclonPartners;


	public PeerInfo() {
		this.tmanPartnersByRes = new LinkedList<PeerDescriptor>();
		this.tmanPartnersByCpu = new LinkedList<PeerDescriptor>();
		this.tmanPartnersByMem = new LinkedList<PeerDescriptor>();
		this.cyclonPartners = new LinkedList<Address>();
	}


	public void updateTManPartnersByRes(LinkedList<PeerDescriptor> partners) {
		this.tmanPartnersByRes = partners;
	}
	
	public void updateTManPartnersByCpu(LinkedList<PeerDescriptor> partners) {
		this.tmanPartnersByCpu = partners;
	}
	
	public void updateTManPartnersByMem(LinkedList<PeerDescriptor> partners) {
		this.tmanPartnersByMem = partners;
	}

	public void updateCyclonPartners(LinkedList<Address> partners) {
		this.cyclonPartners = partners;
	}


	public LinkedList<PeerDescriptor> getTManPartnersByRes() {
		return this.tmanPartnersByRes;
	}
	
	public LinkedList<PeerDescriptor> getTManPartnersByCpu() {
		return this.tmanPartnersByCpu;
	}
	
	public LinkedList<PeerDescriptor> getTManPartnersByMem() {
		return this.tmanPartnersByMem;
	}


	public LinkedList<Address> getCyclonPartners() {
		return this.cyclonPartners;
	}
}
