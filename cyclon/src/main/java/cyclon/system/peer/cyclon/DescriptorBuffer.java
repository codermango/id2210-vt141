package cyclon.system.peer.cyclon;


import java.io.Serializable;
import java.util.LinkedList;
import se.sics.kompics.address.Address;


public class DescriptorBuffer implements Serializable {
	private static final long serialVersionUID = -4414783055393007206L;
	private final Address from;
	private final LinkedList<PeerDescriptor> descriptors;


	public DescriptorBuffer(Address from,
			LinkedList<PeerDescriptor> descriptors) {
		super();
		this.from = from;
		this.descriptors = descriptors;
	}


	public Address getFrom() {
		return from;
	}


	public int getSize() {
		return descriptors.size();
	}


	public LinkedList<PeerDescriptor> getDescriptors() {
		return descriptors;
	}
}
