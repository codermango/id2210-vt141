package tman.system.peer.tman;

import java.util.Comparator;

import cyclon.system.peer.cyclon.PeerDescriptor;

public class ComparatorByMemory implements Comparator<PeerDescriptor> {

	private PeerDescriptor self;

	public ComparatorByMemory(PeerDescriptor self) {
		this.self = self;
	}

	@Override
	public int compare(PeerDescriptor p1, PeerDescriptor p2) {

		int amountMem1 = p1.getAv().getFreeMemInMbs();
		int amountMem2 = p2.getAv().getFreeMemInMbs();
		int amountSelfMem = self.getAv().getFreeMemInMbs();

		if (amountMem1 < amountSelfMem && amountMem2 > amountSelfMem) {
			return 1;
		} else if (amountMem1 > amountSelfMem && amountMem2 < amountSelfMem) {
			return -1;
		} else if (Math.abs(amountMem1 - amountSelfMem) < Math.abs(amountMem2 - amountSelfMem)) {
			return -1;
		} else if (Math.abs(amountMem1 - amountSelfMem) > Math.abs(amountMem2 - amountSelfMem)) {
			return 1;
		}
		return 0;
	}

}
