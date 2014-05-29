package tman.system.peer.tman;

import java.util.Comparator;

import cyclon.system.peer.cyclon.PeerDescriptor;

public class ComparatorByCPU implements Comparator<PeerDescriptor>{

	private final PeerDescriptor self;
	
	public ComparatorByCPU(PeerDescriptor self) {
		this.self = self;
	}
	
	@Override
	public int compare(PeerDescriptor p1, PeerDescriptor p2) {
		
		int numCPU1 = p1.getAv().getNumFreeCpus();
		int numCPU2 = p2.getAv().getNumFreeCpus();
		int numSelfCPU = self.getAv().getNumFreeCpus();
		
		if( numCPU1 < numSelfCPU && numCPU2 > numSelfCPU ) {
			return 1;
		}
		else if( numCPU1 > numSelfCPU && numCPU2 <  numSelfCPU){
			return -1;
		}
		else if( Math.abs(numCPU1 - numSelfCPU ) < Math.abs(numCPU2 - numSelfCPU) ) {
			return -1;
		}
		else if( Math.abs(numCPU1 - numSelfCPU ) > Math.abs(numCPU2 - numSelfCPU) ) {
			return 1;
		}
		return 0;
	}
}
