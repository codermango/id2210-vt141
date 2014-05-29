package tman.system.peer.tman;

import java.util.Comparator;
import se.sics.kompics.address.Address;

public class ComparatorById implements Comparator<Address> {
    Address self;

    public ComparatorById(Address self) {
        this.self = self;
    }

    @Override
    public int compare(Address a1, Address a2) {
        assert (a1.getId() == a2.getId());
        if (a1.getId() < self.getId() && a2.getId() > self.getId()) {
            return 1;
        } else if (a2.getId() < self.getId() && a1.getId() > self.getId()) {
            return -1;
        } else if (Math.abs(a1.getId() - self.getId()) < Math.abs(a2.getId() - self.getId())) {
            return -1;
        }
        return 1;
    }
    
}
