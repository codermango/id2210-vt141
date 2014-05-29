package resourcemanager.system.peer.rm;

import java.util.Comparator;

public class ComparatorByQueueSize implements Comparator<ResponseMsg> {

    @Override
    public int compare(ResponseMsg arg0, ResponseMsg arg1) {
        if (arg0.getQueueSize() > arg1.getQueueSize()) {
            return 1;
        } else if (arg0.getQueueSize() < arg1.getQueueSize()) {
            return -1;
        }
        return 0;
    }

}
