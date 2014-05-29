package system.peer;


import se.sics.kompics.Event;

public final class StartApplication extends Event {

    private final int numCpus;
    private final int amountMem;

    public StartApplication(int numCpus, int amountMem) {
        this.numCpus = numCpus;
        this.amountMem = amountMem;
    }

    public int getNumCpus() {
        return numCpus;
    }

    public int getAmountMem() {
        return amountMem;
    }
    
}
