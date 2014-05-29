package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.AllocateResourcesManyMachines;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import system.peer.RmPort;
import tman.system.peer.tman.ComparatorByCPU;
import tman.system.peer.tman.ComparatorByMemory;
import tman.system.peer.tman.ComparatorByResources;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;
import simulator.snapshot.Snapshot;

/**
 * Should have some comments here.
 *
 */
public final class ResourceManager extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    Positive<RmPort> indexPort = requires(RmPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    Negative<Web> webPort = provides(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = requires(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanPort = requires(TManSamplePort.class);
    LinkedList<PeerDescriptor> cyclonPartners = new LinkedList<PeerDescriptor>();
    LinkedList<PeerDescriptor> tmanPartners = new LinkedList<PeerDescriptor>();
    LinkedList<PeerDescriptor> tmanPartnersByResource = new LinkedList<PeerDescriptor>();
    LinkedList<PeerDescriptor> tmanPartnersByCPU = new LinkedList<PeerDescriptor>();
    LinkedList<PeerDescriptor> tmanPartnersByMemory = new LinkedList<PeerDescriptor>();
    
    private Map<Long, Long> restoreTimeMap = new HashMap<Long, Long>();

    int requestedNumCpus;
    int requestedNumMem;
    static int requestedNumMachines = 1;

    private Address self;
    private RmConfiguration configuration;
    private Random random;
    private AvailableResources availableResources;
    private static final int MAX_NUM_NODES = 10;

    private long startTime, endTime, averageTime;

    //true = cyclon
    public static boolean isCyclonOrTMAN = true;

    // queue where we put incoming requests for resources
    private Queue<AllocateResourceMsg> queue = new LinkedList<AllocateResourceMsg>();
    private Map<Long, RequestResources> requestResourcesMap = new HashMap<Long, RequestResources>();

    Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
        @Override
        public int compare(PeerDescriptor t, PeerDescriptor t1) {
            if (t.getAge() > t1.getAge()) {
                return 1;
            } else {
                return -1;
            }
        }
    };

    public ResourceManager() {

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleBatchRequest, indexPort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleJobFinishTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleAllocateResources, networkPort);
        subscribe(handleTManSample, tmanPort);
    }

//----------------------------------------------------------------------------------------------------------------------------------
    Handler<RmInit> handleInit = new Handler<RmInit>() {
        @Override
        public void handle(RmInit init) {
            self = init.getSelf();
            configuration = init.getConfiguration();
            random = new Random(init.getConfiguration().getSeed());
            availableResources = init.getAvailableResources();
            long period = configuration.getPeriod();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));

            trigger(rst, timerPort);

        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
        @Override
        public void handle(UpdateTimeout event) {

            // pick a random neighbour to ask for index updates from.
            // You can change this policy if you want to.
            // Maybe a gradient neighbour who is closer to the leader?
            //System.out.println(averageTime);
            if (cyclonPartners.isEmpty()) {
                return;
            }
            PeerDescriptor dest = cyclonPartners.get(random.nextInt(cyclonPartners.size()));

        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    /**
     * Handle incoming resource allocation request. Check if we have enough
     * resources. This is for Worker. It listens Request from the Scheduler
     * which probe the Worker.
     */
    Handler<RequestMsg> handleResourceAllocationRequest = new Handler<RequestMsg>() {
        @Override
        public void handle(RequestMsg event) {
			// System.out.println(self.getId() + " : Received Request for " +
            // event.getNumCpus() + " cpus and " + event.getAmountMemInMb()
            // + " memInMb" + " from peer with id " +
            // event.getSource().getId());
            // System.out.println("I have " +
            // availableResources.getNumFreeCpus() + " cpus and " +
            // availableResources.getFreeMemInMbs());

            // send a response event with a boolean value
            boolean isSuccess = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
            ResponseMsg response = new ResponseMsg(self, event.getSource(), isSuccess, queue.size(), event.getRequestID(), event.getStartTime());
            trigger(response, networkPort);

        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    /**
     * Handle incoming resource allocation response. Check if we get the
     * requested resources? This is for Scheduler. It listens to the Respond
     * from the Worker. The Respond is the probes that was sent before. If all
     * the probes returned, the decision will be made by the Scheduler. The
     * decison is decided according to the size of the job queue.
     */
    Handler<ResponseMsg> handleResourceAllocationResponse = new Handler<ResponseMsg>() {
        @Override
        public void handle(ResponseMsg event) {
//             System.out.println(self + " Got response from " +
//             event.getSource().getId());
//            System.out.println("Got allocate event for requestId " + event.getRequsetID());
            RequestResources requestResources = requestResourcesMap.get(event.getRequsetID());
            //System.out.println("bbbbbbbb"+requestResources);
            if (requestResources == null) {
                return;
            }
            //System.out.println("==========================================");
            requestResources.restoreResponses(event);
            requestResources.waitingResponses--;
            requestResourcesMap.put(event.getRequsetID(), requestResources);
            if (requestResources.waitingResponses == 0) {
                for (int i = 0; i < requestedNumMachines; i++) {
                    AllocateResourceMsg allocateResource = new AllocateResourceMsg(self, requestResources.getResponses().get(i).getSource(), requestResources.getNumCpus(), requestResources.getAmountMem(), requestResources.getTime(), event.getRequsetID(), event.getStartTime());
                    trigger(allocateResource, networkPort);
                }
            }
        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    /**
     * This is for Worker. Put the job in the queue if there is not enough
     * resources. Allocate the resources if there is enough resources.
     */
    Handler<AllocateResourceMsg> handleAllocateResources = new Handler<AllocateResourceMsg>() {

        @Override
        public void handle(AllocateResourceMsg event) {

            boolean isSuccess = availableResources.isAvailable(event.getNumCPU(), event.getAmountMem());
            if (!isSuccess) {
                if (!queue.contains(event)) {
                    queue.add(event);
                }
            } else {
                
                long timeToFindResources = System.currentTimeMillis() - event.getStartTime();
		Snapshot.addTime(event.getRequestID(), timeToFindResources);
                availableResources.allocate(event.getNumCPU(), event.getAmountMem());
                if (!queue.isEmpty()) {
                    queue.remove();
                }

                ScheduleTimeout st = new ScheduleTimeout(event.getTime());
                st.setTimeoutEvent(new JobFinishTimeout(st, event.getNumCPU(), event.getAmountMem()));
                trigger(st, timerPort);

            }
        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    /**
     * This is for Worker. Allocate the resources if there is enough resources.
     */
    Handler<JobFinishTimeout> handleJobFinishTimeout = new Handler<JobFinishTimeout>() {

        @Override
        public void handle(JobFinishTimeout event) {

            availableResources.release(event.getNumCPU(), event.getAmountMem());
            if (!queue.isEmpty()) {
                AllocateResourceMsg allocateResourceMsg = queue.peek();
                trigger(allocateResourceMsg, networkPort);
            }
        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {

            // receive a new list of cyclonPartners
            cyclonPartners.clear();
            cyclonPartners.addAll(event.getSample());

        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    Handler<AllocateResourcesManyMachines> handleBatchRequest = new Handler<AllocateResourcesManyMachines>() {

        @Override
        public void handle(AllocateResourcesManyMachines event) {

            long startTime = System.currentTimeMillis();

            event.setStartTime(startTime);
            setRequestedNumMachines(event.getNumMachines());

            int numCPU = event.getNumCpus();
            int amountMemory = event.getMemoryInMbs();

            if (numCPU == 0) {
                tmanPartners = tmanPartnersByMemory;
            } else if (amountMemory == 0) {
                tmanPartners = tmanPartnersByCPU;
            } else {
                tmanPartners = tmanPartnersByResource;
            }

            if (isCyclonOrTMAN) {
                if (cyclonPartners.size() >= event.getNumMachines() && cyclonPartners.size() <= MAX_NUM_NODES) {
                    requestResourcesMap.put(event.getId(), new RequestResources(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), cyclonPartners.size()));
                    for (PeerDescriptor dest : cyclonPartners) {
                        RequestMsg requestMsg = new RequestMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getId(), event.getStartTime());
                        trigger(requestMsg, networkPort);
                    }
                } else if (cyclonPartners.size() >= event.getNumMachines() && cyclonPartners.size() > MAX_NUM_NODES) {
                    requestResourcesMap.put(event.getId(), new RequestResources(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), MAX_NUM_NODES));
                    for (int i = 0; i < MAX_NUM_NODES; i++) {
                        int index = random.nextInt(cyclonPartners.size());
                        PeerDescriptor dest = cyclonPartners.get(index);
                        cyclonPartners.remove(index);
                        RequestMsg req = new RequestMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getId(), event.getStartTime());
                        trigger(req, networkPort);
                    }
                } else {
                    return;
                }
            } else {
                if (tmanPartners.size() >= event.getNumMachines()) {
                    for (int i = 0; i < requestedNumMachines; i++) {
                        PeerDescriptor dest = tmanPartners.get(i);
                        AllocateResourceMsg allocateResourceMsg = new AllocateResourceMsg(self, dest.getAddress(), event.getNumCpus(),
                                event.getMemoryInMbs(), event.getTimeToHoldResource(), event.getId(), event.getStartTime());
                        trigger(allocateResourceMsg, networkPort);
                    }
                    int index = 0;
                    PeerDescriptor dest = tmanPartners.get(index);
                    AllocateResourceMsg al = new AllocateResourceMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), event.getId(), event.getStartTime());
                    trigger(al, networkPort);
                }
            }
        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    /**
     * This is for Scheduler. RequestResource is from DataCenterSimulator. If
     * the cpu is empty, the tmanPartnersByMemory will be taken If the memory is
     * empty, the tmanPartnersByCPU will be taken
     */
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            // TODO: Ask for resources from cyclonPartners by sending a ResourceRequest

            event.setStartTime(System.currentTimeMillis());
            int numCPU = event.getNumCpus();
            int amountMemory = event.getMemoryInMbs();

            if (numCPU == 0) {
                tmanPartners = tmanPartnersByMemory;
            } else if (amountMemory == 0) {
                tmanPartners = tmanPartnersByCPU;
            } else {
                tmanPartners = tmanPartnersByResource;
            }

            if (isCyclonOrTMAN) {
                getCyclonSample(event);
            } else {
                getTmanSample(event);
            }

        }
    };

//----------------------------------------------------------------------------------------------------------------------------------
    private void getCyclonSample(RequestResource event) {
        if (cyclonPartners.size() <= MAX_NUM_NODES && !cyclonPartners.isEmpty()) {
            requestResourcesMap.put(event.getId(), new RequestResources(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), cyclonPartners.size()));
            for (PeerDescriptor dest : cyclonPartners) {
                RequestMsg request = new RequestMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getId(), event.getStartTime());
                trigger(request, networkPort);
            }
        } else if (cyclonPartners.size() > MAX_NUM_NODES && !cyclonPartners.isEmpty()) {
            requestResourcesMap.put(event.getId(), new RequestResources(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), MAX_NUM_NODES));
            for (int i = 0; i < MAX_NUM_NODES; i++) {
                //System.out.println("aaaaaaaaaaaaaa"+cyclonPartners.size());
                int index = random.nextInt(cyclonPartners.size());
                PeerDescriptor dest = cyclonPartners.get(index);
                cyclonPartners.remove(index);
                RequestMsg request = new RequestMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getId(), event.getStartTime());
                trigger(request, networkPort);
            }

        }
    }

//----------------------------------------------------------------------------------------------------------------------------------
    private void getTmanSample(RequestResource event) {
        if (tmanPartners.size() != 0) {

            //int index = random.nextInt(tmanPartners.size());
            int index = 0;
            PeerDescriptor dest = tmanPartners.get(index);
            AllocateResourceMsg al = new AllocateResourceMsg(self, dest.getAddress(), event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), event.getId(), event.getStartTime());
            trigger(al, networkPort);

        }
    }

//----------------------------------------------------------------------------------------------------------------------------------
    public static int getRequestedNumMachines() {
        return requestedNumMachines;
    }

//----------------------------------------------------------------------------------------------------------------------------------
    public void setRequestedNumMachines(int requestedNumMachines) {
        this.requestedNumMachines = requestedNumMachines;
    }

//----------------------------------------------------------------------------------------------------------------------------------
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {

            tmanPartnersByResource.clear();
            tmanPartnersByCPU.clear();
            tmanPartnersByMemory.clear();
            tmanPartnersByResource.addAll(event.getPartnersByRes());
            tmanPartnersByCPU.addAll(event.getPartnersByCpu());
            tmanPartnersByMemory.addAll(event.getPartnersByMem());
            Collections.sort(tmanPartnersByResource, new ComparatorByResources(new PeerDescriptor(self, availableResources)));
            Collections.sort(tmanPartnersByCPU, new ComparatorByCPU(new PeerDescriptor(self, availableResources)));
            Collections.sort(tmanPartnersByMemory, new ComparatorByMemory(new PeerDescriptor(self, availableResources)));

        }
    };

}
