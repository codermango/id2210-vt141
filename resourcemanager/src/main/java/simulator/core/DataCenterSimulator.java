package simulator.core;

import common.simulation.SimulatorPort;

import java.util.HashMap;
import java.util.Iterator;

import resourcemanager.system.peer.rm.ResourceManager;
import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import system.peer.Peer;
import system.peer.PeerInit;
import simulator.snapshot.Snapshot;
import common.configuration.RmConfiguration;
import common.configuration.Configuration;
import common.configuration.CyclonConfiguration;
import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.simulation.AllocateResourcesManyMachines;
import common.simulation.ConsistentHashtable;
import common.simulation.GenerateReport;
import common.simulation.PeerFail;
import common.simulation.PeerJoin;
import common.simulation.RequestResource;
import common.simulation.SimulatorInit;

import java.net.InetAddress;
import java.util.Random;

import cyclon.system.peer.cyclon.PeerDescriptor;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.stat.descriptive.rank.Percentile;
import se.sics.ipasdistances.AsIpGenerator;
import system.peer.RmPort;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public final class DataCenterSimulator extends ComponentDefinition {

    Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
    Positive<Network> network = positive(Network.class);
    Positive<Timer> timer = positive(Timer.class);
    private final HashMap<Long, Component> peers;
    private final HashMap<Long, Address> peersAddress;
    private BootstrapConfiguration bootstrapConfiguration;
    private CyclonConfiguration cyclonConfiguration;
    private RmConfiguration rmConfiguration;
    private TManConfiguration tmanConfiguration;
    private Long identifierSpaceSize;
    private ConsistentHashtable<Long> ringNodes;
    private AsIpGenerator ipGenerator = AsIpGenerator.getInstance(125);

    Random r = new Random(System.currentTimeMillis());

    public DataCenterSimulator() {
        peers = new HashMap<Long, Component>();
        peersAddress = new HashMap<Long, Address>();
        ringNodes = new ConsistentHashtable<Long>();

        subscribe(handleInit, control);
        subscribe(handleGenerateReport, timer);
        subscribe(handlePeerJoin, simulator);
        subscribe(handlePeerFail, simulator);
        subscribe(handleTerminateExperiment, simulator);
        subscribe(handleRequestResource, simulator);
        subscribe(handleBatchRequest, simulator);
    }

    Handler<SimulatorInit> handleInit = new Handler<SimulatorInit>() {
        @Override
        public void handle(SimulatorInit init) {
            peers.clear();

            bootstrapConfiguration = init.getBootstrapConfiguration();
            cyclonConfiguration = init.getCyclonConfiguration();
            tmanConfiguration = init.getTmanConfiguration();
            rmConfiguration = init.getAggregationConfiguration();

            identifierSpaceSize = cyclonConfiguration.getIdentifierSpaceSize();

            // generate periodic report
            int snapshotPeriod = Configuration.SNAPSHOT_PERIOD;
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod, snapshotPeriod);
            spt.setTimeoutEvent(new GenerateReport(spt));
            trigger(spt, timer);

        }
    };

    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {

            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            trigger(event, peer.getNegative(RmPort.class));

        }
    };

    Handler<AllocateResourcesManyMachines> handleBatchRequest = new Handler<AllocateResourcesManyMachines>() {

        @Override
        public void handle(AllocateResourcesManyMachines event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            trigger(event, peer.getNegative(RmPort.class));
        }
    };

//-----------------------------------------------------------------------------------------
    Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
        @Override
        public void handle(PeerJoin event) {
            Long id = event.getPeerId();

            // join with the next id if this id is taken
            Long successor = ringNodes.getNode(id);

            while (successor != null && successor.equals(id)) {
                id = (id + 1) % identifierSpaceSize;
                successor = ringNodes.getNode(id);
            }

            createAndStartNewPeer(id, event.getNumFreeCpus(),
                    event.getFreeMemoryInMbs());

            System.out.println("=============================================");
            for (Long a : peersAddress.keySet()) {
                System.out.println("Peer starts. It is id is : " + a);
            }
            System.out.println("=============================================");

            ringNodes.addNode(id);
        }
    };

    Handler<PeerFail> handlePeerFail = new Handler<PeerFail>() {
        @Override
        public void handle(PeerFail event) {
            Long id = ringNodes.getNode(event.getId());

            if (ringNodes.size() == 0) {
                System.err.println("Empty network");
                return;
            }

            ringNodes.removeNode(id);
            stopAndDestroyPeer(id);
        }
    };

    Handler<TerminateExperiment> handleTerminateExperiment = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            System.err.println("Finishing experiment - terminating....\n");

            long avg = 0;
            for (Long time : Snapshot.restoreTimeMap.values()) {
                //System.out.println(time);
                avg += time;
            }
            avg = avg / Snapshot.restoreTimeMap.size();
            //TODO print results here
            System.err.println("Finished experiment...Generating Statistics");
            //System.out.println("ccccccccccc"+avg);
//
//            
            String line;
            double[] values = new double[100000];
            long sum = 0, average = 0;
            int total = 0;
            double percentile = 0;
            String which;
            if (ResourceManager.isCyclonOrTMAN) {
                which = ", Simple Sparrow";
            } else {
                which = ", Sparrow with gradient";
            }
//
            for (Long l : Snapshot.restoreTimeMap.values()) {
                total++;
                values[(int) total] = l;
            }
            percentile = getPercentile(values, total);
            //System.out.println("ccccccccc"+percentile);
            System.out.println("A total of " + total + " allocations of resources with " + peers.size() + " peers" + which + " with Jobs of " + ResourceManager.getRequestedNumMachines() + " task(s)\n"
                    + "Average time : " + avg + " ms\n"
                    + "99th Percentile : " + (int) percentile + " ms");
            Snapshot.restoreTimeMap.clear();

//            if (ResourceManager.isFlag()) {
//                ResourceManager.setFlag(false);
//            } else {
//                ResourceManager.setFlag(true);
//            }
            System.exit(0);
        }

    

};

private double getPercentile(double[] values, int total) {
        Percentile p = new Percentile();
        return p.evaluate(values, 0, total, 99.0);
    }

    Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
        @Override
        public void handle(GenerateReport event) {
            Snapshot.report();
        }
    };

    private 

void createAndStartNewPeer(long id, int numCpus, int memInMb) {
        //System.out.println("Starting a new peer with " + numCpus + " " + memInMb);
        Component peer = create(Peer.class  

    );
        InetAddress ip = ipGenerator.generateIP();
    Address address = new Address(ip, 8058, (int) id);

    connect(network, peer.getNegative

    (Network.class  

        ), new MessageDestinationFilter(address));
        connect(timer, peer.getNegative

        (Timer.class  

            ));

        AvailableResources ar = new AvailableResources(numCpus, memInMb);

            trigger(

            new PeerInit(address, bootstrapConfiguration, cyclonConfiguration,
                tmanConfiguration, rmConfiguration, ar), peer.getControl());

        trigger(

            new Start(), peer.getControl());
        peers.put (id, peer);

            peersAddress.put (id, address);

            Snapshot.addPeer (
        

        new PeerDescriptor(address, ar), ar);
    }

    private void stopAndDestroyPeer(Long id) {
            Component peer = peers.get(id);

            trigger(new Stop(), peer.getControl());

            disconnect(network, peer.getNegative(Network.class));
            disconnect(timer, peer.getNegative(Timer.class));

            peers.remove(id);
            Address addr = peersAddress.remove(id);
            Snapshot.removePeer(addr);

            destroy(peer);
        }

        private final static class MessageDestinationFilter extends ChannelFilter<Message, Address> {

            public MessageDestinationFilter(Address address) {
                super(Message.class, address, true);
            }

            @Override
            public Address getValue(Message event) {
                return event.getDestination();
            }
        }
    }
