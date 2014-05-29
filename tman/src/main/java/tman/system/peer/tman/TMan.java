package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;

import java.util.LinkedList;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.DescriptorBuffer;
import cyclon.system.peer.cyclon.PeerDescriptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import tman.simulator.snapshot.Snapshot;

public final class TMan extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(TMan.class);

    Negative<TManSamplePort> tmanPort = negative(TManSamplePort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    private long period;
    private Address self;
    private PeerDescriptor selfDescriptor;
    private LinkedList<PeerDescriptor> tmanPartnersByResource;
    private LinkedList<PeerDescriptor> tmanPartnersByCPU;
    private LinkedList<PeerDescriptor> tmanPartnersByMemory;
    private LinkedList<PeerDescriptor> cyclonPartners;

    private DescriptorBuffer resourceBuffer;
    private DescriptorBuffer cpuBuffer;
    private DescriptorBuffer memoryBuffer;

    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources;

    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

    public TMan() {
        cyclonPartners = new LinkedList<PeerDescriptor>();
        tmanPartnersByResource = new LinkedList<PeerDescriptor>();
        tmanPartnersByCPU = new LinkedList<PeerDescriptor>();
        tmanPartnersByMemory = new LinkedList<PeerDescriptor>();

        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handletmanPartnersResponseRes, networkPort);
        subscribe(handletmanPartnersResponseCpu, networkPort);
        subscribe(handletmanPartnersResponseMem, networkPort);
        subscribe(handletmanPartnersRequestRes, networkPort);
        subscribe(handletmanPartnersRequestCpu, networkPort);
        subscribe(handletmanPartnersRequestMem, networkPort);
    }

    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {

            self = init.getSelf();
            tmanConfiguration = init.getConfiguration();
            period = tmanConfiguration.getPeriod();
            r = new Random(tmanConfiguration.getSeed());
            availableResources = init.getAvailableResources();
            selfDescriptor = new PeerDescriptor(self, availableResources);

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);

        }
    };

    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            Snapshot.updateTManPartnersByRes(new PeerDescriptor(self, availableResources), tmanPartnersByResource);
            Snapshot.updateTManPartnersByCpu(new PeerDescriptor(self, availableResources), tmanPartnersByCPU);
            Snapshot.updateTManPartnersByMem(new PeerDescriptor(self, availableResources), tmanPartnersByMemory);

            trigger(new TManSample(tmanPartnersByResource, tmanPartnersByCPU, tmanPartnersByMemory), tmanPort);
        }
    };

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            cyclonPartners = event.getSample();

            // merge cyclonPartners into tmanPartnersByResource
            tmanPartnersByResource.clear();
            tmanPartnersByCPU.clear();
            tmanPartnersByMemory.clear();

            if (!cyclonPartners.isEmpty()) {
                tmanPartnersByResource.addAll(cyclonPartners);
                tmanPartnersByCPU.addAll(cyclonPartners);
                tmanPartnersByMemory.addAll(cyclonPartners);

                Collections.sort(tmanPartnersByResource, new ComparatorByResources(new PeerDescriptor(self, availableResources)));
                Collections.sort(tmanPartnersByCPU, new ComparatorByCPU(new PeerDescriptor(self, availableResources)));
                Collections.sort(tmanPartnersByMemory, new ComparatorByMemory(new PeerDescriptor(self, availableResources)));


                PeerDescriptor selectedPeerByRes, selectedPeerByCpu, selectedPeerByMem;
                selectedPeerByRes = selectPeer(tmanPartnersByResource.size() / 2, tmanPartnersByResource);
                selectedPeerByCpu = selectPeer(tmanPartnersByCPU.size() / 2, tmanPartnersByCPU);
                selectedPeerByMem = selectPeer(tmanPartnersByMemory.size() / 2, tmanPartnersByMemory);

                if (!tmanPartnersByResource.contains(selfDescriptor)) {
                    tmanPartnersByResource.add(selfDescriptor);
                }

                if (!tmanPartnersByCPU.contains(selfDescriptor)) {
                    tmanPartnersByCPU.add(selfDescriptor);
                }

                if (!tmanPartnersByMemory.contains(selfDescriptor)) {
                    tmanPartnersByMemory.add(selfDescriptor);
                }

                Collections.sort(tmanPartnersByResource, new ComparatorByResources(new PeerDescriptor(selectedPeerByRes.getAddress(), selectedPeerByRes.getAv())));
                Collections.sort(tmanPartnersByCPU, new ComparatorByCPU(new PeerDescriptor(selectedPeerByCpu.getAddress(), selectedPeerByCpu.getAv())));
                Collections.sort(tmanPartnersByMemory, new ComparatorByMemory(new PeerDescriptor(selectedPeerByMem.getAddress(), selectedPeerByMem.getAv())));

//                System.out.println(self.getId() + " In TMan : tmanPartnersByResource after sort#2 -> " + tmanPartnersByResource);
//                for(PeerDescriptor pd : tmanPartnersByResource) {
//                	System.out.println(pd.getAddress().getId() + " " + pd.getAv().getNumFreeCpus() * pd.getAv().getFreeMemInMbs());
//                }
                resourceBuffer = new DescriptorBuffer(self, tmanPartnersByResource);
                cpuBuffer = new DescriptorBuffer(self, tmanPartnersByCPU);
                memoryBuffer = new DescriptorBuffer(self, tmanPartnersByMemory);

                //check null selectedPeerByRes
                trigger(new ExchangeMsg.RequestRes(UUID.randomUUID(), resourceBuffer, self, selectedPeerByRes.getAddress()), networkPort);
                trigger(new ExchangeMsg.RequestCpu(UUID.randomUUID(), cpuBuffer, self, selectedPeerByCpu.getAddress()), networkPort);
                trigger(new ExchangeMsg.RequestMem(UUID.randomUUID(), memoryBuffer, self, selectedPeerByMem.getAddress()), networkPort);
            } else {
                System.out.println("empty sample");
                return;
            }

        }
    };

    Handler<ExchangeMsg.RequestRes> handletmanPartnersRequestRes = new Handler<ExchangeMsg.RequestRes>() {
        @Override
        public void handle(ExchangeMsg.RequestRes event) {
            LinkedList<PeerDescriptor> receivedView = event.getRandomBuffer().getDescriptors();

            PeerDescriptor sourcePeer = null;
            for (PeerDescriptor peer : event.getRandomBuffer().getDescriptors()) {
                if (peer.getAddress() == event.getSource()) {
                    sourcePeer = peer;
                    break;
                }
            }
            if (!tmanPartnersByResource.contains(selfDescriptor)) {
                tmanPartnersByResource.add(selfDescriptor);
            }

            Collections.sort(tmanPartnersByResource, new ComparatorByResources(new PeerDescriptor(event.getSource(), sourcePeer.getAv())));

            resourceBuffer = new DescriptorBuffer(self, tmanPartnersByResource);
            trigger(new ExchangeMsg.ResponseRes(UUID.randomUUID(), resourceBuffer, self, event.getSource()), networkPort);
            
            tmanPartnersByResource.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByResource);
            tmanPartnersByResource.clear();
            tmanPartnersByResource.addAll(hs);
        }
    };

//-----------------------------------------------------------------------------------------
    Handler<ExchangeMsg.RequestMem> handletmanPartnersRequestMem = new Handler<ExchangeMsg.RequestMem>() {
        @Override
        public void handle(ExchangeMsg.RequestMem event) {
            LinkedList<PeerDescriptor> receivedView = event.getRandomBuffer().getDescriptors();
            PeerDescriptor sourcePeer = null;
            for (PeerDescriptor pd : event.getRandomBuffer().getDescriptors()) {
                if (pd.getAddress() == event.getSource()) {
                    sourcePeer = pd;
                    break;
                }
            }
            if (!tmanPartnersByMemory.contains(selfDescriptor)) {
                tmanPartnersByMemory.add(selfDescriptor);
            }

            Collections.sort(tmanPartnersByMemory, new ComparatorByMemory(new PeerDescriptor(event.getSource(),
                    sourcePeer.getAv())));

            memoryBuffer = new DescriptorBuffer(self, tmanPartnersByMemory);
            trigger(new ExchangeMsg.ResponseMem(UUID.randomUUID(), memoryBuffer, self, event.getSource()), networkPort);
            tmanPartnersByMemory.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByMemory);
            tmanPartnersByMemory.clear();
            tmanPartnersByMemory.addAll(hs);
        }
    };

    
//-----------------------------------------------------------------------------------------
    Handler<ExchangeMsg.RequestCpu> handletmanPartnersRequestCpu = new Handler<ExchangeMsg.RequestCpu>() {
        @Override
        public void handle(ExchangeMsg.RequestCpu event) {
            LinkedList<PeerDescriptor> receivedView = event.getRandomBuffer().getDescriptors();
            PeerDescriptor sourcePeer = null;
            for (PeerDescriptor pd : event.getRandomBuffer().getDescriptors()) {
                if (pd.getAddress() == event.getSource()) {
                    sourcePeer = pd;
                    break;
                }
            }
            if (!tmanPartnersByCPU.contains(selfDescriptor)) {
                tmanPartnersByCPU.add(selfDescriptor);
            }

            Collections.sort(tmanPartnersByCPU, new ComparatorByCPU(new PeerDescriptor(event.getSource(),
                    sourcePeer.getAv())));

            cpuBuffer = new DescriptorBuffer(self, tmanPartnersByCPU);
            trigger(new ExchangeMsg.ResponseCpu(UUID.randomUUID(), cpuBuffer, self, event.getSource()), networkPort);
            tmanPartnersByCPU.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByCPU);
            tmanPartnersByCPU.clear();
            tmanPartnersByCPU.addAll(hs);
        }
    };

    
//-----------------------------------------------------------------------------------------
    Handler<ExchangeMsg.ResponseRes> handletmanPartnersResponseRes = new Handler<ExchangeMsg.ResponseRes>() {
        @Override
        public void handle(ExchangeMsg.ResponseRes event) {
            LinkedList<PeerDescriptor> receivedView = event.getSelectedBuffer().getDescriptors();
//        	System.out.println(event.getSource().getId() + " In TMan : tmanPartnersByResource after receive response -> " + receivedView);
//            for(PeerDescriptor pd : receivedView) {
//            	System.out.println(pd.getAddress().getId() + " " + pd.getAv().getNumFreeCpus() * pd.getAv().getFreeMemInMbs());
//            }
            tmanPartnersByResource.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByResource);
            tmanPartnersByResource.clear();
            tmanPartnersByResource.addAll(hs);
        }
    };

    
//-----------------------------------------------------------------------------------------
    Handler<ExchangeMsg.ResponseCpu> handletmanPartnersResponseCpu = new Handler<ExchangeMsg.ResponseCpu>() {
        @Override
        public void handle(ExchangeMsg.ResponseCpu event) {
            LinkedList<PeerDescriptor> receivedView = event.getSelectedBuffer().getDescriptors();
            tmanPartnersByCPU.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByCPU);
            tmanPartnersByCPU.clear();
            tmanPartnersByCPU.addAll(hs);
        }
    };

    
//-----------------------------------------------------------------------------------------
    Handler<ExchangeMsg.ResponseMem> handletmanPartnersResponseMem = new Handler<ExchangeMsg.ResponseMem>() {
        @Override
        public void handle(ExchangeMsg.ResponseMem event) {
            LinkedList<PeerDescriptor> receivedView = event.getSelectedBuffer().getDescriptors();
            tmanPartnersByMemory.addAll(receivedView);
            HashSet<PeerDescriptor> hs = new HashSet<PeerDescriptor>();
            hs.addAll(tmanPartnersByMemory);
            tmanPartnersByMemory.clear();
            tmanPartnersByMemory.addAll(hs);
        }
    };

    public void PrintMsg(String msg) {
        logger.info("Peer " + self.getId() + " :" + msg);
    }

    public PeerDescriptor selectPeer(int psi, LinkedList<PeerDescriptor> view) {
        if (view.size() == 0) {
            return null;
        } else if (view.size() == 1) {
            return view.get(0);
        } else if (psi == 0) {
            return view.get(0);
        } else if (psi > view.size()) {
            return view.get(r.nextInt(view.size()));
        } else {
            return view.get(r.nextInt(psi));
        }
    }

    // TODO - if you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // A temperature of '1.0' will be greedy and always return the best node.
    // A temperature of '0.000001' will return a random node.
    // A temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    public Address getSoftMaxAddress(List<Address> entries) {
        Collections.sort(entries, new ComparatorById(self));

        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
            double val = j;
            j--;
            values[i] = Math.exp(val / tmanConfiguration.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }
        return entries.get(entries.size() - 1);
    }

}
