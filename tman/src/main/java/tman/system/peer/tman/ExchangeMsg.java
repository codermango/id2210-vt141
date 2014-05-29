package tman.system.peer.tman;

import java.util.UUID;

import cyclon.system.peer.cyclon.DescriptorBuffer;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class ExchangeMsg {

    public static class RequestRes extends Message {

        private static final long serialVersionUID = 8493601671018888143L;
        private final UUID requestId;
        private final DescriptorBuffer randomBuffer;


        public RequestRes(UUID requestId, DescriptorBuffer randomBuffer, Address source, Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.randomBuffer = randomBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }

        
        public DescriptorBuffer getRandomBuffer() {
            return randomBuffer;
        }


        public int getSize() {
            return 0;
        }
    }
    
    public static class RequestCpu extends Message {

        private static final long serialVersionUID = 8493601671018888143L;
        private final UUID requestId;
        private final DescriptorBuffer randomBuffer;


        public RequestCpu(UUID requestId, DescriptorBuffer randomBuffer, Address source, 
                Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.randomBuffer = randomBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }

        
        public DescriptorBuffer getRandomBuffer() {
            return randomBuffer;
        }


        public int getSize() {
            return 0;
        }
    }
    
    public static class RequestMem extends Message {

        private static final long serialVersionUID = 8493601671018888143L;
        private final UUID requestId;
        private final DescriptorBuffer randomBuffer;


        public RequestMem(UUID requestId, DescriptorBuffer randomBuffer, Address source, 
                Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.randomBuffer = randomBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }

        
        public DescriptorBuffer getRandomBuffer() {
            return randomBuffer;
        }


        public int getSize() {
            return 0;
        }
    }

    public static class ResponseRes extends Message {

        private static final long serialVersionUID = -5022051054665787770L;
        private final UUID requestId;
        private final DescriptorBuffer selectedBuffer;


        public ResponseRes(UUID requestId, DescriptorBuffer selectedBuffer, Address source, Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.selectedBuffer = selectedBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }


        public DescriptorBuffer getSelectedBuffer() {
            return selectedBuffer;
        }


        public int getSize() {
            return 0;
        }
    }
    
    public static class ResponseCpu extends Message {

        private static final long serialVersionUID = -5022051054665787770L;
        private final UUID requestId;
        private final DescriptorBuffer selectedBuffer;


        public ResponseCpu(UUID requestId, DescriptorBuffer selectedBuffer, Address source, Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.selectedBuffer = selectedBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }


        public DescriptorBuffer getSelectedBuffer() {
            return selectedBuffer;
        }


        public int getSize() {
            return 0;
        }
    }
    
    public static class ResponseMem extends Message {

        private static final long serialVersionUID = -5022051054665787770L;
        private final UUID requestId;
        private final DescriptorBuffer selectedBuffer;


        public ResponseMem(UUID requestId, DescriptorBuffer selectedBuffer, Address source, Address destination) {
            super(source, destination);
            this.requestId = requestId;
            this.selectedBuffer = selectedBuffer;
        }


        public UUID getRequestId() {
            return requestId;
        }


        public DescriptorBuffer getSelectedBuffer() {
            return selectedBuffer;
        }


        public int getSize() {
            return 0;
        }
    }

    public static class RequestTimeout extends Timeout {

        private final Address peer;


        public RequestTimeout(ScheduleTimeout request, Address peer) {
            super(request);
            this.peer = peer;
        }


        public Address getPeer() {
            return peer;
        }
    }
}