package cyclon.system.peer.cyclon;

import common.configuration.CyclonConfiguration;
import common.peer.AvailableResources;
import se.sics.kompics.Init;

public final class CyclonInit extends Init {

    private final CyclonConfiguration configuration;
    private final AvailableResources availableResources;

    public CyclonInit(CyclonConfiguration configuration,
            AvailableResources availableResources) {
        super();
        this.configuration = configuration;
        this.availableResources = availableResources;
    }

    public AvailableResources getAvailableResources() {
        return availableResources;
    }

    public CyclonConfiguration getConfiguration() {
        return configuration;
    }
}
