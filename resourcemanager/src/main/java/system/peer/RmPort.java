package system.peer;

import common.simulation.AllocateResourcesManyMachines;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
	positive(RequestResource.class);
	positive(AllocateResourcesManyMachines.class);
}}
