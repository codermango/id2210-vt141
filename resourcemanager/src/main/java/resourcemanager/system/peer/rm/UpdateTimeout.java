package resourcemanager.system.peer.rm;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class UpdateTimeout extends Timeout {

	public UpdateTimeout(SchedulePeriodicTimeout request) {
		super(request);
	}


	public UpdateTimeout(ScheduleTimeout request) {
		super(request);
	}
}
