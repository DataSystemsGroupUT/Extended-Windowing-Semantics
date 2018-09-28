package ee.ut.cs.dsg.windowingsemantics.triggers;

public class InactivityTrigger extends Trigger {

	private long inactivityGap;
	
	public InactivityTrigger(long gap)
	{
		inactivityGap = gap;
	}
	public long getInactivityGap()
	{
		return inactivityGap;
	}
}
