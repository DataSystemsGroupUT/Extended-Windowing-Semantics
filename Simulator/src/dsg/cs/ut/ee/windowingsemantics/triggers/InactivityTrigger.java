package dsg.cs.ut.ee.windowingsemantics.triggers;

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
