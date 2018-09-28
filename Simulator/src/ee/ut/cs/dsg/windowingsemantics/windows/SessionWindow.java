package ee.ut.cs.dsg.windowingsemantics.windows;

public class SessionWindow extends VariableTimeWindow {

	protected long inactivityGap;
	
	public SessionWindow (long gap)
	{
		inactivityGap = gap;
	}
	
	public long getInactivityGap()
	{
		return inactivityGap;
	}
	
	public static SessionWindow createSessionWindow(long gap)
	{
		return new SessionWindow(gap);
	}
}
