package dsg.cs.ut.ee.windowingsemantics.evictors;



public final class TimeEvictor extends Evictor {

	private long period;
	
	public TimeEvictor(long p)
	{
		period = p;
	}
	
	public long getPeriod()
	{
		return period;
	}
}
