package ee.ut.cs.dsg.windowingsemantics.evictors;



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
	public static TimeEvictor createTimeEvictor(long period)
	{
		TimeEvictor te = new TimeEvictor(period);
		return te;
	}
}
