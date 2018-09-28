package ee.ut.cs.dsg.windowingsemantics.triggers;

public final class TimeTrigger extends Trigger {
	private long period; // in milliseconds
	
	public TimeTrigger(long p)
	{
		period = p;
	}
	
	public long getPeriod()
	{
		return period;
	}
	public static TimeTrigger createPeriodicTrigger(long period)
	{
		TimeTrigger tt = new TimeTrigger(period);
		return tt;
	}
}
