package dsg.cs.ut.ee.windowingsemantics.triggers;

public final class CountTrigger extends Trigger {

	private long count;
	
	public CountTrigger(long c)
	{
		count = c;
	}
	public long getCount()
	{
		return count;
	}
	public static CountTrigger createCountTrigger(long cnt)
	{
		CountTrigger ct = new CountTrigger(cnt);
		return ct;
	}
}
