package ee.ut.cs.dsg.windowingsemantics.evictors;

public final class CountEvictor extends Evictor {

	private int count;
	public CountEvictor(int c)
	{
		count = c;
	}
	public int getCount()
	{
		return count;
	}
	public static CountEvictor createCountEvictor(int cnt)
	{
		CountEvictor ce = new CountEvictor(cnt);
		return ce;
	}
}
