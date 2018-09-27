package dsg.cs.ut.ee.windowingsemantics.evictors;

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
}
