package ee.ut.cs.dsg.windowingsemantics.utils;

public class Range {
	private long lower;
	private long upper;
	
	public Range(long l, long u)
	{
		lower = l;
		upper = u;
	}
	public long getLowerBound()
	{
		return lower;
	}
	public long getUpperBound()
	{
		return upper;
	}

}
