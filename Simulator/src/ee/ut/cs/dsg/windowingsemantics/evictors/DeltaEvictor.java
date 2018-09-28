package ee.ut.cs.dsg.windowingsemantics.evictors;

public final class DeltaEvictor extends Evictor {
	private double deltaAttributeThreshold;
	public DeltaEvictor(double threshold)
	{
		deltaAttributeThreshold = threshold;
	}
	public double getThreshold()
	{
		return deltaAttributeThreshold;
	}
	public static DeltaEvictor createDeltaEvictor(double threshold)
	{
		DeltaEvictor de = new DeltaEvictor(threshold);
		return de;
	}
}
