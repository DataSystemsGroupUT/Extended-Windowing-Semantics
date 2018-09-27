package dsg.cs.ut.ee.windowingsemantics.evictors;

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
}
