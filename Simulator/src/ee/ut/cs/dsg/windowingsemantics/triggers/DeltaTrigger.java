package ee.ut.cs.dsg.windowingsemantics.triggers;

public class DeltaTrigger extends Trigger {
	
	private double deltaAttributeThreshold;
	public DeltaTrigger(double threshold)
	{
		deltaAttributeThreshold = threshold;
	}
	public double getThreshold()
	{
		return deltaAttributeThreshold;
	}

}
