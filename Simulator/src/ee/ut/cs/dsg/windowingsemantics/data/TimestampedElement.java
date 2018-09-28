package ee.ut.cs.dsg.windowingsemantics.data;

public class TimestampedElement {
	protected long timestamp;
	
	public TimestampedElement(long ts)
	{
		timestamp = ts;
	}
	
	public long getTimestamp()
	{
		return timestamp;
	}
	
	public static TimestampedElement createTimestamptedElement(String rawData, boolean isPartitioned)
	{
		
		String[] data = rawData.split(",");
		try
		{
			if (data[1].equals("W"))
			{
				return new Watermark(Long.valueOf(data[0]));
			}
			else
			{
				StreamElement element = new StreamElement(Long.valueOf(data[0]), data[1], Double.valueOf(data[2]), isPartitioned);
				return element;
			}
		}
		catch(NumberFormatException nfe)
		{
			System.err.print("Raw data format to create a stream element is improper. It should be (timestamp(long), key(string), value(double))");
			return null;
		}
	}
}
