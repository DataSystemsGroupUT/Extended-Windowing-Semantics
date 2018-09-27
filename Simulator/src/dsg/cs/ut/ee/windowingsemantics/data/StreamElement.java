package dsg.cs.ut.ee.windowingsemantics.data;

import java.util.HashMap;
import java.util.Map;

public class StreamElement extends TimestampedElement {
	// we assume the input to be timestamp, key(partitioning) attribute, value attribute
//	private long timestamp;
	private String key;
	private double value;
	//This is used for time-based triggers and evictors
	private long windowAssignmentTimestamp=0;
	private static Map<String,Long> countReached = new HashMap<String,Long>();
	private long ID;
	public StreamElement(long t, String k, double v, boolean isPartitioned)
	{
		super(t);
//		this.timestamp = t;
		this.key = k;
		this.value = v;
		
		String key;
		if (isPartitioned)
		{
			key = this.key;
			
		}
		else
		{
			key = "Dummy";
		}
		Long value = countReached.get(key);
		if (value == null)
		{
			this.ID = 1;
		}
		else
			this.ID = value.longValue()+1;
		countReached.put(key, Long.valueOf(this.ID));
			
	}
	public StreamElement(StreamElement elem)
	{
		super(elem.getTimestamp());
		//this.timestamp = elem.getTimestamp();
		this.key = elem.getKey();
		this.value = elem.getValue();
		this.ID = elem.getID();
	}
	public long getID() {
		// TODO Auto-generated method stub
		return ID;
	}
	public double getValue() {
		return value;
	}
	
	public String getKey() {
		return key;
	}
	
//	public long getTimestamp() {
//		return timestamp;
//	}
	
	
	public long getWindowAssignmentTimestamp()
	{
		return windowAssignmentTimestamp;
	}
	public void setWindowAssignmentTimestamp(long wats)
	{
		this.windowAssignmentTimestamp = wats;
	}
	
	
	
	@Override
	public String toString()
	{
		return "StreamElement timestamp:"+timestamp+" key:"+key +" value:"+value;
	}
	
}
