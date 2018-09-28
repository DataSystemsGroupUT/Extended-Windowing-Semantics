package ee.ut.cs.dsg.windowingsemantics.utils;

public class MapKey {
	private Long windowID;
	private String partitionKey;
	
	public MapKey(Long l, String k)
	{
		windowID = Long.valueOf(l.longValue());
		partitionKey = k;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == null) return false;
		if (!(other instanceof MapKey))
			return false;
		MapKey otherMK = (MapKey)other;
//		System.out.println(this.toString() +"===>"+ otherMK.toString());
		return this.partitionKey.equals(otherMK.getKey()) && this.windowID.longValue() == otherMK.getWindowID();
	}

	public String getKey() {
		
		return partitionKey;
	}
	
	public long getWindowID() {
		
		return windowID.longValue();
	}
	@Override
	public String toString()
	{
		return "WindowID: "+windowID.longValue()+" ParitioningKey: "+partitionKey;
	}
	@Override
	public int hashCode()
	{
		return this.toString().hashCode();
	}
	
}
