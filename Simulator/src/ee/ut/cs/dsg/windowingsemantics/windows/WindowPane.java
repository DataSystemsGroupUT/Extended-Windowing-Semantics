package ee.ut.cs.dsg.windowingsemantics.windows;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import ee.ut.cs.dsg.windowingsemantics.data.StreamElement;

public class WindowPane {

	private WindowInstance windowInstance;
	private List<StreamElement> content;
	private String firingTime;
	private OperationalState opState;
	public WindowPane(WindowInstance wi, String ft)
	{
		this.windowInstance = new WindowInstance(wi);
		content = new ArrayList<>();
		firingTime = ft;
	}
	public void add(StreamElement elem)
	{
		content.add(elem);
	}
	public void setOperationalState(OperationalState state)
	{
		opState = state;
	}
	public String toString()
	{
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
		StringBuilder sb = new StringBuilder();
		
		//sb.append("Window type:"+window.getClass().getName()+" partitioning value:"+partitioningValue+" start: ");
		if (windowInstance.getWindowSpecification() instanceof CountWindow || windowInstance.getWindowSpecification() instanceof SessionWindow)
		{
			sb.append("("+sdfDate.format(this.windowInstance.getMinTimestamp()) +","+sdfDate.format(this.windowInstance.getMaxTimestamp()));
		}
		else
		{
			sb.append("("+sdfDate.format(this.windowInstance.getStart()) +","+sdfDate.format(this.windowInstance.getEnd()));
		}
		sb.append(", "+firingTime+",("+windowInstance.getPartitioningValue()+"),"+ content.size()+")");
		return sb.toString();
	}
}
