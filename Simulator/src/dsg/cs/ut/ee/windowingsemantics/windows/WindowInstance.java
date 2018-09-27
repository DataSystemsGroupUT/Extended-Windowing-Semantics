package dsg.cs.ut.ee.windowingsemantics.windows;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import dsg.cs.ut.ee.windowingsemantics.data.StreamElement;
import dsg.cs.ut.ee.windowingsemantics.triggers.CountTrigger;
import dsg.cs.ut.ee.windowingsemantics.triggers.DeltaTrigger;
import dsg.cs.ut.ee.windowingsemantics.triggers.TimeTrigger;
import dsg.cs.ut.ee.windowingsemantics.triggers.Trigger;
import dsg.cs.ut.ee.windowingsemantics.utils.MapKey;


public class WindowInstance {
	protected Window window;
	protected long start;
	protected long end;
	protected long minTimestamp = Long.MAX_VALUE;
	protected long maxTimestamp = Long.MIN_VALUE;
	protected List<StreamElement> earlyContent;
	protected List<StreamElement> lateContent;
	protected long watermark=Long.MIN_VALUE;
	protected String partitioningValue;
	protected MapKey key;
	protected OperationalState opState;
	protected List<WindowPane> firingResults;
	protected StreamElement lastElement;
	protected StreamElement lastElementForDeltaTrigger;
	protected TimerTask processingTimeTrigger;
	protected Timer timer;
	// I might need to treat the elements as an array where content of cell 0 is the oldest element joining the window instance and then progressing
	private void initProcessingTimeTrigger(String ft)
	{
		//FIXME: change the value from string to enum
		Trigger t;
		if (ft.equals("EARLY"))
		 {
			t= window.getEarlyTrigger();
		 }
		else
		{
			t = window.getLateTrigger();
		}
		
		if (t == null)
			return;
		if (t instanceof TimeTrigger)
		{
			TimeTrigger tt = (TimeTrigger) t;
			processingTimeTrigger = new TimerTask() {
				
				@Override
				public void run() {
					fire(ft);
					
				}
			};
			
			timer = new Timer();
			timer.scheduleAtFixedRate(processingTimeTrigger, 0, tt.getPeriod());
			
		}
	}
	public WindowInstance(long s, Window w)
	{
		start = s;
		earlyContent = new ArrayList<StreamElement>();
		lateContent = new ArrayList<>();
		window = w;
		opState = OperationalState.active;
		firingResults = new ArrayList<>();
		initProcessingTimeTrigger("EARLY");
	}
	public WindowInstance(long s, long e, Window w)
	{
		this(s,w);
		end = e;
		
	}
	public WindowInstance(long s, long e, Window w, String partitionValue)
	{
		this(s,e,w);
		partitioningValue = partitionValue;
		
	}
	public WindowInstance(Window w, String partitionValue) // this should be mainly used with session and punctuation windows
	{
		window = w;
		partitioningValue = partitionValue;
		earlyContent = new ArrayList<StreamElement>();
		lateContent = new ArrayList<>();
		opState = OperationalState.active;
		firingResults = new ArrayList<>();
		initProcessingTimeTrigger("EARLY");
	}
	public WindowInstance(WindowInstance other)
	{
		this.window = other.getWindowSpecification();
		partitioningValue = other.getPartitioningValue();
		earlyContent = new ArrayList<>();
		lateContent = new ArrayList<>();
		opState = OperationalState.active;
		start = other.getStart();
		end = other.getEnd();
		minTimestamp = other.getMinTimestamp();
		maxTimestamp = other.getMaxTimestamp();
		initProcessingTimeTrigger("EARLY");
	}
	public String getPartitioningValue()
	{
		return partitioningValue;
	}
	public void addElement(StreamElement o)
	{
		// We need to check the life-cycle of the window to see what to trigger and what to evict
		//if (window.)
		StreamElement se = new StreamElement(o); 
		se.setWindowAssignmentTimestamp(System.currentTimeMillis());
		lastElement = se;
		if (lastElementForDeltaTrigger == null)
			lastElementForDeltaTrigger = se;
		
		long gap=0;
		if (window instanceof SessionWindow)
			gap =((SessionWindow)window).getInactivityGap();
		
		
		
		if (se.getTimestamp()+ window.getAllowedLateness() > watermark && opState==OperationalState.active) // If items arrive before a watermark is assigned to the window instance, add it normally. Otherwise, it is late
		{
			minTimestamp = Math.min(se.getTimestamp(), minTimestamp);
			maxTimestamp = Math.max(se.getTimestamp()+gap, maxTimestamp);
			earlyContent.add(se);
			handleTupleTrigger("EARLY");
		}
		else if (opState==OperationalState.completed)
		{
			lateContent.add(se);
			handleTupleTrigger("LATE");
		}
		
	}
	private void handleTupleTrigger(String ft) {
		Trigger t;
		List<StreamElement> content=new ArrayList<>();
		if(ft.equals("EARLY"))
		{
			t=this.window.getEarlyTrigger();
			content.addAll(earlyContent);
		}
		else
		{
			t=this.window.getLateTrigger();
			//content.addAll(earlyContent);
			content.addAll(lateContent);
		}
		if ( t != null)
		{
			if (t instanceof CountTrigger)
			{
				CountTrigger ct = (CountTrigger) t;
				if (content.size() % ct.getCount() ==0 ) // It is time to fire the count trigger
				{
					fire(ft);
				}
			}
			else if (t instanceof TimeTrigger)
			{
				// I am not sure yet how to simulate that.
			}
			else if (t instanceof DeltaTrigger)
			{
				DeltaTrigger dt = (DeltaTrigger) t;
				if (lastElement.getValue() -lastElementForDeltaTrigger.getValue() > dt.getThreshold())
				{
					fire(ft);
					lastElementForDeltaTrigger = lastElement;
				}
			}
		}
		
	}
	private synchronized void fire(String ft)
	{
		WindowPane windowPane = new WindowPane(this, ft);
		//FIXME: Change the flag of firing to an enum
		for (StreamElement e: earlyContent)
		{
			windowPane.add(e);

		}
		if (ft.equals("EARLY")) // I know this is not the best way to do it. But, will fix it later
		{	
			
		}
		else
		{
			for (StreamElement e: lateContent)
			{
				windowPane.add(e);

			}
		}
		firingResults.add(windowPane);
	}
	public long getMaxTimestamp()
	{
		return maxTimestamp;
	}
	
	public long getMinTimestamp()
	{
		return minTimestamp;
	}
	public void setEnd(long e)
	{
		this.end = e;
	}
	public long getEnd()
	{
		return end;
	}
	public long getStart()
	{
		return start;
	}
	public List<StreamElement> getLateContent()
	{
		List<StreamElement> elems = new ArrayList<>(lateContent.size());
		elems.addAll(lateContent);
		return elems;
	}
	protected List<StreamElement> getContent()
	{
		List<StreamElement> elems = new ArrayList<>(earlyContent.size());
		elems.addAll(earlyContent);
		return elems;
	}
	public WindowInstance merge(WindowInstance other)
	{
		if (canMerge(other))
		{
			WindowInstance merged = new WindowInstance(this.window, this.partitioningValue);
			List<StreamElement> content = this.getContent();
			content.addAll(other.getContent());
			for (StreamElement e :content)
			{
				merged.addElement(e);
			}
			merged.setWatermark(this.getWatermark());
			content.clear();
			content = this.getLateContent();
			content.addAll(other.getLateContent());
			for (StreamElement e :content)
			{
				merged.addElement(e);
			}
			this.setOperationalState(OperationalState.purged);
			other.setOperationalState(OperationalState.purged);
			
		}
		return null;
	}
	public boolean canMerge(WindowInstance other)
	{
		return this.window instanceof SessionWindow && other.getWindowSpecification() instanceof SessionWindow  
				&& (this.opState == OperationalState.active || other.getOperationalState()== OperationalState.active)
				&& (Math.abs(this.minTimestamp - (other.getMaxTimestamp() -((SessionWindow)window).getInactivityGap())) < ((SessionWindow)window).getInactivityGap() 
						|| Math.abs(other.getMinTimestamp() - (this.getMaxTimestamp()- ((SessionWindow)window).getInactivityGap())) < ((SessionWindow)window).getInactivityGap());
	}
	public void setKey(MapKey k) throws Exception
	{
		if (key == null)
		{
			key = k; 
		}
		else
		{
			throw new Exception("Key cannot be changed after first set!");
		}
	}
	//TODO: I need to see if I have to adapt this for late and early as well.
	public long getContentCount()
	{
		return earlyContent.size();
	}
	public List<StreamElement> getEarlyContent()
	{
		List<StreamElement> elems = new ArrayList<>(earlyContent.size());
		elems.addAll(earlyContent);
		return elems;
	}
	
	public Window getWindowSpecification()
	{
		return window;
	}
	public void setWatermark(long wm)//, long allowedLateness)
	{
		if (window instanceof FixedTimeWindow || window instanceof SessionWindow)
		{
			if (wm > watermark  ) //&& this.end+allowedLateness <= wm)
			{
				watermark = wm;
				long gap = 0;
				long endd;
				if (window instanceof SessionWindow)
				{
					gap = 0; // gap is already included in max time stamp for session window ((SessionWindow)window).getInactivityGap();
					endd = getMaxTimestamp();
				}
				else
					endd = end;
				
				if (endd+ gap + window.allowedLateness <= wm)
				{
					this.setOperationalState(OperationalState.completed);
					// cancel any timers if there are any
					cancelProcessingTimeTrigger();
					fire("DEFAULT");
					// check if there is a late processing time trigger, initilize the timer again
					initProcessingTimeTrigger("LATE");
							
					
				}
			}
		}
		if (window instanceof CountWindow)
			return;
		
//		if (window instanceof SessionWindow)
//		{
//			if (watermark == Long.MIN_VALUE && this.getMaxTimestamp()+allowedLateness <= wm)
//				watermark = wm;
//		}
	}
	private void cancelProcessingTimeTrigger() {
		if (timer != null)
		{
			timer.cancel();
			
		}
		if (processingTimeTrigger != null)
		{
			processingTimeTrigger.cancel();
		}
	}
	public long getWatermark()
	{
		return watermark;
	}
	public OperationalState getOperationalState()
	{
		return opState;
	}
	public void setOperationalState(OperationalState newState)
	{
		if (opState == OperationalState.purged) // you can change the state of a purged window
			return;
		if (opState == OperationalState.completed && newState == OperationalState.active)
			return;
		opState = newState;
		if (opState == OperationalState.purged)
			cancelProcessingTimeTrigger();
		
	}
	public boolean insertAsSessionWindow(StreamElement elem, long gap)
	{
		List<StreamElement> allElements = new ArrayList<>(earlyContent.size()+lateContent.size());
//		all
		allElements.addAll(earlyContent);
		//allElements.addAll(lateContent);
		for (StreamElement inElem: allElements)
		{
			long diff = Math.abs( elem.getTimestamp() - inElem.getTimestamp());
			if (0< diff && diff < gap)
			{
				this.addElement(elem);
				return true;
			}
		}
//		//This is more efficient and still semantically matching the definition in the paper
//		if (elem.getTimestamp() >= minTimestamp && elem.getTimestamp() <= maxTimestamp)
//		{
//			this.addElement(elem);
//			return true;
//		}
		return false;
	}
	
	@Override
	public String toString()
	{
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
		StringBuilder sb = new StringBuilder();
		
		//sb.append("Window type:"+window.getClass().getName()+" partitioning value:"+partitioningValue+" start: ");
		if (window instanceof CountWindow || window instanceof SessionWindow)
		{
			sb.append("("+sdfDate.format(this.minTimestamp) +","+sdfDate.format(this.maxTimestamp));
		}
		else
		{
			sb.append("("+sdfDate.format(this.start) +","+sdfDate.format(this.end));
		}
		sb.append(",("+partitioningValue+"),"+ earlyContent.size()+","+lateContent.size()+")");
		return sb.toString();
	}
	public List<WindowPane> getFiringResults()
	{
		List<WindowPane> result = new ArrayList<>(firingResults.size());
		result.addAll(firingResults);
		return result;
	}
}
