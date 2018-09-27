package dsg.cs.ut.ee.windowingsemantics.windows;



import dsg.cs.ut.ee.windowingsemantics.triggers.TimeTrigger;

public class FixedTimeWindow extends Window {

	protected long width;
	protected long slide;
	public static FixedTimeWindow createTumblingTimeWindow(long w)
	{
		FixedTimeWindow window = new FixedTimeWindow();
		window.width = w;
		window.slide = w;
		// We should handle the triggers
		
		window.setDefaultTrigger(new TimeTrigger(w));
		return window;
	}
	
	public static FixedTimeWindow createSlidingTimeWindow(long w, long s)
	{
		FixedTimeWindow window = new FixedTimeWindow();
		window.width = w;
		window.slide = s;
		// We should handle the triggers
		window.setDefaultTrigger(new TimeTrigger(w));
		
		return window;
	}
	public long getWindowSlide()
	{
		return this.slide;
	}
	
	public long getWindowWidth()
	{
		return this.width;
	}
}
