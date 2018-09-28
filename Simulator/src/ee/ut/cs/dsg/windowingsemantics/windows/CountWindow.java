package ee.ut.cs.dsg.windowingsemantics.windows;

public class CountWindow extends VariableTimeWindow {
	
	protected long tupleCount;
	protected long tupleSlide;
	
	public static CountWindow createTumblingCountWindow(long tCount)
	{
		CountWindow window = new CountWindow();
		window.tupleCount = tCount;
		window.tupleSlide = tCount;
		return window;
	}
	public long getWindowWidth()
	{
		return tupleCount;
	}
	public long getWindowSlide()
	{
		return tupleSlide;
	}
	public static CountWindow createSlidingCountWindow(long tCount, long tSlide)
	{
		CountWindow window = new CountWindow();
		window.tupleCount = tCount;
		window.tupleSlide = tSlide;
		return window;
	}

}
