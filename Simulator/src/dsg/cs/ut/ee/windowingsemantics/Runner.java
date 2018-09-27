package dsg.cs.ut.ee.windowingsemantics;

import dsg.cs.ut.ee.windowingsemantics.query.Query;
import dsg.cs.ut.ee.windowingsemantics.triggers.CountTrigger;
import dsg.cs.ut.ee.windowingsemantics.triggers.TimeTrigger;
import dsg.cs.ut.ee.windowingsemantics.windows.CountWindow;
import dsg.cs.ut.ee.windowingsemantics.windows.FixedTimeWindow;
import dsg.cs.ut.ee.windowingsemantics.windows.SessionWindow;

public class Runner {
	
	public static void main(String[] args)
	{
		Query q = new Query();
		
		try {
			q.window(SessionWindow.createSessionWindow(5))
			//q.window(FixedTimeWindow.createSlidingTimeWindow(10,2))
//			q.window(CountWindow.createSlidingCountWindow(5, 2))
			.addSource("data2.csv")
//			.addSource("DummyDataWithDelay.csv")
			.partitionWindow(true)
			.addEarlyTrigger(CountTrigger.createCountTrigger(2))
			//.addEarlyTrigger(TimeTrigger.createPeriodicTrigger(3))
			.addLateTrigger(CountTrigger.createCountTrigger(2))
			.processQuery();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
