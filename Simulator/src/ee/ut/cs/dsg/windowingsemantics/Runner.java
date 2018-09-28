package ee.ut.cs.dsg.windowingsemantics;

import ee.ut.cs.dsg.windowingsemantics.evictors.CountEvictor;
import ee.ut.cs.dsg.windowingsemantics.evictors.DeltaEvictor;
import ee.ut.cs.dsg.windowingsemantics.evictors.TimeEvictor;
import ee.ut.cs.dsg.windowingsemantics.query.Query;
import ee.ut.cs.dsg.windowingsemantics.triggers.CountTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.TimeTrigger;
import ee.ut.cs.dsg.windowingsemantics.windows.CountWindow;
import ee.ut.cs.dsg.windowingsemantics.windows.FixedTimeWindow;
import ee.ut.cs.dsg.windowingsemantics.windows.SessionWindow;

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
			//.addEvictor(CountEvictor.createCountEvictor(5))
			//.addEvictor(DeltaEvictor.createDeltaEvictor(1))
			.addEvictor(TimeEvictor.createTimeEvictor(5))
			.processQuery();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
