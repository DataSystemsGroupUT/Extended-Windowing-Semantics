package ee.ut.cs.dsg.windowingsemantics.windows;



import ee.ut.cs.dsg.windowingsemantics.evictors.CountEvictor;
import ee.ut.cs.dsg.windowingsemantics.evictors.DeltaEvictor;
import ee.ut.cs.dsg.windowingsemantics.evictors.Evictor;
import ee.ut.cs.dsg.windowingsemantics.evictors.TimeEvictor;
import ee.ut.cs.dsg.windowingsemantics.triggers.CountTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.DeltaTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.InactivityTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.ResultUpdateMode;
import ee.ut.cs.dsg.windowingsemantics.triggers.TimeTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.Trigger;

public abstract class Window {

	
	protected Trigger earlyTrigger;
	protected Trigger defaultTrigger;
	protected Trigger lateTrigger;
	protected WindowLifeCycle lifecycle;
	protected ResultUpdateMode resultUpdateMode;
	protected Evictor evictor;
	protected long allowedLateness=0;
	public void setEarlyTrigger(Trigger t)
	{
		earlyTrigger = t;
	}
	public Trigger getEarlyTrigger()
	{
		return earlyTrigger;
	}
	
	public void setLateTrigger(Trigger t)
	{
		lateTrigger = t;
	}
	public Trigger getLateTrigger()
	{
		return lateTrigger;
	}
	protected void setDefaultTrigger(Trigger t)
	{
		defaultTrigger = t;
		adjustWindowLifeCycle();
	}
	
	public void setEvictor(Evictor e)
	{
		evictor = e;
		adjustWindowLifeCycle();
	}
	public Evictor getEvictor()
	{
		return evictor;
	}
	public WindowLifeCycle getWindowLifeCycle()
	{
		return lifecycle;
	}
	public long getAllowedLateness()
	{
		return allowedLateness;
	}
	public void setAllowedLatness(long lateness)
	{
		if (allowedLateness == 0)
			allowedLateness = lateness;
	}
	private void adjustWindowLifeCycle()
	{
		if (defaultTrigger != null && evictor != null)
		{
			// We apply the logic from the generic windowing paper. What about session window?
			if (defaultTrigger instanceof CountTrigger && evictor instanceof CountEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger;
			else if (defaultTrigger instanceof CountTrigger && evictor instanceof TimeEvictor)
				this.lifecycle = WindowLifeCycle.InsertTriggerEvict; // there is no specific order between trigger and evict
			else if (defaultTrigger instanceof CountTrigger && evictor instanceof DeltaEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger;
			
			else if (defaultTrigger instanceof TimeTrigger && evictor instanceof CountEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger;
			else if (defaultTrigger instanceof TimeTrigger && evictor instanceof TimeEvictor)
				this.lifecycle = WindowLifeCycle.InsertTriggerEvict; // there is no specific order between trigger and evict and insert
			else if (defaultTrigger instanceof TimeTrigger && evictor instanceof DeltaEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger; // there is no specific order between trigger and evict 
			
			// in activity gap trigger, we can see it as a special case of time trigger. However, it occurs once per window
			else if (defaultTrigger instanceof InactivityTrigger && evictor instanceof CountEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger;
			else if (defaultTrigger instanceof InactivityTrigger && evictor instanceof TimeEvictor)
				this.lifecycle = WindowLifeCycle.InsertTriggerEvict; // there is no specific order between trigger and evict and insert
			else if (defaultTrigger instanceof InactivityTrigger && evictor instanceof DeltaEvictor)
				this.lifecycle = WindowLifeCycle.EvictInsertTrigger; // there is no specific order between trigger and evict
			
			else if (defaultTrigger instanceof DeltaTrigger && evictor instanceof CountEvictor)
				this.lifecycle = WindowLifeCycle.TriggerEvictInsert;
			else if (defaultTrigger instanceof DeltaTrigger && evictor instanceof TimeEvictor)
				this.lifecycle = WindowLifeCycle.TriggerEvictInsert; // although there is no specific order between evict and insert
			else if (defaultTrigger instanceof DeltaTrigger && evictor instanceof DeltaEvictor)
				this.lifecycle = WindowLifeCycle.TriggerEvictInsert;
		}
	}
}
