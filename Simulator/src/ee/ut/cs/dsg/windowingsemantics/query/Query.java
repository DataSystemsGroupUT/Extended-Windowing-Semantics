package ee.ut.cs.dsg.windowingsemantics.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import java.util.Map;
import java.util.Set;

import ee.ut.cs.dsg.windowingsemantics.evictors.Evictor;
import ee.ut.cs.dsg.cs.windowingsemantics.source.Source;
import ee.ut.cs.dsg.windowingsemantics.data.StreamElement;
import ee.ut.cs.dsg.windowingsemantics.data.TimestampedElement;
import ee.ut.cs.dsg.windowingsemantics.data.Watermark;
import ee.ut.cs.dsg.windowingsemantics.triggers.CountTrigger;
import ee.ut.cs.dsg.windowingsemantics.triggers.ResultUpdateMode;
import ee.ut.cs.dsg.windowingsemantics.triggers.Trigger;
import ee.ut.cs.dsg.windowingsemantics.utils.MapKey;
import ee.ut.cs.dsg.windowingsemantics.utils.Range;
import ee.ut.cs.dsg.windowingsemantics.windows.CountWindow;
import ee.ut.cs.dsg.windowingsemantics.windows.FixedTimeWindow;
import ee.ut.cs.dsg.windowingsemantics.windows.OperationalState;
import ee.ut.cs.dsg.windowingsemantics.windows.SessionWindow;
import ee.ut.cs.dsg.windowingsemantics.windows.Window;
import ee.ut.cs.dsg.windowingsemantics.windows.WindowInstance;
import ee.ut.cs.dsg.windowingsemantics.windows.WindowPane;

public class Query {

	private Window window;
	private boolean computeOnPartialWindow;
	private ResultUpdateMode resultUpdateMode;
//	private long allowedLatness=0;
	private Map<MapKey,WindowInstance> windowInstances;
	private Source source;
	private boolean isPartitioned=false;
	private long currentWatermark = Long.MIN_VALUE;
	private long sleepMillis=100;
	public Query()
	{
		computeOnPartialWindow = false;
		resultUpdateMode = ResultUpdateMode.Accumulate;
//		allowedLatness = 0;
		//This maps the window index starting from 0 to the actual window instance
		windowInstances = new HashMap<MapKey,WindowInstance>(1000);
	}
	public Query window(Window w)
	{
		this.window = w;
		return this;
	}
	public Query computeOnPartialWindow(boolean b)
	{
		this.computeOnPartialWindow = b;
		return this;
	}
	public Query resultUpdateMode(ResultUpdateMode m)
	{
		this.resultUpdateMode = m;
		return this;
	}
	public Query addEarlyTrigger(Trigger t)
	{
		if (this.window != null)
		{
			this.window.setEarlyTrigger(t);
		}
		return this;
	}
	public Query partitionWindow(boolean b)
	{
		this.isPartitioned = b;
		return this;
	}
	public Query addLateTrigger(Trigger t)
	{
		if (this.window != null)
		{
			this.window.setLateTrigger(t);
		}
		return this;
	}

	public Query addEvictor(Evictor v)
	{
		if (this.window != null)
		{
			this.window.setEvictor(v);
		}
		return this;
	}
	public Query allowedLateness(long lateness)
	{
		//this.allowedLatness = lateness;
		if (this.window != null)
			this.window.setAllowedLatness(lateness);
		return this;
	}
	public Query addSource(String filename)
	{
		this.source = new Source(filename, sleepMillis);
		return this;
	}
	//	public List<WindowInstance> run()
	//	{
	//		return null;
	//	}

	public void processQuery() throws Exception
	{
		if (this.window instanceof FixedTimeWindow)
		{
			processFixedTimeWindow();

		}
		else if (this.window instanceof CountWindow)
		{
			processCountWindow();
		}
		else if (this.window instanceof SessionWindow)
		{
			processSessionWindow();
		}
		for (WindowInstance wi: windowInstances.values())
		{
			wi.setOperationalState(OperationalState.purged);
			for (WindowPane wp: wi.getFiringResults())
				System.out.println(wp.toString());
		}
		// After processing, we need to run early triggers, if any
//		List<WindowInstance> instancesAfterProcessingTriggers = processEarlyTrigger();
//		instancesAfterProcessingTriggers.addAll(processLateTrigger());
		
//		for (WindowInstance wi :instancesAfterProcessingTriggers)
//		{
//			//			System.out.println(i.toString());
//			System.out.println(wi.toString());
//		}
	}
//	private List<WindowInstance> processLateTrigger() {
//
//		if (this.window.getLateTrigger() == null)
//		{
//			return new ArrayList<>(0);
//		}
//		else
//		{
//			List<WindowInstance> result = new ArrayList<WindowInstance>(windowInstances.values().size());
//			for (MapKey k: windowInstances.keySet())
//			{
//				WindowInstance wi;
//				wi = windowInstances.get(k);
//				if (this.window.getLateTrigger() instanceof CountTrigger)
//				{
//					result.addAll(processCountTrigger(wi, false));
//				}
//				
//			}
//			return result;
//		}
//	}
	private void processFixedTimeWindow() throws Exception
	{
		FixedTimeWindow ftw = (FixedTimeWindow) this.window;
		if (source == null)
		{
			throw new Exception("Source is not defined");
		}
		TimestampedElement tsElement = source.getNextItem(isPartitioned);

		if (tsElement == null)
		{
			return;
		}
		// special handling for the first element
		long t0 = 0;
		//element.getTimestamp();


		do
		{
			Range range = getContainingWindowsRange(t0, tsElement.getTimestamp(), ftw.getWindowSlide(), ftw.getWindowWidth());
			if (tsElement instanceof Watermark)
			{
				currentWatermark = Math.max(currentWatermark, tsElement.getTimestamp());
				//TODO: Handle watermark by adding it as an element to all window instance who have ended prior to the watermark
				//We have to assign the watermark for all window instances that are applicable to it under any partitioning value
				Set<MapKey> keys = windowInstances.keySet();
				
				//for (MapKey k : keys)
				//for (long i = range.getLowerBound() ; i <= range.getUpperBound(); i++)
				//{
					for (MapKey key: keys)
					{
//						if (key.getWindowID() ==i)
//						{
							WindowInstance wi;
							wi = windowInstances.get(key);
							if (wi.getEnd()+window.getAllowedLateness() < currentWatermark)
							{
								wi.setWatermark(currentWatermark);//, allowedLatness);
								windowInstances.put(key, wi);
							}
//						}
					}
				//}
			}
			else if (tsElement instanceof StreamElement)
			{
				StreamElement element = (StreamElement) tsElement;
				
				for (long i = range.getLowerBound() ; i <= range.getUpperBound(); i++)
				{
					WindowInstance wi;
					MapKey mk;
					if (isPartitioned)
					{
						mk = new MapKey(Long.valueOf(i), element.getKey());
					}
					else
					{
						mk = new MapKey(Long.valueOf(i), "Dummy");
					}

					if (windowInstances.containsKey(mk))
					{
						wi = windowInstances.get(mk);


					}
					else
					{
						wi = new WindowInstance(t0+i*ftw.getWindowSlide(), t0+ftw.getWindowWidth() +(i*ftw.getWindowSlide()), ftw, isPartitioned? element.getKey(): "Dummy");



					}
					
					wi.addElement(element);
					windowInstances.put(mk, wi);
				}
			}
			tsElement = source.getNextItem(isPartitioned); 

		}
		while(tsElement != null);

	}

	private void processCountWindow() throws Exception
	{
		CountWindow cw = (CountWindow) this.window;
		if (source == null)
		{
			throw new Exception("Source is not defined");
		}
		TimestampedElement tsElement = source.getNextItem(isPartitioned);
		if (tsElement == null)
		{
			return;
		}
		// special handling for the first element
		
		long d0 = 0;
		do
		{
			if (tsElement instanceof StreamElement)

			{
				StreamElement element = (StreamElement) tsElement;
				Range range = getContainingTupleWindowsRange(element.getID(), cw.getWindowSlide(), cw.getWindowWidth());
				for (long i = range.getLowerBound() ; i <= range.getUpperBound(); i++)
				{
					WindowInstance wi;
					MapKey mk;
					if (isPartitioned)
					{
						mk = new MapKey(Long.valueOf(i), element.getKey());
					}
					else
					{
						mk = new MapKey(Long.valueOf(i), "Dummy");
					}

					if (windowInstances.containsKey(mk))
					{
						wi = windowInstances.get(mk);


					}
					else
					{
						wi = new WindowInstance(cw, isPartitioned? element.getKey(): "Dummy");



					}
					//TODO: I need to clone the object here or within the add element as it will be assigned to multiple windows
					wi.addElement(element);
					windowInstances.put(mk, wi);
				}
			}
			tsElement = source.getNextItem(isPartitioned); 
		}
		while(tsElement != null);
		// A clean up step
		Set<MapKey> keys = new HashSet<MapKey>();
		for (MapKey key : windowInstances.keySet())
		{
			WindowInstance wi = windowInstances.get(key);
			if((key.getWindowID() > Math.floor(((double)cw.getWindowWidth())/cw.getWindowSlide())-1 && wi.getContentCount() < cw.getWindowWidth()) || 
					wi.getContentCount() < cw.getWindowSlide() ||
					(key.getWindowID() <= Math.floor(((double)cw.getWindowWidth())/cw.getWindowSlide())-1 && wi.getContentCount() < ((key.getWindowID()+1)*cw.getWindowSlide())))
			{
				// we need to drop this from the result
				keys.add(key);
			}
		}
		for (MapKey key : keys)
		{
			windowInstances.remove(key);
		}

	}

//	private void assignElementToSessionWindow(StreamElement element, SessionWindow sw)
//	{
//		
//		//now we have to check the fusion case
//		
//	}
//	private boolean assignElementToSessionWindow (StreamElement element, WindowInstance wi)
//	{
//		for (WindowInstance)
//	}
	private void processSessionWindow() throws Exception
	{
		SessionWindow sw = (SessionWindow) this.window;
		if (source == null)
		{
			throw new Exception("Source is not defined");
		}
		TimestampedElement tsElement = source.getNextItem(isPartitioned);
		if (tsElement == null)
		{
			return;
		}
		
		
		// special handling for the first element
		HashMap<String, Long> activeSessionWindowPerKey = new HashMap<String,Long>();

		String key;
		long activeSessionWindowID;
		do
		{
			if (tsElement instanceof Watermark)
			{
				currentWatermark = Math.max(currentWatermark, tsElement.getTimestamp());
				Set<MapKey> keys = windowInstances.keySet();
				for (MapKey k: keys)
				{
					WindowInstance wi;
					wi = windowInstances.get(k);
					wi.setWatermark(currentWatermark);//, allowedLatness);
					windowInstances.put(k, wi);

				}
			}
			else
			{
				StreamElement element = (StreamElement) tsElement;
				Set<MapKey> keys = windowInstances.keySet();
				boolean found = false;
				for (MapKey k : keys)
				{
					if (isPartitioned && !k.getKey().equals(element.getKey()))
					{
						continue;
					}
					else if (!isPartitioned && !k.getKey().equals("Dummy")) 
					{
						continue;
					}
					WindowInstance wi = windowInstances.get(k);
					
					found = wi.insertAsSessionWindow(element, sw.getInactivityGap());
					windowInstances.put(k, wi);
					if (found)
						break;
					
				}
				if (found) // we need to merge if there is a need
				{
					Set<Set<MapKey>> mergables = new HashSet<Set<MapKey>>();
					for(MapKey k1: keys)
					{
						for (MapKey k2: keys)
						{
							if (k1.equals(k2))
								continue;
							if (!k1.getKey().equals(k2.getKey()))
								continue;
							WindowInstance wi1, wi2;
							wi1 = windowInstances.get(k1);
							wi2 = windowInstances.get(k2);
							if (wi1.canMerge(wi2))
							{
								Set<MapKey> mergee = new HashSet<MapKey>();
								mergee.add(k1);
								mergee.add(k2);
								mergables.add(mergee);
								break;
							}
						}
					}
					for (Set<MapKey> mergee : mergables) 
					{
						MapKey k1, k2, k3;
						k1 = (MapKey)mergee.toArray()[0];
						k2 = (MapKey)mergee.toArray()[1];
						WindowInstance wi1, wi2, wi3;
						wi1 = windowInstances.get(k1);
						wi2 = windowInstances.get(k2);
						wi3 = wi1.merge(wi2);
						if (wi3 != null)
						{
							activeSessionWindowID = activeSessionWindowPerKey.get(k1.getKey());
							activeSessionWindowID++;
							activeSessionWindowPerKey.put(k1.getKey(), activeSessionWindowID);
							k3 = new MapKey(activeSessionWindowID, k1.getKey());
							windowInstances.put(k1, wi1);
							windowInstances.put(k2, wi2);
							windowInstances.put(k3, wi3);
						}
					}
				}
				else
				{
					WindowInstance wi;
					MapKey mk;
					if (isPartitioned)
					{
						key = element.getKey();
					}
					else
					{
						key = "Dummy";
					}


					if (activeSessionWindowPerKey.keySet().contains(key))
					{
						activeSessionWindowID = activeSessionWindowPerKey.get(key);
						activeSessionWindowID++;
					}
					else
					{
						activeSessionWindowID = 0;
						
					}
					activeSessionWindowPerKey.put(key, activeSessionWindowID);

					if (isPartitioned)
					{
						mk = new MapKey(Long.valueOf(activeSessionWindowID), element.getKey());
					}
					else
					{
						mk = new MapKey(Long.valueOf(activeSessionWindowID), "Dummy");
					}

//					if (windowInstances.containsKey(mk))
//					{
//						wi = windowInstances.get(mk);
//						// check the gap
//						if (element.getTimestamp() - wi.getMaxTimestamp() < sw.getInactivityGap())
//						{
//							wi.addElement(element);
//						}
//						else
//						{
//							activeSessionWindowID++;
//							activeSessionWindowPerKey.put(key, activeSessionWindowID);
//							if (isPartitioned)
//							{
//								mk = new MapKey(Long.valueOf(activeSessionWindowID), element.getKey());
//							}
//							else
//							{
//								mk = new MapKey(Long.valueOf(activeSessionWindowID), "Dummy");
//							}
//							wi = new WindowInstance(sw, isPartitioned? element.getKey(): "Dummy");
//							wi.addElement(element);
//						}
//						windowInstances.put(mk, wi);
//
//					}
//					else // this has to be a new window anyway for that key especially 0 (first time to see this key value)
//					{
					wi = new WindowInstance(sw, isPartitioned? element.getKey(): "Dummy");
					wi.addElement(element);
					windowInstances.put(mk, wi);
//					}

				}
			}
			tsElement = source.getNextItem(isPartitioned); 
		}
		while(tsElement != null);

	}
	private Range getContainingWindowsRange(long startTimestamp, long currentTimestamp, long windowSlide, long windowWidth)
	{
		// This equation is according to our calculations in the paper for sliding time windows (tumbling) as a special case

		long u = Math.max(0, (long) Math.floor(((double)currentTimestamp - startTimestamp)/windowSlide));
		//long l = Math.max(0, u + 1 - (long)Math.floor(((double)windowWidth/windowSlide)));

		long l = Math.max(0, (long)Math.ceil(((double)(((currentTimestamp - startTimestamp) - windowWidth)+1)/windowSlide)));
		// (t - t0)/beta == (current - start)/windowslide
		return new Range(l,u);
	}

	private Range getContainingTupleWindowsRange(long currentTupleID, long windowSlide, long windowWidth)
	{
		// This equation is according to our calculations in the paper for sliding time windows (tumbling) as a special case

		long u = (long) Math.floor(((double)(currentTupleID+windowWidth -1))/windowSlide) -1;



		long l = (long) Math.ceil(((double)currentTupleID)/windowSlide)-1;

		return new Range(l,u);
	}

//	private List<WindowInstance> processEarlyTrigger()
//	{
//		if (this.window.getEarlyTrigger() == null)
//		{
//			return (List<WindowInstance>) windowInstances.values();
//		}
//		else
//		{
//			List<WindowInstance> result = new ArrayList<WindowInstance>(windowInstances.values().size());
//			for (MapKey k: windowInstances.keySet())
//			{
//				WindowInstance wi;
//				wi = windowInstances.get(k);
//				if (this.window.getEarlyTrigger() instanceof CountTrigger)
//				{
//					result.addAll(processCountTrigger(wi, true));
//				}
//				result.add(wi);
//			}
//			return result;
//		}
//	}
//	private List<WindowInstance> processCountTrigger(WindowInstance wi, boolean isEarly) {
//		
//		List<WindowInstance> result = new ArrayList<>();
//		CountTrigger ct = (CountTrigger) wi.getWindowSpecification().getEarlyTrigger();
//		List<StreamElement> content; 
//		if (isEarly)
//		{
//			content = wi.getEarlyContent();
//			if (ct.getCount() > content.size())
//				return result; // nothing to add as this trigger shouldn't fire
//			
//		}
//		else
//		{
//			content = wi.getLateContent();
//			if (ct.getCount() > content.size())
//				return result;
//			
//		}
//		int steps = (int) Math.floor(((double) content.size()/ ct.getCount()));
//		for (int i = 1; i <= steps; i++)
//		{
//			
//			
//			for (int j = 0; j <= (i*steps)-1; j++)
//			{
//				WindowInstance nwi = new WindowInstance(wi);
//				nwi.addElement(content.get(j));
//				result.add(nwi);
//				
//				//TODO: Handle evictor here
//			}
//		}
//		return result;
//	}

}
