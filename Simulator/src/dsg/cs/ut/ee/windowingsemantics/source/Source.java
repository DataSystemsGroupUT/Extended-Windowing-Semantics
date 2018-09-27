package dsg.cs.ut.ee.windowingsemantics.source;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dsg.cs.ut.ee.windowingsemantics.data.StreamElement;
import dsg.cs.ut.ee.windowingsemantics.data.TimestampedElement;

public class Source {
	
	private String file;
	private String line;
	BufferedReader reader;
	private boolean eof=false;
	private long delay;
	public Source(String f, long delay)
	{
		file = f;
		this.delay = delay;
	}
	private void open()
	{
		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public TimestampedElement getNextItem(boolean isPartitioned)
	{
		if (reader == null)
			open();
		
		if (reader != null && !eof)
		{
			try {
				line = reader.readLine();
				if (delay > 0)
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				if (line != null)
					return TimestampedElement.createTimestamptedElement(line, isPartitioned);
				else
				{
					eof=true;
					return null;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		else
			return null;
	}
	public void close()
	{
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
	}

}
