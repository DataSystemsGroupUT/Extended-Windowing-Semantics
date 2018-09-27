package ee.ut.cs.dsg.windowingsemantics.events;

public class SimpleEvent {
    private long timestamp;
    private double temperature;
    private String key;

    public SimpleEvent()
    {
        timestamp = System.currentTimeMillis();
    }
    public SimpleEvent(long ts)
    {
        timestamp = ts;
    }
    public SimpleEvent(long ts, double temp)
    {
        timestamp = ts;
        temperature = temp;
    }
    public SimpleEvent(long ts, double temp, String k)
    {
        timestamp = ts;
        temperature = temp;
        key = k;
    }
    public long getTimestamp()
    {
        return timestamp;
    }
    public void setTimestamp(long ts)
    {
        timestamp = ts;
    }

    public double getTemperature(){return temperature;}

    public void setTemperature(double t)
    {
        temperature = t;
    }
    @Override
    public String toString()
    {
        return "SimpleEvent("+this.timestamp+","+this.temperature+")";
    }
}
