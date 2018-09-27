package ee.ut.cs.dsg.windowingsemantics.utilities;

public class Utilities {
public static boolean isNumeric(String s)
{
    try
    {
        double d = Double.parseDouble(s);
    }
    catch(NumberFormatException nfe)
    {
        return false;
    }
    return true;
}
}
