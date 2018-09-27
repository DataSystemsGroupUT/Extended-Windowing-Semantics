package org.apache.beam.examples.subprocess.utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.Instant;
public class FromJodaTimeToHumanReadbleDate {
    public static void convertFromJodaTimeToHumanReadbleDate(String inFile, String outFile) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try

        {

            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

            BufferedReader reader = new BufferedReader(new FileReader(inFile));

            String line = "", jodaTimeWithDate = "";
            line = reader.readLine();
            Instant jodaInst;

            while (line != null)

            {
                jodaInst=new Instant(line);


                String strDate = sdfDate.format(new Date(jodaInst.getMillis()));
                jodaTimeWithDate = strDate + "," + line;


                writer.write(jodaTimeWithDate + "\n");

                line = reader.readLine();

            }

            writer.flush();

            writer.close();

            reader.close();

        } catch (IOException e) {

            // TODO Auto-generated catch block

            e.printStackTrace();

        }

    }
}
