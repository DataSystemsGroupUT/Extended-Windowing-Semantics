package org.apache.beam.examples.subprocess;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class TimestampsToHumanReadableForm {
    public static void rewriteTimestampsInHumanReadableForm(String inFile, String outFile)

    {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try

        {

            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

            BufferedReader reader = new BufferedReader(new FileReader(inFile));

            String line = "", TimeStampWithDate="";
            String [] lineSplitted;

            line = reader.readLine();

            while (line != null)

            {
                lineSplitted=line.split(",");


                String strDate = sdfDate.format(new Date(Long.valueOf(lineSplitted[0])));
                TimeStampWithDate=strDate+","+line;

                writer.write(TimeStampWithDate + "\n");

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
