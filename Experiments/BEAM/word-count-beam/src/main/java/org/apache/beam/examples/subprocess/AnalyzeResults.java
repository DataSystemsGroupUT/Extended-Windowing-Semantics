package org.apache.beam.examples.subprocess;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class AnalyzeResults {
    public static void SumUpCountedElements(String inFile, String outFile)

    {




        try

        {



            BufferedReader reader = new BufferedReader(new FileReader(inFile));

            String line = "";
            String [] lineSplitted;

            line = reader.readLine();
            int total=0;

            while (line != null)

            {
                lineSplitted=line.split(",");
                total+=Integer.parseInt(lineSplitted[1]);


                line = reader.readLine();

            }
            System.out.println(total);



            reader.close();

        } catch (IOException e) {

            // TODO Auto-generated catch block

            e.printStackTrace();

        }

    }

}
