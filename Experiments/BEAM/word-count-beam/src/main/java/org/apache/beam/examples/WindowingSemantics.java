package org.apache.beam.examples;

import java.io.*;
import java.util.Random;

import org.apache.beam.examples.WindowedWordCount.Options;
import org.apache.beam.examples.common.WriteOneFilePerWindow.PerWindowFiles;
import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import org.joda.time.Duration;
public class WindowingSemantics {
	public static  Long systemPreviousTimstamp=new Long(Instant.now().getMillis());
	public static  Long dataPreviousTimstamp=new Long("1383451271626");
	private static boolean running=true;
	private static Random random = new Random();

	static class WindowedContent
	{
		public long windowStart;
		public long windowEnd;
		PCollection<String> content;
	}

	static class AddTimestampFn extends DoFn<String, Integer> {


		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<Integer> receiver) {
			//We assume that the sttring is a comma separated values of timestamp, key, value
			String[] elements = element.split(",");

			receiver.output(Integer.parseInt(elements[2]));
		}
	}

	static class AddTimestamp extends DoFn<String, KV<String,Integer>> {


		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<KV<String,Integer>> receiver) {
			//We assume that the sttring is a comma separated values of timestamp, key, value
			String[] elements = element.split(",");
			Instant currTimeStamp=new Instant(Instant.now());
			Random random=new Random();
			int rand=random.nextInt(4);
			if(rand==3){
				Long TSInMil=currTimeStamp.getMillis()-300;
				currTimeStamp=new Instant(TSInMil);
			}

/*			//convert data timestamp into the current maintining the same differences
			Long currentTimeStamp=systemPreviousTimstamp+(Long.parseLong(elements[0])-dataPreviousTimstamp);
			Instant oldtimstamp=new Instant(currentTimeStamp);*/


/*			System.out.println(Long.parseLong(elements[0])+", " +
					" "+dataPreviousTimstamp+", "+(Long.parseLong(elements[0])-dataPreviousTimstamp)+"=>"+oldtimstamp);
			systemPreviousTimstamp=currentTimeStamp;
			dataPreviousTimstamp=Long.parseLong(elements[0]);*/


/*
			//convert the data timeStamps to the current maintaining milliseconds and adding some dealy

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(oldtimstamp.getMillis());
            int oldMilisecodsPart = calendar.get(Calendar.MILLISECOND);

			//add current timestamp with some dealay

            double THRESHOLD = 0.001;
            int MIN_DELAY = 1, MAX_DELAY = 5000;
            Instant timestamp = Instant.now();
            calendar.setTimeInMillis(timestamp.getMillis());
            int newMilisecodsPart = calendar.get(Calendar.MILLISECOND);
            Random random = new Random();
            timestamp = new Instant(timestamp.getMillis() - newMilisecodsPart+oldMilisecodsPart);
            if (random.nextDouble() < THRESHOLD) {

                int range = MAX_DELAY - MIN_DELAY;
                int delayInMillis = random.nextInt(range) + MIN_DELAY;
                timestamp = new Instant(timestamp.getMillis() - delayInMillis);

            }
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String strDate = sdfDate.format(new Date(Long.valueOf(timestamp.getMillis())));
            System.out.println(strDate);*/

			System.out.println(currTimeStamp);
			receiver.outputWithTimestamp(KV.of(elements[1],Integer.parseInt(elements[2])),currTimeStamp);

		}
	}
/////////////////////////////////
static class AddTimestampToInfos extends DoFn<Event, Event> {


	@ProcessElement
	public void processElement(@Element Event elements, OutputReceiver<Event> receiver) {
		//We assume that the sttring is a comma separated values of timestamp, key, value

		Instant timeStamp=new Instant(elements.getTimestamp());

		receiver.outputWithTimestamp(elements,timeStamp);

	}

}
///////////////////////////////////////////////
static class PrintOUtElements extends DoFn<KV<String,Long>, KV<String,Long>> {


	@ProcessElement
	public void processElement(@Element KV<String,Long> elements, OutputReceiver<KV<String,Long>> receiver) throws IOException{
		//BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\Drive\\Study\\PhD\\Research\\Streaming Data Processing\\IMPL\\BEAM\\word-count-beam\\sessionWResult.rs"));
		//writer.append(elements.getKey()+" ,"+elements.getValue());
		System.out.println(elements.getKey()+" ,"+elements.getValue());
		System.out.println("\n\n");



	}

}
////////////////////////////////
	static class ConvertToStr extends DoFn<KV<String,Long>, String > {


		@ProcessElement
		public void processElement(@Element KV<String,Long> element, OutputReceiver<String> receiver) {
			//We assume that the sttring is a comma separated values of timestamp, key, value
			String str="";


			receiver.output(str+element.getKey()+","+element.getValue());
            //receiver.output(element.toString());
		}
	}
	static class ConvertIntToStr extends DoFn<KV<String,Long>, String > {


		@ProcessElement
		public void processElement(@Element KV<String,Long> element, OutputReceiver<String> receiver) {
			//We assume that the sttring is a comma separated values of timestamp, key, value
			String str="";


			receiver.output(str+element.getKey()+","+element.getValue());
			//receiver.output(element.toString());
		}
	}
	public static class MapToDummyKey extends SimpleFunction<String, KV<String,String>> {
		@Override
		public KV<String,String> apply(String input) {
			return  KV.of("Dummy",input);

		}
	}



	public static class ConvertLongToString extends SimpleFunction<Long, String> {
		@Override
		public String apply(Long input) {
			return String.valueOf(input.longValue());
		}
	}
	public static class ConvertIntToString extends SimpleFunction<Integer, String> {
		@Override
		public String apply(Integer input) {
			return String.valueOf(input.longValue());
		}
	}
	public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
		@Override
		public Integer apply(Iterable<Integer> input) {
			int sum = 0;
			for (int item : input) {
				sum += item;
			}
			return sum;
		}
	}

	/*static void testTimeWindows(Options options) throws IOException
	{
		Pipeline p = Pipeline.create(options);

		PCollection<String> dataWithTimestamp =  p.apply("read the data from file", TextIO.read().from("data.csv"))
		.apply(ParDo.of(new AddTimestampFn()));

		PCollection<KV<String,String>> keyedData = dataWithTimestamp.apply(MapElements.via(new MapToDummyKey()));

		PCollection<String> windowedData = dataWithTimestamp.apply(Window.into(SlidingWindows.of(Duration.millis(100)).every(Duration.millis(10))));

//		PCollection<KV<String, String>> windowedData = keyedData.apply(Window.into(Sessions.withGapDuration(Duration.millis(10))));
		//PCollection<Long, Long, PCollection<String>> startEndwindowedData = windowedData.apply(MapElements.via())

		PCollection<Long> aggregationsPerWindow = windowedData.apply( Combine.globally(Count.<KV<String,String>>combineFn()).withoutDefaults());
		PCollection<String> mappedToString = aggregationsPerWindow.apply(MapElements.via(new ConvertLongToString()));

		mappedToString.apply(new WriteOneFilePerWindow(options.getOutput(), options.getNumShards()));


//		 ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
//		 TextIO.Write write =
//				 TextIO.write()
//				 //.to(resource.getFilename())
//				 .to(new PerWindowFiles(resource))
//				 .withoutSharding()
//
//				.withTempDirectory(resource.getCurrentDirectory())
//				 .withWindowedWrites(); // commenting this part out gives an unsupported exception
//		 mappedToString.apply(write);


		 PipelineResult result = p.run();
		    try {
		      result.waitUntilFinish();

		    } catch (Exception exc) {
		      result.cancel();
		    }

	}*/

	/**
	 * This function is used to test different types of triggers in Apache BEAM.
	 * @param options
	 * @throws IOException
	 */
	//SerializableFunction<String, Instant> timestampFn = input -> new Instant(Long.valueOf(input));
	public static void TestBeamTriggers(Options options, String filePath) throws IOException
	{

		Pipeline p = Pipeline.create(options);

/*		PCollection<KV<String, Integer>> input=p.apply("read the data from file", TextIO.read().from("SmallSampleData.csv"))
				.apply(ParDo.of(new AddTimestamp()));*/
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //#1 :Count perKey without triggers

/*		PCollection<KV<String, Long>> sumPerKey = input
				.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
				.apply(Count.perKey());


		//to write the results
		PCollection<String> StrScores=sumPerKey.apply(ParDo.of(new ConvertToStr()));
		StrScores.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*

        //#2 default trigger
        PCollection<KV<String, Long>> defaultTriggerResults = input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.ZERO).discardingFiredPanes())
                .apply(Count.perKey());
        //to write the results
        PCollection<String> writeDefalut = defaultTriggerResults.apply(ParDo.of(new ConvertToStr()));
        writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //default trigger with allowed lateness
/*        PCollection<KV<String, Long>> defaultTriggerResults = input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.millis(20)).discardingFiredPanes())
                .apply(Count.perKey());
        //to write the results
        PCollection<String> writeDefalut = defaultTriggerResults.apply(ParDo.of(new ConvertToStr()));
        writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //processing time trigger
/*        		PCollection<KV<String, Long>> sumPerKey = input
				.apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane().
                                plusDelayOf(Duration.millis(50))).discardingFiredPanes().withAllowedLateness(Duration.millis(200)))
				.apply(Count.perKey());
        PCollection<String> StrScores=sumPerKey.apply(ParDo.of(new ConvertToStr()));
        StrScores.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*      //combination of processing and late data (giving duration in processing time means speculative estimate)
        PCollection<KV<String, Long>> defaultTriggerResults = input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(2)))
                        .triggering(AfterEach.inOrder(
                                Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardMinutes(1))).orFinally(AfterWatermark.pastEndOfWindow()),
                                Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardMinutes(1)))))
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.standardMinutes(1)))
                .apply(Count.perKey());
        //to write the results
        PCollection<String> writeDefalut = defaultTriggerResults.apply(ParDo.of(new ConvertToStr()));
        writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*        //Event time triggers
        PCollection<KV<String, Long>> EventTimeTrigger = input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
                        .triggering(AfterWatermark.pastEndOfWindow()).discardingFiredPanes().withAllowedLateness(Duration.millis(300)))
                .apply(Count.perKey());
        //to write the results
        *//*PCollection<String> writeDefalut = EventTimeTrigger.apply(ParDo.of(new ConvertToStr()));
        writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*//*
		//to write the results
		PCollection<String> writeDefalut = EventTimeTrigger.apply(ParDo.of(new ConvertToStr()));
		//writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());

		//write with window dimention
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		TextIO.Write write =
				TextIO.write()
						//.to(resource.getFilename())
						.to(new PerWindowFiles(resource)) //.withoutSharding()
						.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites(); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Event time triggers with Early Firing
/*		PCollection<KV<String, Long>> EventTimeTriggerWithEarlyFinring = input
				.apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
						.triggering(AfterWatermark.pastEndOfWindow()
								.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())).discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply(Count.perKey());
		//to write the results
		PCollection<String> writeDefalut = EventTimeTriggerWithEarlyFinring.apply(ParDo.of(new ConvertToStr()));
		//writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());

		//write with window dimention
				 ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		 TextIO.Write write =
				 TextIO.write()
				 //.to(resource.getFilename())
				 .to(new PerWindowFiles(resource)) //.withoutSharding()
				.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites(); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Event time triggers with Early Firing based on processing time afer 10 ms
/*		PCollection<KV<String, Long>> EventTimeTriggerWithEarlyFinring = input
				.apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
						.triggering(AfterWatermark.pastEndOfWindow()
								.withEarlyFirings(AfterProcessingTime
										.pastFirstElementInPane().plusDelayOf(Duration.millis(10)))).discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply(Count.perKey());
		//to write the results
		PCollection<String> writeDefalut = EventTimeTriggerWithEarlyFinring.apply(ParDo.of(new ConvertToStr()));
		//writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());

		//write with window dimention
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		TextIO.Write write =
				TextIO.write()
						//.to(resource.getFilename())
						.to(new PerWindowFiles(resource)) //.withoutSharding()
						.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites(); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Event time triggers with Late Firing
/*		PCollection<KV<String, Long>> EventTimeTriggerWithLateFinring = input
				.apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
						.triggering(AfterWatermark.pastEndOfWindow()
								.withLateFirings(AfterProcessingTime.pastFirstElementInPane())).discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply(Count.perKey());
		//to write the results
		PCollection<String> writeDefalut = EventTimeTriggerWithLateFinring.apply(ParDo.of(new ConvertToStr()));
		//writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());

		//write with window dimention
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		TextIO.Write write =
				TextIO.write()
						//.to(resource.getFilename())
						.to(new PerWindowFiles(resource)) //.withoutSharding()
						.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites(); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Data driven triggers
/*        PCollection<KV<String, Long>> EventTimeTrigger = input
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(200)))
                        .triggering(AfterPane.elementCountAtLeast(10)).accumulatingFiredPanes().withAllowedLateness(Duration.ZERO))
                .apply(Count.perKey());
        //to write the results
        PCollection<String> writeDefalut = EventTimeTrigger.apply(ParDo.of(new ConvertToStr()));
        writeDefalut.apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*		//Tigger testing using testStream manual elements feeding
		SerializableFunction<Event, Instant> timestampFn = input -> new Instant(input.getTimestamp());

		final Duration ALLOWED_LATENESS = Duration.millis(2);
		final Duration TEAM_WINDOW_DURATION = Duration.millis(5);
		TestStream<Event> infos = TestStream.create(AvroCoder.of(Event.class))
				.addElements(new Event("navy", "Team1", 3, new Instant(0L)),
						new Event("navy", "Team1", 3, new Instant(0L)
								.plus(Duration.millis(3))))

				// Move the watermark past the end the end of the window
				.advanceWatermarkTo(new Instant(0L).plus(TEAM_WINDOW_DURATION)
						.plus(Duration.millis(1))
						)

				.addElements(new Event("sky", "Team2", 12, new Instant(0L)
						.plus(TEAM_WINDOW_DURATION).minus(Duration.millis(1))
						)
				)
				.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> teamScores = p.apply(infos)
				.apply("assignEventTimestamp",WithTimestamps.of(timestampFn).withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
                .apply(ParDo.of(new AddTimestampToInfos()))
				.apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));


		//write with window dimention
		PCollection<String> writeDefalut = teamScores.apply(ParDo.of(new ConvertIntToStr()));
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		TextIO.Write write =
				TextIO.write()
						//.to(resource.getFilename())
						.to(new PerWindowFiles(resource)) //.withoutSharding()
						.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites().withNumShards(1); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Trigger testing using testStream. Read stream from file
		SerializableFunction<Event, Instant> timestampFn = input -> new Instant(input.getTimestamp());
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		String line;
		line = reader.readLine();
		final Duration ALLOWED_LATENESS = Duration.millis(2);
		final Duration TEAM_WINDOW_DURATION = Duration.millis(10);
		Event tempEvent;

		TestStream<Event> infos;
		TestStream.Builder<Event> eventBuilder =TestStream.create(AvroCoder.of(Event.class));

		while (running && line != null){

			if (line.startsWith("*"))
			{
				line = reader.readLine();
				continue;
			}
			long ts; double temperature; String key;
			String[] data = line.split(",");

			if (data.length == 3)
			{
				ts = Long.parseLong(data[0]);
				temperature = Double.parseDouble(data[2]);
				key = data[1];
			}
			else
			{
				ts = Long.parseLong(data[0]);
				temperature = Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;
				key = "DUMMY";
			}

			if (key.equals("W")) // This is a watermark timestamp
			{
				eventBuilder = eventBuilder.advanceWatermarkTo(new Instant(ts));
			}
			else
			{

				tempEvent=new Event("",key,(int)temperature,new Instant(ts));
				eventBuilder=eventBuilder.addElements(tempEvent);
			}
			line = reader.readLine();

		}
		reader.close();

		infos= eventBuilder.advanceWatermarkToInfinity();

		//p.apply(infos).apply(ParDo.of(new PrintOUtElements()));

		PCollection<KV<String, Long>> teamScores = p.apply(infos)
				//Make it possible to insert late items
				.apply("assignEventTimestamp",WithTimestamps.of(timestampFn).withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
				.apply(ParDo.of(new AddTimestampToInfos()))
				.apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));
		//in case of session window write the elements into the consol
		teamScores.apply(ParDo.of(new PrintOUtElements()));


/*		//write with window dimention
		PCollection<String> writeDefalut = teamScores.apply(ParDo.of(new ConvertIntToStr()));
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
		TextIO.Write write =
				TextIO.write()
						//.to(resource.getFilename())
						.to(new PerWindowFiles(resource)) //.withoutSharding()
						.withTempDirectory(resource.getCurrentDirectory()).withWindowedWrites().withNumShards(1); // commenting this part out gives an unsupported exception
		writeDefalut.apply(write);*/


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		PipelineResult result = p.run();
		try {
			result.waitUntilFinish();

		} catch (Exception exc) {
			result.cancel();
		}
	}

	public static void main(String[] args) throws IOException
	{
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		String filePath="D:\\Drive\\Study\\PhD\\Research\\Streaming Data Processing\\IMPL\\BEAM\\word-count-beam\\DataIntWithDelayAndWMSample.csv";
		//testTimeWindows(options);

		TestBeamTriggers(options, filePath);
	}




	/** Calculates scores for each team within the configured window duration. */
	// [START DocInclude_WindowAndTrigger]
	// Extract team/score pairs from the event stream, using hour-long windows by default.
	@VisibleForTesting
	static class CalculateTeamScores
			extends PTransform<PCollection<Event>, PCollection<KV<String, Long>>> {
		private final Duration teamWindowDuration;
		private final Duration allowedLateness;


		CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
			this.teamWindowDuration = teamWindowDuration;
			this.allowedLateness = allowedLateness;
		}

		@Override
		public PCollection<KV<String, Long>> expand(PCollection<Event> infos) {

/*			// FixedWindows


			return infos
					.apply(
							"LeaderboardTeamFixedWindows",
							Window.<Event>into(FixedWindows.of(teamWindowDuration))
									// We will get early (speculative) results as well as cumulative
									// processing of late data.
									.triggering(AfterWatermark.pastEndOfWindow()
                                            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1)))
                                            .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2)))
                                    )
									.withAllowedLateness(allowedLateness)
									.discardingFiredPanes()
                    )
					// Extract and sum teamname/score pairs from the event data.
					.apply("ExtractTeamScore", new ExtractAndSumScore("team"));*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*			//sliding window
			return infos
					.apply(
							"LeaderboardTeamSlidingWindows",
							Window.<Event>into(SlidingWindows.of(Duration.millis(10)).every(Duration.millis(4)))
									// We will get early (speculative) results as well as cumulative
									// processing of late data.
									.triggering(AfterWatermark.pastEndOfWindow()
											.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1)))
											.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2)))
									)
									.withAllowedLateness(allowedLateness)
									.accumulatingFiredPanes()
					)
					// Extract and sum teamname/score pairs from the event data.
					.apply("ExtractTeamScore", new ExtractAndSumScore("team"));*/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


			//session window
			return infos
					.apply(
							"LeaderboardTeamSessionsWindows",
							Window.<Event>into(Sessions.withGapDuration(Duration.millis(10)))
									// We will get early (speculative) results as well as cumulative
									// processing of late data.
									.triggering(AfterWatermark.pastEndOfWindow()
											.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1)))
											.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2)))
									)
									.withAllowedLateness(allowedLateness)
									.discardingFiredPanes()
					)
					// Extract and sum teamname/score pairs from the event data.
					.apply("ExtractTeamScore", new ExtractAndSumScore("team"));
		}
	}

	/**
	 * A transform to extract key/score information from Event, and sum the scores. The
	 * constructor arg determines whether 'team' or 'user' info is extracted.
	 */
	// [START DocInclude_USExtractXform]
	public static class ExtractAndSumScore
			extends PTransform<PCollection<Event>, PCollection<KV<String, Long>>> {

		private final String field;

		ExtractAndSumScore(String field) {
			this.field = field;
		}

		@Override
		public PCollection<KV<String, Long>> expand(PCollection<Event> gameInfo) {

			return gameInfo
					.apply(
							MapElements.into(
									TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
									.via((Event gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
					.apply(Count.perKey());
		}
	}

}
