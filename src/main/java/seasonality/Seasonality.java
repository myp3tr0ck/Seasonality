package seasonality;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateUtils;

import cascading.flow.AssemblyPlanner.Context;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
//import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pattern.pmml.PMMLPlanner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
//import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
//import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
//import joptsimple.OptionParser;
//import joptsimple.OptionSet;

public class Seasonality {
	/** @param args */
	public static void main(String[] args) throws RuntimeException {
		//String inputPath = args[0];
		String inputPath = "./src/test/resources/dates.txt";
		//String classifyPath = args[1];
		String classifyPath = "./src/test/resources/results.txt";

		// set up the config properties
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, Seasonality.class);
		LocalFlowConnector flowConnector = new LocalFlowConnector(properties);

		List<String> holidayStrings = null;
		try {
			holidayStrings = FileUtils.readLines(new File(
					"./src/main/resources/holiday.txt"));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		List<Date> holidays = new ArrayList<Date>();
		List<Date> plusOne = new ArrayList<Date>();
		List<Date> minusOne = new ArrayList<Date>();
		SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/YYYY");
		for (String dateString : holidayStrings) {
			Date date = null;
			try {
				date = sdf.parse(dateString);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			holidays.add(date);
			minusOne.add(DateUtils.addDays(date, -1));
			plusOne.add(DateUtils.addDays(date, 1));
		}
		
		Fields date = new Fields("date");
		Fields pmmlFields = new Fields("date", "Total", "Weekday", "Month",
				"Year", "Holiday", "PlusOne", "MinusOne", "MONDAY", "TUESDAY",
				"WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
				"JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY",
				"AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER");

		// create source and sink taps
		Tap inputTap = new FileTap(new TextDelimited(date),  inputPath);
		Pipe pipe = new Pipe("plainCopy");
		Each enrich = new Each(pipe, date,  new EnrichFunction(
				//date,
				pmmlFields,
				holidays, plusOne, minusOne));//, pmmlFields);
		Tap classifyTap = new FileTap(new TextDelimited(true, "\t"),
				classifyPath);

		// handle command line options
		// OptionParser optParser = new OptionParser();
		// optParser.accepts( "pmml" ).withRequiredArg();

		// OptionSet options = optParser.parse( args );

		// connect the taps, pipes, etc., into a flow
		FlowDef flowDef = FlowDef.flowDef().setName("classify")
				.addSource(pipe, inputTap)
				.addTail(enrich)
				.addSink("classify", classifyTap);

		// build a Cascading assembly from the PMML description
		// if( options.hasArgument( "pmml" ) )
		{
			// String pmmlPath = (String) options.valuesOf( "pmml" ).get( 0 );
			String pmmlPath = "./src/main/resources/patient-seasonality.xml";

			PMMLPlanner pmmlPlanner = new PMMLPlanner()
					.setPMMLInput(new File(pmmlPath))
					.retainOnlyActiveIncomingFields()
					.setDefaultPredictedField(
							new Fields("predict", Double.class)); // default
																	// value if
																	// missing
																	// from the
																	// model

			flowDef.addAssemblyPlanner(pmmlPlanner);
			// }

			// write a DOT file and run the flow
			Flow classifyFlow = flowConnector.connect(flowDef);
			classifyFlow.writeDOT("dot/classify.dot");
			classifyFlow.complete();
		}
	}

	static class EnrichFunction extends BaseOperation<Context> implements
			Function<Context> {

		private static final long serialVersionUID = 1L;

		List<Date> holidays;
		List<Date> plusOne;
		List<Date> minusOne;

		public EnrichFunction(Fields fieldDeclaration, List<Date> holidays,
				List<Date> plusOne, List<Date> minusOne) {
			super(1, fieldDeclaration);
			this.holidays = holidays;
			this.plusOne = plusOne;
			this.minusOne = minusOne;
		}

		@Override
		public void operate(FlowProcess flow, FunctionCall<Context> call) {
			TupleEntry argument = call.getArguments();
			String dateString = argument.getString(0);
			Date date = null;
			try {
				date = new SimpleDateFormat("mm/dd/YYYY").parse(dateString);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);

			int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
			int month = calendar.get(Calendar.MONTH);
			int year = calendar.get(Calendar.YEAR);

			Tuple result = new Tuple();

			result.add(date);
			// Total
			result.add(-1);
			result.add(dayOfWeek);
			result.add(month);
			result.add(year);
			// Holiday
			result.add(dateInList(date, holidays));
			// PlusOne
			result.add(dateInList(date, plusOne));
			// MinusOne
			result.add(dateInList(date, minusOne));
			// Day of Week
			result.add(dayOfWeek == Calendar.MONDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.TUESDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.WEDNESDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.THURSDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.FRIDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.SATURDAY ? "TRUE" : "FALSE");
			result.add(dayOfWeek == Calendar.SUNDAY ? "TRUE" : "FALSE");
			// Month of Year
			result.add(month == Calendar.JANUARY ? "TRUE" : "FALSE");
			result.add(month == Calendar.FEBRUARY ? "TRUE" : "FALSE");
			result.add(month == Calendar.MARCH ? "TRUE" : "FALSE");
			result.add(month == Calendar.APRIL ? "TRUE" : "FALSE");
			result.add(month == Calendar.MAY ? "TRUE" : "FALSE");
			result.add(month == Calendar.JUNE ? "TRUE" : "FALSE");
			result.add(month == Calendar.JULY ? "TRUE" : "FALSE");
			result.add(month == Calendar.AUGUST ? "TRUE" : "FALSE");
			result.add(month == Calendar.SEPTEMBER ? "TRUE" : "FALSE");
			result.add(month == Calendar.OCTOBER ? "TRUE" : "FALSE");
			result.add(month == Calendar.NOVEMBER ? "TRUE" : "FALSE");
			result.add(month == Calendar.DECEMBER ? "TRUE" : "FALSE");

			call.getOutputCollector().add(result);

		}

		private double dateInList(Date date, List<Date> list) {
			for (Date candidate : list) {
				if (DateUtils.isSameDay(date, candidate)) {
					return 1d;
				}
			}
			return 0d;					
		}

	}

}
