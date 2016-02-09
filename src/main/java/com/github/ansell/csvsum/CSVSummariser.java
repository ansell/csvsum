/**
 * 
 */
package com.github.ansell.csvsum;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Summarises CSV files to easily debug and identify likely parse issues before
 * pushing them through a more heavy tool or process.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVSummariser {

	/**
	 * Private constructor for static only class
	 */
	private CSVSummariser() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be summarised.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class)
				.describedAs("The output file, or the console if not specified.");
		final OptionSpec<Integer> samplesToShow = parser.accepts("samples").withRequiredArg().ofType(Integer.class)
				.defaultsTo(20)
				.describedAs("The maximum number of sample values for each field to include in the output.");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		} catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		final Path inputPath = input.value(options).toPath();
		if (!Files.exists(inputPath)) {
			throw new FileNotFoundException("Could not find input CSV file: " + inputPath.toString());
		}

		final Writer writer;
		if (options.has(output)) {
			writer = Files.newBufferedWriter(output.value(options).toPath());
		} else {
			writer = new BufferedWriter(new OutputStreamWriter(System.out));
		}

		int maxSampleCount = samplesToShow.value(options);

		JDefaultDict<String, AtomicInteger> emptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		JDefaultDict<String, AtomicInteger> nonEmptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		JDefaultDict<String, AtomicBoolean> possibleIntegerFields = new JDefaultDict<>(k -> new AtomicBoolean(true));
		JDefaultDict<String, AtomicBoolean> possibleDoubleFields = new JDefaultDict<>(k -> new AtomicBoolean(true));

		JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts = new JDefaultDict<String, JDefaultDict<String, AtomicInteger>>(
				k -> new JDefaultDict<>(l -> new AtomicInteger()));

		List<String> headers = new ArrayList<String>();

		CSVUtil.streamCSV(Files.newBufferedReader(inputPath), h -> headers.addAll(h), (h, l) -> {
			for (int i = 0; i < h.size(); i++) {
				if (l.get(i).trim().isEmpty()) {
					emptyCounts.get(h.get(i)).incrementAndGet();
				} else {
					nonEmptyCounts.get(h.get(i)).incrementAndGet();
					valueCounts.get(h.get(i)).get(l.get(i)).incrementAndGet();
					try {
						Integer.parseInt(l.get(i));
					} catch (NumberFormatException nfe) {
						possibleIntegerFields.get(h.get(i)).set(false);
					}
					try {
						Double.parseDouble(l.get(i));
					} catch (NumberFormatException nfe) {
						possibleDoubleFields.get(h.get(i)).set(false);
					}
				}
			}
			return l;
		} , l -> {
		});

		CsvSchema schema = CsvSchema.builder().addColumn("fieldName")
				.addColumn("emptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("nonEmptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("uniqueValueCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("possiblyInteger", CsvSchema.ColumnType.BOOLEAN)
				.addColumn("possiblyFloatingPoint", CsvSchema.ColumnType.BOOLEAN).addColumn("sampleValues")
				.setUseHeader(true).build();

		SequenceWriter csvWriter = CSVUtil.newCSVWriter(writer, schema);

		headers.forEach(h -> {
			int emptyCount = emptyCounts.get(h).get();
			int nonEmptyCount = nonEmptyCounts.get(h).get();
			boolean possiblyInteger = possibleIntegerFields.get(h).get();
			boolean possiblyDouble = possibleDoubleFields.get(h).get();
			// System.out.println(h + " : \tempty=\t" + emptyCount + "
			// \tnon-empty=\t" + nonEmptyCount);

			int valueCount = valueCounts.get(h).keySet().size();
			// System.out.println("");
			// System.out.println(h + " : \tunique values=\t" + valueCount);
			List<String> list = valueCounts.get(h).keySet().stream().limit(maxSampleCount).sorted()
					.collect(Collectors.toList());
			StringBuilder sampleValue = new StringBuilder();
			for (int i = 0; i < list.size(); i++) {
				if (i > 0) {
					sampleValue.append(", ");
				}
				sampleValue.append(list.get(i));
			}
			if (valueCount > maxSampleCount) {
				sampleValue.append(", ...");
			}
			// System.out.println(sampleValue.toString());

			try {
				csvWriter.write(Arrays.asList(h, emptyCount, nonEmptyCount, valueCount, possiblyInteger, possiblyDouble,
						sampleValue));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

}
