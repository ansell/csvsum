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

		JDefaultDict<String, AtomicInteger> emptyCounts = new JDefaultDict<String, AtomicInteger>(
				k -> new AtomicInteger());
		JDefaultDict<String, AtomicInteger> nonEmptyCounts = new JDefaultDict<String, AtomicInteger>(
				k -> new AtomicInteger());

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
				}
			}
			return l;
		} , l -> {
		});

		CsvSchema schema = CsvSchema.builder().addColumn("fieldName")
				.addColumn("emptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("nonEmptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("uniqueValueCount", CsvSchema.ColumnType.NUMBER).addColumn("sampleValues").setUseHeader(true)
				.build();

		SequenceWriter csvWriter = CSVUtil.newCSVWriter(writer, schema);

		headers.forEach(h -> {
			int emptyCount = emptyCounts.get(h).get();
			int nonEmptyCount = nonEmptyCounts.get(h).get();
			// System.out.println(h + " : \tempty=\t" + emptyCount + "
			// \tnon-empty=\t" + nonEmptyCount);

			int valueCount = valueCounts.get(h).keySet().size();
			// System.out.println("");
			// System.out.println(h + " : \tunique values=\t" + valueCount);
			List<String> list = valueCounts.get(h).keySet().stream().limit(20).collect(Collectors.toList());
			StringBuilder sampleValue = new StringBuilder();
			list.forEach(s -> {
				sampleValue.append(s + ", ");
			});
			if (valueCount > 20) {
				sampleValue.append("...");
			}
			// System.out.println(sampleValue.toString());

			try {
				csvWriter.write(Arrays.asList(h, emptyCount, nonEmptyCount, valueCount, sampleValue));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

}
