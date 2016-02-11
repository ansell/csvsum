/*
 * Copyright (c) 2016, Peter Ansell
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.ansell.csvsum;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csvutil.CSVUtil;
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
	 * The default number of samples to include for each field in the summarised
	 * CSV.
	 */
	private static final int DEFAULT_SAMPLE_COUNT = 20;

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
				.defaultsTo(DEFAULT_SAMPLE_COUNT).describedAs(
						"The maximum number of sample values for each field to include in the output, or -1 to dump all sample values for each field.");

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

		runSummarise(Files.newBufferedReader(inputPath), writer, samplesToShow.value(options));
	}

	/**
	 * Summarise the CSV file from the input {@link Reader} and emit the summary
	 * CSV file to the output {@link Writer}, including the default maximum
	 * number of sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input CSV file, as a {@link Reader}.
	 * @param output
	 *            The output CSV file as a {@link Writer}.
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(Reader input, Writer output) throws IOException {
		runSummarise(input, output, DEFAULT_SAMPLE_COUNT);
	}

	/**
	 * Summarise the CSV file from the input {@link Reader} and emit the summary
	 * CSV file to the output {@link Writer}, including the given maximum number
	 * of sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input CSV file, as a {@link Reader}.
	 * @param output
	 *            The output CSV file as a {@link Writer}.
	 * @param maxSampleCount
	 *            THe maximum number of sample values in the summary for each
	 *            field. Set to -1 to include all unique values for each field.
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(Reader input, Writer output, int maxSampleCount) throws IOException {
		final JDefaultDict<String, AtomicInteger> emptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		final JDefaultDict<String, AtomicInteger> nonEmptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		final JDefaultDict<String, AtomicBoolean> possibleIntegerFields = new JDefaultDict<>(
				k -> new AtomicBoolean(true));
		final JDefaultDict<String, AtomicBoolean> possibleDoubleFields = new JDefaultDict<>(
				k -> new AtomicBoolean(true));
		final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts = new JDefaultDict<String, JDefaultDict<String, AtomicInteger>>(
				k -> new JDefaultDict<>(l -> new AtomicInteger()));
		final List<String> headers = new ArrayList<String>();
		final AtomicInteger rowCount = new AtomicInteger();

		CSVUtil.streamCSV(input, h -> headers.addAll(h), (h, l) -> {
			rowCount.incrementAndGet();
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
			// We are a streaming summariser, and do not store the raw original
			// lines. Only unique, non-empty, values are stored in the
			// valueCounts map for uniqueness summaries
		});

		// This schema defines the fields and order for the columns in the
		// summary CSV file
		final CsvSchema schema = CsvSchema.builder().addColumn("fieldName")
				.addColumn("emptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("nonEmptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("uniqueValueCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("possiblePrimaryKey", CsvSchema.ColumnType.BOOLEAN)
				.addColumn("possiblyInteger", CsvSchema.ColumnType.BOOLEAN)
				.addColumn("possiblyFloatingPoint", CsvSchema.ColumnType.BOOLEAN).addColumn("sampleValues")
				.setUseHeader(true).build();

		// Shared StringBuilder across fields for efficiency
		// After each field the StringBuilder is truncated
		final StringBuilder sampleValue = new StringBuilder();
		final Consumer<? super String> sampleHandler = s -> {
			if (sampleValue.length() > 0) {
				sampleValue.append(", ");
			}
			sampleValue.append(s);
		};

		try (final SequenceWriter csvWriter = CSVUtil.newCSVWriter(output, schema);) {
			headers.forEach(h -> {
				final int emptyCount = emptyCounts.get(h).get();
				final int nonEmptyCount = nonEmptyCounts.get(h).get();
				final int valueCount = valueCounts.get(h).keySet().size();
				final boolean possiblePrimaryKey = valueCount == nonEmptyCount && valueCount == rowCount.get();

				boolean possiblyInteger = false;
				boolean possiblyDouble = false;
				// Only expose our numeric type guess if non-empty values found
				if (nonEmptyCount > 0) {
					possiblyInteger = possibleIntegerFields.get(h).get();
					possiblyDouble = possibleDoubleFields.get(h).get();
				}

				final Stream<String> stream = valueCounts.get(h).keySet().stream();
				if (maxSampleCount >= 0) {
					stream.limit(maxSampleCount).sorted().forEach(sampleHandler);
					if (valueCount > maxSampleCount) {
						sampleValue.append(", ...");
					}
				} else {
					stream.sorted().forEach(sampleHandler);
				}

				try {
					csvWriter.write(Arrays.asList(h, emptyCount, nonEmptyCount, valueCount, possiblePrimaryKey,
							possiblyInteger, possiblyDouble, sampleValue));
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					// Very important to reset this shared StringBuilder after
					// each row is written
					sampleValue.setLength(0);
				}
			});
		}
	}

}
