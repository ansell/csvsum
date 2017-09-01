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
package com.github.ansell.csv.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.TextFileSorter;
import com.github.ansell.csv.stream.CSVStream;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Rewrites newlines that occur naturally in RFC4180 CSV files with replacement
 * characters so large CSV files can be sorted using traditional on-disk sorting
 * algorithms that rely solely on line ending characters.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVSorter {

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class).required()
				.describedAs("The output sorted CSV file.");
		// TODO: Change this to a delimited string to allow ordered multiple
		// indexes
		final OptionSpec<Integer> idFieldIndex = parser.accepts("id-field-index").withRequiredArg()
				.ofType(Integer.class).required()
				.describedAs("The index of the column in the CSV file that is to be used for sorting");
		final OptionSpec<Integer> ignoreHeaderLines = parser.accepts("ignore-header-line-count").withRequiredArg()
				.ofType(Integer.class).defaultsTo(1).describedAs(
						"The number of header lines to ignore, with the first representing the actual headers to use");

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

		final int idFieldIndexInteger = idFieldIndex.value(options);

		final Path inputPath = input.value(options).toPath();
		if (!Files.exists(inputPath)) {
			throw new FileNotFoundException("Could not find input CSV file: " + inputPath.toString());
		}

		final Path outputPath = output.value(options).toPath();
		if (Files.exists(outputPath)) {
			throw new FileAlreadyExistsException("Output file already exists: " + outputPath.toString());
		}

		if (!Files.exists(outputPath.getParent())) {
			throw new FileNotFoundException("Could not find directory for output file: " + outputPath.getParent());
		}

		try (final BufferedReader readerInput = Files.newBufferedReader(inputPath);) {
			runSorter(readerInput, outputPath, getSafeSortingMapper(), ignoreHeaderLines.value(options),
					getCsvSchema(CSVStream.defaultSchema(), ignoreHeaderLines.value(options)),
					getComparator(idFieldIndexInteger));
		}
	}

	private static CsvSchema getCsvSchema(CsvSchema baseSchema, int ignoreHeaderLines) {
		if (ignoreHeaderLines > 0) {
			return new CsvSchema.Builder(baseSchema).setUseHeader(true).build();
		} else {
			return new CsvSchema.Builder(baseSchema).setUseHeader(false).build();
		}
	}

	public static Comparator<StringList> getComparator(int idFieldIndex, int... otherFieldIndexes) {
		Comparator<StringList> result = Comparator.comparing(list -> list.get(idFieldIndex));
		if (otherFieldIndexes != null) {
			for (int nextOtherFieldIndex : otherFieldIndexes) {
				result = result.thenComparing(list -> list.get(nextOtherFieldIndex));
			}
		}
		return result;
	}

	public static CsvMapper getSafeSortingMapper() {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		return mapper;
	}

	public static void runSorter(Reader input, Path output, CsvMapper mapper, int ignoreHeaderLines, CsvSchema schema,
			Comparator<StringList> comparator) throws IOException {

		Path tempDir = Files.createTempDirectory(output.getParent(), "temp-csvsort");
		Path tempFile = Files.createTempFile(tempDir, "temp-input", ".csv");

		try (final Writer tempWriter = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8);) {
			IOUtils.copy(input, tempWriter);
		}

		// The sorter cannot handle headers if we are using List as the output,
		// so always set it to false for the version that CSVStream is receiving
		CsvSchema cleanSchema = new CsvSchema.Builder(schema).setUseHeader(false).build();

		Path headerlessTempFile = tempFile;
		ArrayList<String> headers = new ArrayList<>();
		// We must strip the header out before parsing, as it causes issues deep
		// inside of Jackson otherwise
		if (ignoreHeaderLines > 0) {
			headerlessTempFile = Files.createTempFile(tempDir, "temp-headerless-input", ".csv");
			AtomicInteger lineCount = new AtomicInteger(0);
			try (final Writer tempHeaderlessWriter = Files.newBufferedWriter(headerlessTempFile,
					StandardCharsets.UTF_8);
					final SequenceWriter csvWriter = CSVStream.newCSVWriter(tempHeaderlessWriter, cleanSchema);
					final Reader inputReader = Files.newBufferedReader(tempFile, StandardCharsets.UTF_8);) {

				CSVStream.parse(inputReader, h -> headers.addAll(h), (h, l) -> {
					// First line is always consumed by the header consumer, so
					// we start off here already having one header line ignored
					if (lineCount.incrementAndGet() < ignoreHeaderLines) {
						// Silently drop lines in excess
						return null;
					} else {
						return l;
					}
				}, Unchecked.consumer(l -> csvWriter.write(l)));

			}
		}

		SortConfig sortConfig = new SortConfig().withMaxMemoryUsage(20 * 1000 * 1000)
				.withTempFileProvider(() -> Files.createTempFile(tempDir, "temp-intermediate-", ".csv").toFile());

		// Rewrite the header line to the output
		try (final Writer headerOutputWriter = Files.newBufferedWriter(output, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE_NEW);) {
			if (schema.usesHeader()) {
				System.out.println("Writing headers to output file: " + headers);
				try (final SequenceWriter csvHeaderOutputWriter = CSVStream.newCSVWriter(headerOutputWriter,
						cleanSchema);) {
					csvHeaderOutputWriter.write(headers);
				}
			} else {
				System.out.println("Not writing headers to output file: " + headers);
			}
		}

		System.out.println("Headers (if any) written to sorted output first:");
		Files.readAllLines(output, StandardCharsets.UTF_8).stream().forEachOrdered(System.out::println);
		System.out.println("End of headers");

		try (final InputStream tempInput = Files.newInputStream(headerlessTempFile);
				final OutputStream outputStream = Files.newOutputStream(output, StandardOpenOption.APPEND,
						StandardOpenOption.WRITE);
				final CsvFileSorter<StringList> sorter = new CsvFileSorter<>(StringList.class, sortConfig,
						getSafeSortingMapper(), cleanSchema, comparator);) {
			sorter.sort(tempInput, outputStream);
		} finally {
			Files.walk(tempDir).sorted(Comparator.reverseOrder())
					// .map(Path::toFile)
					// .peek(System.out::println)
					// .forEach(File::delete);
					.forEach(f -> System.out.println("Would have deleted temporaryFile: " + f.toString()));
		}

		System.out.println("Sorted output (if any):");
		Files.readAllLines(output, StandardCharsets.UTF_8).stream().forEachOrdered(System.out::println);
		System.out.println("End of sorted output");

	}
}
