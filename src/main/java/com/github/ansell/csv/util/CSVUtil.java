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
package com.github.ansell.csv.util;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

/**
 * Utilities used by CSV processors.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVUtil {

	/**
	 * Private constructor for static only class
	 */
	private CSVUtil() {
	}

	/**
	 * Stream a CSV file from the given Reader through the header validator,
	 * line checker, and if the line checker succeeds, send the
	 * checked/converted line to the consumer.
	 * 
	 * @param inputStreamReader
	 * @param headerValidator
	 *            The validator of the header line.
	 * @param lineChecker
	 *            The validator and converter of lines, based on the header
	 *            line.
	 * @param writer
	 *            The consumer of the checked lines.
	 * @throws IOException
	 *             If an error occurred accessing the input.
	 */
	public static <T> void streamCSV(final Reader inputStreamReader, final Consumer<List<String>> headerValidator,
			final BiFunction<List<String>, List<String>, T> lineChecker, final Consumer<T> writer) throws IOException {
		final CsvMapper mapper = new CsvMapper();
		// important: we need "array wrapping" (see next section) here:
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

		List<String> headers = null;

		final MappingIterator<List<String>> it = mapper.readerFor(List.class).readValues(inputStreamReader);
		List<String> nextLine;
		while (it.hasNext()) {
			nextLine = it.next();
			if (headers == null) {
				headers = nextLine.stream().map(v -> v.trim()).collect(Collectors.toList());
				try {
					headerValidator.accept(headers);
				} catch (final IllegalArgumentException e) {
					throw new RuntimeException("Could not verify headers for csv file", e);
				}
			} else {
				if (nextLine.size() != headers.size()) {
					throw new RuntimeException("Line and header sizes were different: " + headers + " " + nextLine);
				}

				final T apply = lineChecker.apply(headers, nextLine);

				// Line checker returning null indicates that a value was not
				// found.
				if (apply != null) {
					writer.accept(apply);
				}
			}
		}

		if (headers == null) {
			throw new RuntimeException("CSV file did not contain a valid header line");
		}
	}

	/**
	 * Returns a Jackson {@link SequenceWriter} which will write CSV lines to
	 * the given {@link Writer} using the {@link CsvSchema}.
	 * 
	 * @param writer
	 *            The writer which will receive the CSV file.
	 * @param schema
	 *            The {@link CsvSchema} that will be used by the returned
	 *            Jackson {@link SequenceWriter}.
	 * @return A Jackson {@link SequenceWriter} that can have
	 *         {@link SequenceWriter#write(Object)} called on it to emit CSV
	 *         lines to the given {@link Writer}.
	 * @throws IOException
	 *             If there is a problem writing the CSV header line to the
	 *             {@link Writer}.
	 */
	public static SequenceWriter newCSVWriter(final Writer writer, CsvSchema schema) throws IOException {
		final CsvMapper mapper = new CsvMapper();
		SequenceWriter csvWriter = mapper.writerWithDefaultPrettyPrinter().with(schema).forType(List.class)
				.writeValues(writer);
		return csvWriter;
	}

	/**
	 * Build a {@link CsvSchema} object using the given headers.
	 * 
	 * @param header
	 *            The list of strings in the header.
	 * @return A {@link CsvSchema} object including the given header items.
	 */
	public static CsvSchema buildSchema(List<String> header) {
		CsvSchema.Builder result = CsvSchema.builder();

		for (String nextHeader : header) {
			result = result.addColumn(nextHeader);
		}

		return result.setUseHeader(true).build();
	}

}