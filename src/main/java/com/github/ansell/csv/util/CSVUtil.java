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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple2;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;
import com.github.ansell.jdefaultdict.JDefaultDict;

/**
 * Utilities used by CSV processors.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVUtil {

	public static final String DOT_REGEX = "\\.";
	public static final Pattern DOT_PATTERN = Pattern.compile(DOT_REGEX);
	public static final String COMMA_REGEX = "\\,";
	public static final Pattern COMMA_PATTERN = Pattern.compile(COMMA_REGEX);

	private static final Collector<Tuple2<String, String>, ?, Map<String, Object>> TUPLE2_TO_MAP = Collectors
			.toMap(e -> e.v1(), e -> (String) e.v2());

	/**
	 * Private constructor for static only class
	 */
	private CSVUtil() {
	}

	public static String oldDateToISO8601LocalDateTime(Date nextColumnDate) {
		LocalDateTime localDateTime = nextColumnDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		String formattedDate = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(localDateTime);
		return formattedDate;
	}

	private static Map<String, Object> leftOuterJoin(ValueMapping mapping, List<String> sourceHeaders,
			List<String> sourceLine, List<String> destHeaders, List<String> destLine, boolean splitFieldNamesByDot) {
		Map<String, Object> matchMap = buildMatchMap(mapping, sourceHeaders, sourceLine, splitFieldNamesByDot);

		boolean allMatch = true;
		for (String nextDestHeader : destHeaders) {
			if (matchMap.containsKey(nextDestHeader)
					&& !matchMap.get(nextDestHeader).equals(destLine.get(destHeaders.indexOf(nextDestHeader)))) {
				allMatch = false;
				break;
			}
		}

		if (allMatch) {
			List<String> filteredDestHeaders = new ArrayList<>(destHeaders.size());
			List<String> filteredDestLine = new ArrayList<>(destLine.size());
			for (int i = 0; i < destHeaders.size(); i++) {
				// Deduplicate matching fields in the two files by ignoring the
				// one from the destination
				if (!sourceHeaders.contains(destHeaders.get(i))) {
					filteredDestHeaders.add(destHeaders.get(i));
					filteredDestLine.add(destLine.get(i));
				}
			}
			return zip(sourceHeaders, sourceLine).concat(zip(filteredDestHeaders, filteredDestLine))
					.collect(TUPLE2_TO_MAP);
		} else {
			return zip(sourceHeaders, sourceLine).collect(TUPLE2_TO_MAP);
		}
	}

	public static Map<String, Object> buildMatchMap(ValueMapping mapping, Map<String, Object> originRow,
			boolean splitFieldNamesByDot) {
		String[] destFields = mapping.getDestFields();
		String[] sourceFields = mapping.getSourceFields();
		Map<String, Object> result = new HashMap<>(destFields.length, 0.75f);

		return buildMatchMap(mapping, originRow, splitFieldNamesByDot, result, sourceFields, destFields);
	}

	private static Map<String, Object> buildMatchMap(ValueMapping m, List<String> inputHeader, List<String> inputLine,
			boolean splitFieldNamesByDot, Map<String, Object> result, String[] sourceFields, String[] destFields) {
		Map<String, Object> originRow = map(inputHeader, inputLine);

		return buildMatchMap(m, originRow, splitFieldNamesByDot, result, sourceFields, destFields);
	}

	private static Map<String, Object> buildMatchMap(ValueMapping mapping, Map<String, Object> originRow,
			boolean splitFieldNamesByDot, Map<String, Object> result, String[] sourceFields, String[] destFields) {
		result.clear();
		// System.out.println("Building match map for: " + mapping + " row=" +
		// originRow);
		for (int i = 0; i < destFields.length; i++) {
			String destField = destFields[i];
			String sourceField = sourceFields[i];
			if (splitFieldNamesByDot) {
				String[] destFieldSplit = DOT_PATTERN.split(destField);
				String[] sourceFieldSplit = DOT_PATTERN.split(sourceField);
				destField = destFieldSplit[1];
				sourceField = sourceFieldSplit[1];
			}
			if (!originRow.containsKey(sourceField)) {
				throw new RuntimeException("Origin row did not contain a field required for mapping: field="
						+ sourceFields[i] + " mapping=" + mapping + " originRow=" + originRow);
			}
			Object nextFKValue = originRow.get(sourceField);
			if (nextFKValue == null) {
				// Return an empty result if one of the source fields was null
				return Collections.emptyMap();
			}
			if (result.containsKey(destField)) {
				throw new RuntimeException("Destination row contained a duplicate field name: field=" + destFields[i]
						+ " mapping=" + mapping);
			}
			result.put(destField, nextFKValue);
		}

		return result;
	}

	private static Map<String, Object> buildMatchMap(ValueMapping m, List<String> inputHeader, List<String> inputLine,
			boolean splitFieldNamesByDot) {
		Map<String, Object> originRow = map(inputHeader, inputLine);

		return buildMatchMap(m, originRow, splitFieldNamesByDot);
	}

	private static Map<String, Object> map(List<String> inputHeader, List<String> inputLine) {
		// Map<String, Object> result = new HashMap<>();
		// for(int i = 0; i < inputHeader.size(); i++) {
		// result.put(inputHeader.get(i), inputLine.get(i));
		// }
		// return result;
		return zip(inputHeader, inputLine).collect(TUPLE2_TO_MAP);
	}

	private static Seq<Tuple2<String, String>> zip(List<String> inputHeader, List<String> inputLine) {
		return Seq.seq(inputHeader).zip(inputLine);
	}

	/**
	 * Joins the two input CSV files according to the {@link ValueMapping}s,
	 * optionally applying the given prefixes to fields in the input and other
	 * inputs respectively.
	 * 
	 * Can also perform a full outer join by setting leftOuterJoin to false.
	 * 
	 * @param input
	 *            The reference input (left)
	 * @param otherInput
	 *            The input to join against (right)
	 * @param map
	 *            The mappings to apply and use to define the join fields
	 * @param output
	 *            The Writer which will receive the output CSV file containing
	 *            the results of the join
	 * @param inputPrefix
	 *            An optional prefix to apply to all of the fields in the input
	 *            file, set to the empty string to disable it.
	 * @param otherPrefix
	 *            An optional prefix to apply to all of the fields in the other
	 *            file, set to the empty string to disable it.
	 * @param leftOuterJoin
	 *            True to use a left outer join and false to use a full outer
	 *            join.
	 * @return The output headers for the joined file.
	 * @throws ScriptException
	 *             If there are issues mapping fields.
	 * @throws IOException
	 *             If there are issues reading or writing files.
	 */
	public static List<String> runJoiner(Reader input, Reader otherInput, List<ValueMapping> map, Writer output,
			String inputPrefix, String otherPrefix, boolean leftOuterJoin) throws ScriptException, IOException {
		// TODO: Use the following measurements to determine what processing
		// method to use
		int inputFileBytes = -1;
		int otherFileBytes = -1;

		final Path tempInputFile = Files.createTempFile("tempInputFile-", ".csv");
		try (final BufferedWriter tempOutput = Files.newBufferedWriter(tempInputFile, StandardCharsets.UTF_8);) {
			inputFileBytes = IOUtils.copy(input, tempOutput);
		}

		final Path tempOtherFile = Files.createTempFile("tempOtherFile-", ".csv");
		try (final BufferedWriter tempOtherOutput = Files.newBufferedWriter(tempOtherFile, StandardCharsets.UTF_8);) {
			otherFileBytes = IOUtils.copy(otherInput, tempOtherOutput);
		}

		if(inputFileBytes < otherFileBytes) {
			// TODO: Swap source and destination so that in-memory set is the smaller of the two
		}
		
		try {
			final List<String> otherH = new ArrayList<>();
			final List<List<String>> otherLines = new ArrayList<>();

			System.out.println("Starting adding other lines to in-memory list...");
			try (final BufferedReader otherTemp = Files.newBufferedReader(tempOtherFile, StandardCharsets.UTF_8)) {
				CSVStream.parse(otherTemp, otherHeader -> otherHeader.forEach(h -> otherH.add(otherPrefix + h)),
						(otherHeader, otherL) -> {
							return otherL;
						}, otherL -> {
							otherLines.add(new ArrayList<>(otherL));
						});
			}
			System.out.println("Completed adding other lines to in-memory list.");
			// Create a set for efficient lookup
			final Set<String> otherHSet = new HashSet<>(otherH);

			final Function<ValueMapping, String> outputFields = e -> e.getOutputField();

			final List<String> outputHeaders = map.stream().filter(k -> k.getShown()).map(outputFields)
					.collect(Collectors.toList());

			final List<ValueMapping> mergeFieldsOrdered = map.stream()
					.filter(k -> k.getLanguage() == ValueMappingLanguage.CSVJOIN).collect(Collectors.toList());
			if (mergeFieldsOrdered.size() != 1) {
				throw new RuntimeException(
						"Can only support exactly one CsvJoin mapping: found " + mergeFieldsOrdered.size());
			}

			final List<ValueMapping> nonMergeFieldsOrdered = map.stream()
					.filter(k -> k.getLanguage() != ValueMappingLanguage.CSVJOIN).collect(Collectors.toList());

			final ValueMapping m = mergeFieldsOrdered.get(0);
			final String[] destFields = m.getDestFields();
			final String[] sourceFields = m.getSourceFields();

			final CsvSchema schema = CSVStream.buildSchema(outputHeaders);
			final Writer writer = output;

			try (final SequenceWriter csvWriter = CSVStream.newCSVWriter(writer, schema);) {
				final JDefaultDict<String, Set<String>> primaryKeys = new JDefaultDict<>(k -> new HashSet<>());
				final Set<List<String>> matchedOtherLines = new LinkedHashSet<>();

				final List<String> previousLine = new ArrayList<>();
				final List<String> previousMappedLine = new ArrayList<>();
				final AtomicInteger lineNumber = new AtomicInteger(0);
				final AtomicInteger filteredLineNumber = new AtomicInteger(0);
				final long startTime = System.currentTimeMillis();
				final BiConsumer<List<String>, List<String>> mapLineConsumer = Unchecked.biConsumer((line, mapped) -> {
					previousLine.clear();
					previousLine.addAll(line);
					previousMappedLine.clear();
					previousMappedLine.addAll(mapped);
					csvWriter.write(mapped);
				});
				// If the streamCSV below is parallelised, each thread must be
				// given
				// a separate temporaryMatchMap
				// Map<String, Object> temporaryMatchMap = new
				// HashMap<>(destFields.length, 0.75f);
				// Map<String, Object> temporaryMatchMap = new
				// LinkedHashMap<>(destFields.length, 0.75f);
				//final Map<String, Object> temporaryMatchMap = new ConcurrentHashMap<>(destFields.length, 0.75f, 4);
				final Map<String, Object> temporaryMatchMap = new HashMap<>(destFields.length, 0.75f);
				
				final List<String> inputHeaders = new ArrayList<>();
				try (final BufferedReader inputTemp = Files.newBufferedReader(tempInputFile, StandardCharsets.UTF_8)) {
					CSVStream.parse(inputTemp, h -> h.forEach(nextH -> inputHeaders.add(inputPrefix + nextH)), (h, l) -> {
						final int nextLineNumber = lineNumber.incrementAndGet();
						if(nextLineNumber % 1000 == 0) {
							double secondsSinceStart = (System.currentTimeMillis() - startTime)/1000.0d;
							System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", nextLineNumber, secondsSinceStart, 
									nextLineNumber/secondsSinceStart);
						}
						final int nextFilteredLineNumber = filteredLineNumber.incrementAndGet();
						try {
							final List<String> mergedInputHeaders = new ArrayList<>(inputHeaders);
							final List<String> nextMergedLine = new ArrayList<>(l);

							final Map<String, Object> matchMap = buildMatchMap(m, mergedInputHeaders, nextMergedLine,
									false, temporaryMatchMap, sourceFields, destFields);
							final Predicate<List<String>> otherLinePredicate = otherL -> {
								return !matchMap.entrySet().stream().filter(nextOtherFieldMatcher -> {
									final String key = nextOtherFieldMatcher.getKey();
									return !otherHSet.contains(key) || !otherL.get(otherH.indexOf(key))
											.equals(nextOtherFieldMatcher.getValue());
								}).findAny().isPresent();
							};
							final Consumer<List<String>> otherLineConsumer = otherL -> {
								matchedOtherLines.add(otherL);
								final Map<String, Object> leftOuterJoinMap = leftOuterJoin(m, mergedInputHeaders,
										nextMergedLine, otherH, otherL, false);
								nonMergeFieldsOrdered.stream().map(nextMapping -> nextMapping.getInputField())
										.forEachOrdered(inputField -> {
											if (leftOuterJoinMap.containsKey(inputField)
													&& !mergedInputHeaders.contains(inputField)) {
												mergedInputHeaders.add(inputField);
												nextMergedLine.add((String) leftOuterJoinMap.get(inputField));
											}
										});
							};
							otherLines.parallelStream().filter(otherLinePredicate).findAny()
									.ifPresent(otherLineConsumer);

							final List<String> mapLine = ValueMapping.mapLine(mergedInputHeaders, nextMergedLine,
									previousLine, previousMappedLine, map, primaryKeys, nextLineNumber,
									nextFilteredLineNumber, mapLineConsumer);
							mapLineConsumer.accept(nextMergedLine, mapLine);

						} catch (final LineFilteredException e) {
							// Swallow line filtered exception and return
							// null
							// below to eliminate it
							// We expect streamCSV to operate in sequential
							// order, print a warning if it doesn't
							final boolean success = filteredLineNumber.compareAndSet(nextFilteredLineNumber,
									nextFilteredLineNumber - 1);
							if (!success) {
								System.out.println("Line numbers may not be consistent");
							}
						}
						return null;
					}, l -> {
					});
				}
				if (!leftOuterJoin) {
					final Consumer<List<String>> fullOuterJoinConsumer = Unchecked.consumer(l -> {
						final int nextLineNumber = lineNumber.incrementAndGet();
						final int nextFilteredLineNumber = filteredLineNumber.incrementAndGet();
						try {
							final List<String> mergedInputHeaders = new ArrayList<>(inputHeaders);
							final List<String> nextMergedLine = new ArrayList<>(l);
							nonMergeFieldsOrdered.stream().map(nextMapping -> nextMapping.getInputField())
									.forEachOrdered(inputField -> {
										if (otherH.contains(inputField) && !mergedInputHeaders.contains(inputField)) {
											mergedInputHeaders.add(inputField);
											nextMergedLine.add(l.get(otherH.indexOf(inputField)));
										}
									});

							final List<String> mapLine = ValueMapping.mapLine(otherH, nextMergedLine, previousLine,
									previousMappedLine, map, primaryKeys, nextLineNumber, nextFilteredLineNumber,
									mapLineConsumer);
							mapLineConsumer.accept(nextMergedLine, mapLine);
						} catch (final LineFilteredException e) {
							// Swallow line filtered exception and return
							// null below to eliminate it
							// We expect streamCSV to operate in sequential
							// order, print a warning if it doesn't
							final boolean success = filteredLineNumber.compareAndSet(nextFilteredLineNumber,
									nextFilteredLineNumber - 1);
							if (!success) {
								System.out.println("Line numbers may not be consistent");
							}
						}
					});
					// Any line that nevermatched any join lines must, for left outer join, be emitted separately
					final Predicate<List<String>> fullOuterJoinPredicate = l -> !matchedOtherLines.contains(l);
					otherLines.stream().filter(fullOuterJoinPredicate).forEach(fullOuterJoinConsumer);
				}
			}

			return outputHeaders;
		} finally {
			Files.deleteIfExists(tempInputFile);
			Files.deleteIfExists(tempOtherFile);
		}
	}
}
