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
package com.github.ansell.csv.access;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.ConsumerRunnable;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;
import com.github.ansell.jdefaultdict.JDefaultDict;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.Joiner;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Mapper from Access to CSV files.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class AccessMapper {

	private static final String DOT_REGEX = "\\.";
	private static final Pattern DOT_PATTERN = Pattern.compile(DOT_REGEX);
	private static final String COMMA_REGEX = "\\,";
	private static final Pattern COMMA_PATTERN = Pattern.compile(COMMA_REGEX);

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input Access file to be mapped.");
		final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg().ofType(File.class).required()
				.describedAs("The mapping file.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class).required()
				.describedAs("The directory to contain the mapped file.");
		final OptionSpec<String> outputPrefix = parser.accepts("prefix").withRequiredArg().ofType(String.class)
				.defaultsTo("Mapped-").describedAs("The prefix to use to name the mapped files.");
		final OptionSpec<Boolean> debug = parser.accepts("debug").withOptionalArg().ofType(Boolean.class)
				.defaultsTo(Boolean.FALSE).describedAs("Set to true to debug the table structures");
		final OptionSpec<Integer> threads = parser.accepts("threads").withOptionalArg().ofType(Integer.class)
				.defaultsTo(2).describedAs("The number of parallel threads to use for mapping.");

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
			throw new FileNotFoundException("Could not find input Access file: " + inputPath.toString());
		}

		final Path mappingPath = mapping.value(options).toPath();
		if (!Files.exists(mappingPath)) {
			throw new FileNotFoundException("Could not find mapping CSV file: " + mappingPath.toString());
		}

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);) {
			try (final InputStream readerDB = Files.newInputStream(inputPath);) {
				dumpToCSVs(readerDB, output.value(options).toPath(), outputPrefix.value(options), debug.value(options));
			}
			// Read the database again to map it to a single CSV
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);

			// Do sanity check on the access mappings
			List<ValueMapping> accessMappings = checkAccessMappings(map);
			try (final InputStream readerDB = Files.newInputStream(inputPath);) {
				mapDBToSingleCSV(readerDB, map, output.value(options).toPath(), outputPrefix.value(options) + "Single-",
						threads.value(options));
			}
		}
	}

	private static List<ValueMapping> checkAccessMappings(List<ValueMapping> map) {

		// Check that there are unique destination tables across all of the
		// mappings
		// There will be no consistency if a destination table is mapped
		// multiple times, as we require a DAG to be sure that we can start at
		// the origin table and reach other tables uniquely
		Set<String> overallDestTables = new LinkedHashSet<>();

		List<ValueMapping> accessMappings = map.stream().filter(m -> m.getLanguage() == ValueMappingLanguage.ACCESS)
				.map(m -> {
					String[] destFields = COMMA_PATTERN.split(m.getMapping());
					String[] sourceFields = COMMA_PATTERN.split(m.getInputField());

					if (destFields.length != sourceFields.length) {
						throw new RuntimeException("Source and destination mapping fields must be equal size: " + m);
					}

					Set<String> destFieldsSet = new LinkedHashSet<>(Arrays.asList(destFields));

					if (destFieldsSet.size() != destFields.length) {
						throw new RuntimeException("Destination mapping contained duplicates: " + m);
					}

					Set<String> sourceTables = new LinkedHashSet<>();
					Set<String> destTables = new LinkedHashSet<>();

					for (int i = 0; i < destFields.length; i++) {
						String[] destField = DOT_PATTERN.split(destFields[i]);
						destTables.add(destField[0]);
						String[] sourceField = DOT_PATTERN.split(sourceFields[i]);
						sourceTables.add(sourceField[0]);
					}

					if (sourceTables.size() != 1) {
						throw new RuntimeException("Cannot map from multiple source tables: " + m);
					}

					if (destTables.size() != 1) {
						throw new RuntimeException("Cannot map to multiple destination tables: " + m);
					}

					if (overallDestTables.contains(destTables.iterator().next())) {
						throw new RuntimeException(
								"Cannot map to a destination table from multiple Access mappings: " + m);
					}

					overallDestTables.add(destTables.iterator().next());

					return m;
				}).collect(Collectors.toList());

		if (overallDestTables.size() != accessMappings.size()) {
			throw new RuntimeException(
					"There is not a unique mapping for each destination table in the access mappings: "
							+ accessMappings.size() + " mappings but only " + overallDestTables.size()
							+ " destination tables.");
		}

		return accessMappings;
	}

	private static void mapDBToSingleCSV(InputStream readerDB, List<ValueMapping> map, Path csvPath, String csvPrefix,
			int parallelism) throws IOException, InterruptedException {

		final Path tempDBPath = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(readerDB, tempDBPath, StandardCopyOption.REPLACE_EXISTING);

		// Ordered mappings so that the first table in the mapping is the
		// one to perform the base joins on
		final JDefaultDict<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMapping = new JDefaultDict<>(
				k -> new ConcurrentHashMap<>());
		final ConcurrentMap<ValueMapping, Joiner> joiners = new ConcurrentHashMap<>();

		final String originTable = mapAndGetOriginTable(tempDBPath, map, foreignKeyMapping, joiners);

		// There may have been no mappings...
		if (originTable != null) {
			List<String> headers = map.stream().filter(k -> k.getShown()).map(m -> m.getOutputField())
					.collect(Collectors.toList());
			final CsvSchema schema = CSVUtil.buildSchema(headers);

			try (final Writer csv = Files.newBufferedWriter(csvPath.resolve(csvPrefix + originTable + ".csv"));
					final SequenceWriter csvWriter = CSVUtil.newCSVWriter(new BufferedWriter(csv), schema);) {

				// Setup the writer first
				final Queue<List<String>> writerQueue = new ConcurrentLinkedQueue<>();
				final List<String> writerSentinel = new ArrayList<>();
				final Consumer<List<String>> writerConsumer = Unchecked.consumer(l -> {
					csvWriter.write(l);
				});
				final Thread writerThread = new Thread(
						ConsumerRunnable.from(writerQueue, writerConsumer, writerSentinel));
				writerThread.start();

				List<Thread> mapThreads = new ArrayList<>(parallelism);
				List<Database> dbCopies = new ArrayList<>(parallelism);

				Queue<Map<String, Object>> originRowQueue = new ConcurrentLinkedQueue<>();
				final Map<String, Object> originRowSentinel = new HashMap<String, Object>();
				try {
					final Thread originRowThread = new Thread(() -> {
						try {
							final Path tempDBFileForThread = Files.createTempFile("Source-accessdb-originrows-",
									".accdb");
							Files.copy(tempDBPath, tempDBFileForThread, StandardCopyOption.REPLACE_EXISTING);
							try (final Database db = DatabaseBuilder.open(tempDBFileForThread.toFile());) {
								for (Map<String, Object> nextRow : db.getTable(originTable)) {
									originRowQueue.add(nextRow);
								}
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						} finally {
							// Add a sentinel for each of the map threads
							for (int i = 0; i < parallelism; i++) {
								originRowQueue.add(originRowSentinel);
							}
						}
					});

					for (int i = 0; i < parallelism; i++) {
						// Take a separate physical copy of the database for
						// each thread to avoid any underlying issues with
						// threadsafety since it isn't guaranteed at any level
						// of the Jackcess API
						final JDefaultDict<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMappingForThread = new JDefaultDict<>(
								k -> new ConcurrentHashMap<>());
						final ConcurrentMap<ValueMapping, Joiner> joinersForThread = new ConcurrentHashMap<>();
						final Path tempDBFileForThread = Files.createTempFile("Source-accessdb-mapthread-" + i + "-",
								".accdb");
						Files.copy(tempDBPath, tempDBFileForThread, StandardCopyOption.REPLACE_EXISTING);
						final Database db = DatabaseBuilder.open(tempDBFileForThread.toFile());
						dbCopies.add(db);
						final String nextOriginTable = parseTableMappings(map, db, foreignKeyMappingForThread,
								joinersForThread);
						final Consumer<Map<String, Object>> originRowConsumer = Unchecked.consumer(r -> {
							List<String> mappedRow = mapNextRow(map, foreignKeyMappingForThread, joinersForThread,
									nextOriginTable, r, db);
							if (mappedRow != null) {
								writerQueue.add(mappedRow);
							}
						});
						final Thread mapThread = new Thread(
								ConsumerRunnable.from(originRowQueue, originRowConsumer, originRowSentinel));
						mapThreads.add(mapThread);
						mapThread.start();
					}

					originRowThread.start();

				} finally {
					try {
						for (Thread nextMapThread : mapThreads) {
							nextMapThread.join();
						}
					} finally {
						try {
							for (Database nextDBCopy : dbCopies) {
								try {
									nextDBCopy.close();
								} catch (Throwable e) {
								}
							}
						} finally {
							try {
								// Add a sentinel to the end of the queue to
								// signal so the writer thread can finish
								writerQueue.add(writerSentinel);
							} finally {
								// Wait for the writer to finish
								writerThread.join();
							}
						}
					}
				}
			}
		}
	}

	private static String mapAndGetOriginTable(Path tempDBPath, List<ValueMapping> map,
			final JDefaultDict<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMapping,
			final ConcurrentMap<ValueMapping, Joiner> joiners) throws IOException {
		try (final Database db = DatabaseBuilder.open(tempDBPath.toFile());) {
			// Populate the table mapping for each value mapping
			return parseTableMappings(map, db, foreignKeyMapping, joiners);
		}
	}

	private static String parseTableMappings(List<ValueMapping> map, final Database db,
			JDefaultDict<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMapping,
			ConcurrentMap<ValueMapping, Joiner> joiners) throws IOException {
		String originTable = map.isEmpty() ? null
				: db.getTable(DOT_PATTERN.split(map.get(0).getInputField())[0]).getName();
		// for (final ValueMapping nextValueMapping : map) {
		// Must be a sequential mapping as ordering is important
		map.stream().sequential().forEach(Unchecked.consumer(nextValueMapping -> {
			if (nextValueMapping.getLanguage() == ValueMappingLanguage.ACCESS) {
				final String[] splitDBField = DOT_PATTERN.split(nextValueMapping.getInputField());
				System.out.println(nextValueMapping.getInputField());
				final Table nextTable = db.getTable(splitDBField[0]);

				final String[] splitForeignDBField = DOT_PATTERN.split(nextValueMapping.getMapping());
				final Table nextForeignTable = db.getTable(splitForeignDBField[0]);
				if (nextForeignTable == null) {
					throw new RuntimeException(
							"Could not find table referenced by access mapping: " + nextValueMapping.getMapping());
				}
				foreignKeyMapping.get(splitForeignDBField[0]).put(nextValueMapping,
						Tuple.tuple(nextTable.getName(), nextForeignTable.getName()));
				try {
					final Joiner create = Joiner.create(nextTable, nextForeignTable);
					if (create != null) {
						joiners.put(nextValueMapping, create);
						System.out.println("PK->FK: " + joiners.get(nextValueMapping).toFKString());
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
		}));
		return originTable;
	}

	private static List<String> mapNextRow(List<ValueMapping> map,
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMapping,
			ConcurrentMap<ValueMapping, Joiner> joiners, String originTable, Map<String, Object> nextRow,
			Database database) throws IOException {
		// Rows, indexed by the table that they came from
		ConcurrentMap<String, Map<String, Object>> componentRowsForThisRow = new ConcurrentHashMap<>();
		componentRowsForThisRow.put(originTable, nextRow);
		for (final ValueMapping nextValueMapping : map) {
			if (nextValueMapping.getLanguage() == ValueMappingLanguage.ACCESS) {
				String[] splitDBFieldSource = DOT_PATTERN.split(nextValueMapping.getInputField());
				String[] splitDBFieldDest = DOT_PATTERN.split(nextValueMapping.getMapping());
				if (splitDBFieldDest.length < 2) {
					throw new RuntimeException(
							"Destination mapping was not in the 'table.column' format: " + nextValueMapping);
				}
				if (!componentRowsForThisRow.containsKey(splitDBFieldDest[0])) {
					// If we have a mapping to another table for the input
					// field, then use it
					if (foreignKeyMapping.containsKey(splitDBFieldDest[0])) {
						if (joiners.containsKey(nextValueMapping)) {
							getRowFromJoiner(joiners, componentRowsForThisRow, nextValueMapping, splitDBFieldSource,
									splitDBFieldDest);
						} else {
							getRowFromTables(foreignKeyMapping, componentRowsForThisRow, splitDBFieldDest, database);
						}
					}
				}
			}
		}

		// Populate the foreign row values
		ConcurrentMap<String, String> output = new ConcurrentHashMap<>();
		// for (final ValueMapping nextValueMapping : map) {
		map.parallelStream().forEach(Unchecked.consumer(nextValueMapping -> {
			String[] splitDBField = DOT_PATTERN.split(nextValueMapping.getInputField());
			if (splitDBField.length >= 2) {
				if (componentRowsForThisRow.containsKey(splitDBField[0])) {
					Map<String, Object> findFirstRow = componentRowsForThisRow.get(splitDBField[0]);
					Object nextColumnValue = findFirstRow.get(splitDBField[1]);
					if (nextColumnValue != null) {
						if (nextColumnValue instanceof Date) {
							Date nextColumnDate = (Date) nextColumnValue;
							output.put(nextValueMapping.getOutputField(),
									CSVUtil.oldDateToISO8601LocalDateTime(nextColumnDate));
						} else {
							output.put(nextValueMapping.getOutputField(), nextColumnValue.toString());
						}
					}
				}
			} else {
				output.put(nextValueMapping.getOutputField(), "");
			}
		}));
		// }

		List<String> nextEmittedRow = new ArrayList<>(map.size());
		// Then after all are filled, emit the row
		List<String> inputHeaders = new CopyOnWriteArrayList<>();
		for (final ValueMapping nextValueMapping : map) {
			inputHeaders.add(nextValueMapping.getInputField());
			nextEmittedRow.add(output.getOrDefault(nextValueMapping.getOutputField(), ""));
		}

		try {
			return ValueMapping.mapLine(inputHeaders, nextEmittedRow, map);
		} catch (final LineFilteredException e) {
			// Swallow line filtered exception and return null below to
			// eliminate it
		}
		return null;
	}

	private static void getRowFromTables(
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<String, String>>> foreignKeyMapping,
			Map<String, Map<String, Object>> componentRowsForThisRow, String[] splitDBFieldDest, Database database)
					throws IOException {

		if (!foreignKeyMapping.containsKey(splitDBFieldDest[0])) {
			throw new RuntimeException("No mappings found to the destination table: " + splitDBFieldDest);
		}

		// A joiner could not be created for this case as the original database
		// did not setup an actual foreign key for the relationship
		ConcurrentMap<ValueMapping, Tuple2<String, String>> mapping = foreignKeyMapping.get(splitDBFieldDest[0]);

		for (Entry<ValueMapping, Tuple2<String, String>> entry : mapping.entrySet()) {
			ValueMapping nextMapping = entry.getKey();
			String origin = entry.getValue().v1();
			String destName = entry.getValue().v2();

			if (componentRowsForThisRow.containsKey(destName)) {
				// Short circuit as we have mapped this destination table
				// already
				continue;
			}

			if (!splitDBFieldDest[0].equals(destName)) {
				// We are not doing this mapping yet
				continue;
			}

			Map<String, Object> originRow = componentRowsForThisRow.get(origin);
			if (originRow == null) {
				// System.out.println("Could not find row: Maybe the order of
				// the mapping file needs changing: origin="
				// + origin + " mapping=" + nextMapping);
				continue;
			}

			Map<String, Object> singletonMap = buildMatchMap(nextMapping, originRow);

			// Cursor cursor = dest.getDefaultCursor();

			// HACK: This will not work if they are not looking for the primary
			// key on the destination
			Cursor cursor = null;
			String primaryKeyIndexName = null;

			Table dest = database.getTable(destName);
			try {
				Index primaryKeyIndex = dest.getPrimaryKeyIndex();

				if (primaryKeyIndex != null) {
					cursor = primaryKeyIndex.newCursor().toIndexCursor();
					primaryKeyIndexName = primaryKeyIndex.getName();
				}
			} catch (IllegalArgumentException e) {
				cursor = null;
			}

			boolean findFirstRow = false;

			if (cursor != null) {
				findFirstRow = cursor.findFirstRow(singletonMap);
			}

			if (cursor != null && findFirstRow) {
				Row currentRow = cursor.getCurrentRow();
				componentRowsForThisRow.put(splitDBFieldDest[0], currentRow);
			} else {
				boolean findFirstRowOtherIndexes = false;

				// Note, keeping the following code for reference, but it is
				// much slower than just looking through the table, in the
				// absence of a primary key index
				if (false) {
					// If the fast index cursor did not work fall back to the
					// slow default cursor
					for (Index index : dest.getIndexes()) {
						// break out if we have already found the row
						if (findFirstRowOtherIndexes) {
							break;
						}

						// Already checked the primary key index above as a
						// priority for performance
						if (index.getName() != null && index.getName().equals(primaryKeyIndexName)) {
							continue;
						}

						Cursor indexCursor = index.newCursor().toIndexCursor();

						findFirstRowOtherIndexes = indexCursor.findFirstRow(singletonMap);

						if (findFirstRowOtherIndexes) {
							Row currentIndexRow = indexCursor.getCurrentRow();
							componentRowsForThisRow.put(splitDBFieldDest[0], currentIndexRow);
						}
					}
				}

				// Only resort to the default, non-indexed, cursor if we didn't
				// find it on any of the indexes
				if (!findFirstRowOtherIndexes) {
					Cursor slowCursor = dest.getDefaultCursor();

					boolean slowFindFirstRow = slowCursor.findFirstRow(singletonMap);

					if (slowFindFirstRow) {
						Row currentRow = slowCursor.getCurrentRow();
						componentRowsForThisRow.put(splitDBFieldDest[0], currentRow);
					}
				}
			}
		}
	}

	private static Map<String, Object> buildMatchMap(ValueMapping mapping, Map<String, Object> originRow) {
		// System.out.println("Building match map for: " + mapping + " row=" +
		// originRow);
		Map<String, Object> result = new HashMap<>();

		String[] destFields = COMMA_PATTERN.split(mapping.getMapping());
		String[] sourceFields = COMMA_PATTERN.split(mapping.getInputField());

		for (int i = 0; i < destFields.length; i++) {
			String[] destField = DOT_PATTERN.split(destFields[i]);
			String[] sourceField = DOT_PATTERN.split(sourceFields[i]);
			if (!originRow.containsKey(sourceField[1])) {
				throw new RuntimeException("Origin row did not contain a field required for mapping: field="
						+ sourceFields[i] + " mapping=" + mapping);
			}
			Object nextFKValue = originRow.get(sourceField[1]);
			if (nextFKValue == null) {
				// Return an empty result if one of the source fields was null
				return new HashMap<>();
			}
			if (result.containsKey(destField[1])) {
				throw new RuntimeException("Destination row contained a duplicate field name: field=" + destFields[i]
						+ " mapping=" + mapping);
			}
			result.put(destField[1], nextFKValue);
		}

		return result;
	}

	private static void getRowFromJoiner(ConcurrentMap<ValueMapping, Joiner> joiners,
			Map<String, Map<String, Object>> componentRowsForThisRow, final ValueMapping nextValueMapping,
			String[] splitDBField, String[] splitDBFieldOutput) throws IOException {
		String key = splitDBField[0];
		Map<String, Object> fromRow = componentRowsForThisRow.get(key);

		if (fromRow == null) {
			// System.out.println("Could not find any linked rows with the key:
			// " + key);
		} else {
			Map<String, Object> findFirstRow = joiners.get(nextValueMapping).findFirstRow(fromRow);
			if (findFirstRow != null) {
				componentRowsForThisRow.put(splitDBFieldOutput[0], findFirstRow);
			}
		}
	}

	private static void dumpToCSVs(InputStream input, Path outputDir, String csvPrefix, boolean debug)
			throws IOException {
		Path tempFile = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);

		final CsvSchema schema = CSVUtil.buildSchema(Arrays.asList("OldField", "NewField", "Language", "Mapping"));
		try (final Database db = DatabaseBuilder.open(tempFile.toFile());
				final Writer columnCsv = Files
						.newBufferedWriter(outputDir.resolve(csvPrefix + "AutoMapping-Columns.csv"));
				final SequenceWriter columnCsvWriter = CSVUtil.newCSVWriter(new BufferedWriter(columnCsv), schema);) {
			for (String tableName : db.getTableNames()) {
				Table table = db.getTable(tableName);

				if (debug) {
					debugTable(table, columnCsvWriter);
				}

				System.out.println("");
				String csvName = csvPrefix + tableName + ".csv";
				Path csvPath = outputDir.resolve(csvName);
				System.out.println("Converting " + tableName + " to CSV: " + csvPath.toAbsolutePath().toString());

				String[] tempArray = new String[table.getColumnCount()];
				int x = 0;
				for (Column nextColumn : table.getColumns()) {
					tempArray[x++] = nextColumn.getName();
				}

				final CsvSchema fullFileSchema = CSVUtil.buildSchema(Arrays.asList(tempArray));
				try (final Writer fullFileCsv = Files.newBufferedWriter(csvPath);
						final SequenceWriter fullFileCsvWriter = CSVUtil.newCSVWriter(new BufferedWriter(fullFileCsv),
								fullFileSchema);) {
					int rows = 0;
					for (Row nextRow : table) {
						int i = 0;
						for (Object nextValue : nextRow.values()) {
							if (nextValue == null) {
								tempArray[i++] = null;
							} else if (nextValue instanceof Date) {
								tempArray[i++] = CSVUtil.oldDateToISO8601LocalDateTime((Date) nextValue);
							} else {
								tempArray[i++] = nextValue.toString();
							}
						}
						fullFileCsvWriter.write(Arrays.asList(tempArray));
						rows++;
					}
					System.out.println("Converted " + rows + " rows from table " + tableName);
				}
				System.out.println("");
				System.out.println("----------------------------");
			}
		}
	}

	private static void debugTable(Table table, SequenceWriter columnCsv) throws IOException {

		System.out.println("\tTable columns for " + table.getName());

		try {
			for (Column nextColumn : table.getColumns()) {
				System.out.println("\t\t" + nextColumn.getName());
				columnCsv.write(Arrays.asList(table.getName() + "." + nextColumn.getName(),
						table.getName() + "." + nextColumn.getName(), "", ""));
			}

			Index primaryKeyIndex = table.getPrimaryKeyIndex();
			System.out.println(
					"\tFound primary key index for table: " + table.getName() + " named " + primaryKeyIndex.getName());
			debugIndex(primaryKeyIndex, new HashSet<>(), columnCsv);

			for (Index nextIndex : table.getIndexes()) {
				if (!nextIndex.getName().equals(primaryKeyIndex.getName())) {
					System.out.println("\tFound non-primary key index for table: " + table.getName() + " named "
							+ nextIndex.getName());
					debugIndex(nextIndex, new HashSet<>(), null);
				}
			}
		} catch (IllegalArgumentException e) {
			System.out.println("No primary key index found for table: " + table.getName());
		}

		Cursor cursor = table.getDefaultCursor();
		int i = 0;
		while (cursor.moveToNextRow()) {
			if (i >= 5) {
				break;
			}
			System.out.println(cursor.getCurrentRow().toString());
			i++;
		}
	}

	private static void debugIndex(Index index, Set<Index> visited, SequenceWriter csvWriter) throws IOException {
		visited.add(index);
		System.out.println("\t\tIndex columns:");
		StringBuilder columnList = new StringBuilder();
		for (Index.Column nextColumn : index.getColumns()) {
			System.out.print("\t\t\t" + nextColumn.getName());

			if (columnList.length() > 0) {
				columnList.append(",");
			}
			columnList.append(index.getTable().getName() + "." + nextColumn.getName());
		}
		if (csvWriter != null) {
			csvWriter.write(
					Arrays.asList(columnList.toString(), columnList.toString(), "Access", columnList.toString()));
		}

		System.out.println("");
		Index referencedIndex = index.getReferencedIndex();
		if (referencedIndex != null) {
			System.out.println("\t" + index.getName() + " references another index: " + referencedIndex.getName());
			if (!visited.contains(referencedIndex)) {
				visited.add(referencedIndex);
				debugIndex(referencedIndex, visited, null);
			}

		}

	}

}
