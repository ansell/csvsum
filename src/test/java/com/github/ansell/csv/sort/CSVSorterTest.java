/**
 * 
 */
package com.github.ansell.csv.sort;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.sort.CSVSorter;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.CSVStreamException;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVSorter}
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVSorterTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testInput1;
	private Path testInput2;
	private Path testInput3;
	private Path testInput4;
	private Path testInput5;
	private Path testOutput;

	private Path testDirectory;

	@Before
	public void setUp() throws Exception {
		testDirectory = tempDir.newFolder("csvsort-test").toPath();
		testOutput = testDirectory.resolve("testresult.csv");
		testInput1 = testDirectory.resolve("test1.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsort/test1.csv"), testInput1);
		testInput2 = testDirectory.resolve("test2.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsort/test2.csv"), testInput2);
		testInput3 = testDirectory.resolve("test3.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsort/test3.csv"), testInput3);
		testInput4 = testDirectory.resolve("test4.tsv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsort/test4.tsv"), testInput4);
		testInput5 = testDirectory.resolve("test5.tsv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsort/test5.tsv"), testInput5);
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sort.CSVSorter#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVSorter.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sort.CSVSorter#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVSorter.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sort.CSVSorter#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {

		thrown.expect(FileNotFoundException.class);
		CSVSorter.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--output",
				testOutput.toAbsolutePath().toString(), "--id-field-index", "0");
	}

	@Test
	public final void testRunSorterFirstColumn() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		
		CsvSchema schema = CsvSchema.builder().setUseHeader(false).build();
		verifyCSV(testInput1, 1, 2, 4, mapper, schema);

		try (Reader inputReader = Files.newBufferedReader(testInput1, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 1, schema, CSVSorter.getComparator(Arrays.asList(0)), true);
		}

		verifyCSV(testOutput, 1, 2, 4, mapper, schema);
	}

	@Test
	public final void testRunSorterSecondColumn() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		
		CsvSchema schema = CsvSchema.builder().setUseHeader(false).build();
		verifyCSV(testInput1, 1, 2, 4, mapper, schema);
		try (Reader inputReader = Files.newBufferedReader(testInput1, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 1, schema, CSVSorter.getComparator(Arrays.asList(1)), true);
		}

		verifyCSV(testOutput, 1, 2, 4, mapper, schema);
	}

	@Test
	public final void testRunSorterSecondColumnThenFirst() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);

		CsvSchema schema = CsvSchema.builder().setUseHeader(false).build();
		verifyCSV(testInput3, 1, 2, 5, mapper, schema);

		try (Reader inputReader = Files.newBufferedReader(testInput3, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 1, schema, CSVSorter.getComparator(Arrays.asList(1, 0)), true);
		}

		verifyCSV(testOutput, 1, 2, 5, mapper, schema);
	}

	@Test
	public final void testRunSorterFirstColumnThenSecond() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		
		CsvSchema schema = CsvSchema.builder().setUseHeader(false).build();
		verifyCSV(testInput3, 1, 2, 5, mapper, schema);

		try (Reader inputReader = Files.newBufferedReader(testInput3, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 1, schema, CSVSorter.getComparator(Arrays.asList(0, 1)), true);
		}

		verifyCSV(testOutput, 1, 2, 5, mapper, schema);
	}

	@Test
	public final void testRunSorterTSV() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		CsvSchema schema = CsvSchema.builder().setUseHeader(false).setColumnSeparator('\t').build();
		verifyCSV(testInput4, 1, 2, 5, mapper, schema);

		try (Reader inputReader = Files.newBufferedReader(testInput4, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 1, schema, CSVSorter.getComparator(Arrays.asList(0, 1)), true);
		}

		verifyCSV(testOutput, 1, 2, 5, mapper, schema);
	}

	@Test
	public final void testRunSorterTSVMultipleHeaderLines() throws Exception {
		CsvFactory csvFactory = new CsvFactory();
		csvFactory.enable(CsvParser.Feature.TRIM_SPACES);
		// csvFactory.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		csvFactory.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		CsvMapper mapper = new CsvMapper(csvFactory);
		mapper.enable(CsvParser.Feature.TRIM_SPACES);
		// mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
		// mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
		// true);
		CsvSchema schema = CsvSchema.builder().setUseHeader(false).setColumnSeparator('\t').build();
		verifyCSV(testInput5, 10, 2, 5, mapper, schema);

		try (Reader inputReader = Files.newBufferedReader(testInput5, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, 10, schema, CSVSorter.getComparator(Arrays.asList(0, 1)), true);
		}

		verifyCSV(testOutput, 10, 2, 5, mapper, schema);
	}

	private void verifyCSV(Path inputPath, int headerLines, int expectedHeaders, int expectedLines, CsvMapper mapper, CsvSchema schema)
			throws IOException, CSVStreamException {
		List<String> inputHeaders = new ArrayList<>();
		List<List<String>> inputLines = new ArrayList<>();
		// Verify that we can read the file ourselves
		try (Reader inputReader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
			CSVStream.parse(inputReader, h -> inputHeaders.addAll(h), (h, l) -> l, l -> inputLines.add(l), null,
					headerLines, CSVStream.defaultMapper(), schema);
		}
		Files.readAllLines(inputPath, StandardCharsets.UTF_8).stream().forEach(System.out::println);
		assertEquals(expectedHeaders, inputHeaders.size());
		assertEquals(expectedLines, inputLines.size());
	}

}
