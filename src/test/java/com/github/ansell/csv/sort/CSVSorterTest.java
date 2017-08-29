/**
 * 
 */
package com.github.ansell.csv.sort;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.sort.CSVSorter;
import com.github.ansell.csv.stream.CSVStream;

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
	public final void testRunSorter() throws Exception {
		try (Reader inputReader = Files.newBufferedReader(testInput1, StandardCharsets.UTF_8)) {
			CSVSorter.runSorter(inputReader, testOutput, CSVStream.defaultMapper(),
					CSVStream.buildSchema(Arrays.asList("testField1")), CSVSorter.getComparator(0));
		}
		
		List<String> resultLines = Files.readAllLines(testOutput, StandardCharsets.UTF_8);
		assertEquals(5, resultLines.size());

		try (Reader outputReader = Files.newBufferedReader(testOutput, StandardCharsets.UTF_8)) {
			CSVStream.parse(outputReader, h -> {
			}, (h, l) -> l, l -> {
			});
		}
	}

}
