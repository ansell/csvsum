/**
 * 
 */
package com.github.ansell.csv.sort;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.sort.CSVSorter;

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

	private Path testOutput;

	@Before
	public void setUp() throws Exception {
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
	@Ignore("TODO: Implement me")
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVSorter.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--output",
				testOutput.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sort.CSVSorter#runSorter(java.io.Reader, boolean, java.lang.String, java.nio.file.Path, com.fasterxml.jackson.dataformat.csv.CsvMapper, com.fasterxml.jackson.dataformat.csv.CsvSchema, java.util.Comparator)}.
	 */
	@Ignore("TODO: Implement me")
	@Test
	public final void testRunSorter() {
	}

}
