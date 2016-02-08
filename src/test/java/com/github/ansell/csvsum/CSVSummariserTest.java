/**
 * 
 */
package com.github.ansell.csvsum;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVSummariser}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVSummariserTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVSummariser.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVSummariser.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVSummariser.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderNoLines() throws Exception {
		Path testFile = tempDir.newFile("test-single-header.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderOneLine() throws Exception {
		Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header-one-line.csv"),
				testFile, StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderOneLineFileOutput() throws Exception {
		Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
		Path testOutput = tempDir.newFile("test-single-header-one-line-output.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header-one-line.csv"),
				testFile, StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output",
				testOutput.toAbsolutePath().toString());

		assertEquals("Did not find enough lines in the output", 2, Files.readAllLines(testOutput).size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderOneLineEmptyValue() throws Exception {
		Path testFile = tempDir.newFile("test-single-header-one-line-empty-value.csv").toPath();
		Files.copy(
				this.getClass()
						.getResourceAsStream("/com/github/ansell/csvsum/test-single-header-one-line-empty-value.csv"),
				testFile, StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderMultipleLinesWithEmptyValues() throws Exception {
		Path testFile = tempDir.newFile("test-single-header-multiple-lines-empty-value.csv").toPath();
		Files.copy(
				this.getClass().getResourceAsStream(
						"/com/github/ansell/csvsum/test-single-header-multiple-lines-empty-value.csv"),
				testFile, StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderTwentyOneLines() throws Exception {
		Path testFile = tempDir.newFile("test-single-header-twenty-one-lines.csv").toPath();
		Files.copy(
				this.getClass()
						.getResourceAsStream("/com/github/ansell/csvsum/test-single-header-twenty-one-lines.csv"),
				testFile, StandardCopyOption.REPLACE_EXISTING);

		CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
	}

}
