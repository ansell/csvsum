/**
 * 
 */
package com.github.ansell.csvmap;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csvsum.CSVMapper;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVMapper}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVMapperTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testMapping;

	private Path testFile;

	@Before
	public void setUp() throws Exception {
		testMapping = tempDir.newFile("test-mapping.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-mapping.csv"), testMapping,
				StandardCopyOption.REPLACE_EXISTING);
		testFile = tempDir.newFile("test-source.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-source.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVMapper.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVMapper.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMapper.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainComplete() throws Exception {
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFile() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString(), "--output",
				testDirectory.resolve("test-output.csv").toString());
	}
}
