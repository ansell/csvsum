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
package com.github.ansell.csv.merge;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.util.CSVUtil;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVMerger}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVMergerTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testMapping;

	private Path testMappingHidden;

	private Path testFile;

	private Path testOtherFile;

	@Before
	public void setUp() throws Exception {
		testMapping = tempDir.newFile("test-mapping.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmerge/test-mapping.csv"), testMapping,
				StandardCopyOption.REPLACE_EXISTING);
		testMappingHidden = tempDir.newFile("test-mapping-with-hidden.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmerge/test-mapping-with-hidden.csv"),
				testMappingHidden, StandardCopyOption.REPLACE_EXISTING);
		testFile = tempDir.newFile("test-source.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmerge/test-source.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
		testOtherFile = tempDir.newFile("test-source-other.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmerge/test-source-other.csv"),
				testOtherFile, StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVMerger.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVMerger.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMerger.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainOtherFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--otherInput",
				testDirectory.resolve("test-does-not-exist.csv").toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--mapping",
				testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainComplete() throws Exception {
		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFile() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString(),
				"--output", testDirectory.resolve("test-output.csv").toString());

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(testDirectory.resolve("test-output.csv"));) {
			CSVUtil.streamCSV(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(12, headers.size());
		assertEquals(4, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("A1", "A1", "A2", "", "A3", "", "A4", "Useful", "A5a", "ZZ1", "A1", "Interesting"),
				lines.get(0));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMerger#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFileHidden() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMerger.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMappingHidden.toAbsolutePath().toString(), "--otherInput",
				testOtherFile.toAbsolutePath().toString(), "--output",
				testDirectory.resolve("test-output.csv").toString());

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(testDirectory.resolve("test-output.csv"));) {
			CSVUtil.streamCSV(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(7, headers.size());
		assertEquals(6, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("A1", "A2", "", "A3", "", "A4", "Useful"), lines.get(0));
	}
}
