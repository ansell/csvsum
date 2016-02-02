/**
 * 
 */
package com.github.ansell.csvsum;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link CSVUtil}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVUtilTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVIllegalHeader() throws Exception {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Could not verify headers for csv file");
		CSVUtil.streamCSV(new StringReader("Header1"), h -> {
			throw new IllegalArgumentException("Did not find header: Header2");
		} , (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVEmpty() throws Exception {
		List<String> headers = new ArrayList<>();

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVUtil.streamCSV(new StringReader(""), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlySingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(new StringReader("Test1"), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlyMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(new StringReader("Test1, Another2, Else3"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowSingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(new StringReader("Test1\nAnswer1"), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(new StringReader("Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsSingleColumn() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(new StringReader("Test1\nAnswer1\nAnswer2\nAnswer3"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
		assertTrue(lines.contains(Arrays.asList("Answer2")));
		assertTrue(lines.contains(Arrays.asList("Answer3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVUtil#streamCSV(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsMultipleColumns() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVUtil.streamCSV(
				new StringReader(
						"Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3\nAnswer4, Alternative5, Attempt6\nAnswer7, Alternative8, Attempt9"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
		assertTrue(lines.contains(Arrays.asList("Answer4", "Alternative5", "Attempt6")));
		assertTrue(lines.contains(Arrays.asList("Answer7", "Alternative8", "Attempt9")));
	}

}
