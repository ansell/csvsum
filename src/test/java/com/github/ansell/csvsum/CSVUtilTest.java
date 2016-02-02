/**
 * 
 */
package com.github.ansell.csvsum;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.util.ArrayList;
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
	public final void testStreamCSVEmpty() throws Exception {
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVUtil.streamCSV(new StringReader(""), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
	}

}
