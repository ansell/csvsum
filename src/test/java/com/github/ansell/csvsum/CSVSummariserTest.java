/**
 * 
 */
package com.github.ansell.csvsum;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVSummariser}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVSummariserTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Test method for
	 * {@link com.github.ansell.csvsum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVSummariser.main();
	}

}
