/**
 * 
 */
package com.github.ansell.csv.db;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test CSVUpload using an in-memory Apache Derby instance.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVUploadTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// TODO: Create embedded Apache Derby:
		// https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#main(java.lang.String[])}.
	 */
	@Test
	public final void testMain() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#dropExistingTable(java.lang.String, java.sql.Connection)}
	 * .
	 */
	@Test
	public final void testDropExistingTable() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#createTable(java.lang.String, java.util.List, java.lang.StringBuilder, java.sql.Connection)}
	 * .
	 */
	@Test
	public final void testCreateTable() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#upload(java.lang.String, java.io.Reader, java.sql.Connection)}
	 * .
	 */
	@Test
	public final void testUpload() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#uploadLine(java.util.List, java.util.List, java.sql.PreparedStatement)}
	 * .
	 */
	@Test
	public final void testUploadLine() {
		fail("Not yet implemented"); // TODO
	}

}
