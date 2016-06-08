/**
 * 
 */
package com.github.ansell.csv.db;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import joptsimple.OptionException;

/**
 * Test CSVUpload using an in-memory Apache Derby instance for each test.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVUploadTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TestName testName = new TestName();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testDir;

	private Connection conn;

	private String databaseConnectionString;

	private String tableString;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// Create embedded Apache Derby:
		// https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html
		System.out.println("Creating Derby database instance for test: " + testName.getMethodName());

		databaseConnectionString = "jdbc:derby:" + testName.getMethodName();
		conn = DriverManager.getConnection(databaseConnectionString + ";create=true");

		tableString = "testTable";

		testDir = tempDir.newFolder(testName.getMethodName()).toPath();

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		try {
			conn.rollback();
		} finally {
			try {
				conn.close();
			} finally {
				try {
					// Shutdown embedded Apache Derby:
					// https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html
					DriverManager.getConnection("jdbc:derby:" + testName.getMethodName() + ";shutdown=true");
					fail("Did not find expected exception when shutting down Derby instance for test: "
							+ testName.getMethodName());
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					assertEquals("Database '" + testName.getMethodName() + "' shutdown.", e.getMessage());
				}
			}
		}
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVUpload.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#dropExistingTable(java.lang.String, java.sql.Connection)}
	 * .
	 */
	@Ignore("TODO: Implement me!")
	@Test
	public final void testDropExistingTable() throws Exception {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#createTable(java.lang.String, java.util.List, java.lang.StringBuilder, java.sql.Connection)}
	 * .
	 */
	@Ignore("TODO: Implement me!")
	@Test
	public final void testCreateTable() throws Exception {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#upload(java.lang.String, java.io.Reader, java.sql.Connection)}
	 * .
	 */
	@Ignore("TODO: Implement me!")
	@Test
	public final void testUpload() throws Exception {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.db.CSVUpload#uploadLine(java.util.List, java.util.List, java.sql.PreparedStatement)}
	 * .
	 */
	@Ignore("TODO: Implement me!")
	@Test
	public final void testUploadLine() throws Exception {
		fail("Not yet implemented"); // TODO
	}
}
