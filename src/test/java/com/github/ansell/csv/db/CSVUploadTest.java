/**
 * 
 */
package com.github.ansell.csv.db;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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

import com.github.ansell.csv.sum.CSVSummariser;

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

		testDir = tempDir.newFolder(testName.getMethodName()).toPath();

		System.setProperty("derby.system.home", testDir.toAbsolutePath().toString());

		databaseConnectionString = "jdbc:derby:" + testName.getMethodName();
		conn = DriverManager.getConnection(databaseConnectionString + ";create=true");

		tableString = "testTable";
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		try {
			conn.rollback();
		} finally {
			try (final Statement deleteTable = conn.createStatement();) {
				deleteTable.executeUpdate("DROP TABLE " + tableString);
			} catch (SQLException e) {
				assertEquals("'DROP TABLE' cannot be performed on 'TESTTABLE' because it does not exist.",
						e.getMessage());
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
					} finally {
						Files.walkFileTree(testDir, new SimpleFileVisitor<Path>() {
							@Override
							public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
								Files.delete(file);
								return FileVisitResult.CONTINUE;
							}

							@Override
							public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
								Files.delete(dir);
								return FileVisitResult.CONTINUE;
							}
						});
					}
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
		Path testFile = testDir.resolve("test-empty.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString(), "--field-type", "CLOB");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderNoLines() throws Exception {
		Path testFile = testDir.resolve("test-single-header.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header.csv"), testFile);

		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString(), "--debug", "true", "--field-type", "CLOB");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderOneLine() throws Exception {
		Path testFile = testDir.resolve("test-single-header-one-line.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header-one-line.csv"),
				testFile);

		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString(), "--debug", "true", "--field-type", "CLOB");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMultiKey() throws Exception {
		Path testFile = testDir.resolve("test-source-multi-key.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-multi-key.csv"),
				testFile);

		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString(), "--debug", "true", "--field-type", "CLOB");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSingleHeaderTwentyOneLines() throws Exception {
		Path testFile = testDir.resolve("test-single-header-twenty-one-lines.csv");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header-twenty-one-lines.csv"),
				testFile);

		CSVUpload.main("--database", databaseConnectionString, "--table", tableString, "--input",
				testFile.toAbsolutePath().toString(), "--debug", "true", "--field-type", "CLOB");
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
