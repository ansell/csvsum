package com.github.ansell.csv.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;

public class ValueMappingTest {

	private ValueMapping testDefaultMapping;
	private ValueMapping testDefaultMapping2;
	private ValueMapping testJavascriptMapping;

	@Before
	public void setUp() throws Exception {
		testDefaultMapping = ValueMapping.newMapping("Default", "anInput", "anotherField", "");
		testDefaultMapping2 = ValueMapping.newMapping("Default", "anInput", "anotherField", "inputValue");
		testJavascriptMapping = ValueMapping.newMapping("Javascript", "aDifferentInput", "aDifferentField",
				"return inputValue;");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testHashCode() {
		assertEquals(testDefaultMapping2.hashCode(), testDefaultMapping2.hashCode());
		assertNotEquals(testDefaultMapping.hashCode(), testJavascriptMapping.hashCode());
	}

	@Test
	public final void testExtractMappings() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testMapLine() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testEqualsObject() {
		assertEquals(testDefaultMapping, testDefaultMapping2);
		assertNotEquals(testDefaultMapping, testJavascriptMapping);
	}

	@Test
	public final void testGetInputField() {
		assertEquals("anInput", testDefaultMapping.getInputField());
		assertEquals("aDifferentInput", testJavascriptMapping.getInputField());
	}

	@Test
	public final void testGetLanguage() {
		assertEquals(ValueMappingLanguage.DEFAULT, testDefaultMapping.getLanguage());
		assertEquals(ValueMappingLanguage.JAVASCRIPT, testJavascriptMapping.getLanguage());
	}

	@Test
	public final void testGetMapping() {
		assertEquals(ValueMapping.DEFAULT_MAPPING, testDefaultMapping.getMapping());
		assertEquals(ValueMapping.DEFAULT_MAPPING, testDefaultMapping2.getMapping());
		assertEquals("return inputValue;", testJavascriptMapping.getMapping());
	}

	@Test
	public final void testGetOutputField() {
		assertEquals("anotherField", testDefaultMapping.getOutputField());
		assertEquals("anotherField", testDefaultMapping2.getOutputField());
		assertEquals("aDifferentField", testJavascriptMapping.getOutputField());
	}

}
