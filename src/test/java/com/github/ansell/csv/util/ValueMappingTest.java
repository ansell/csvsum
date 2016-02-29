package com.github.ansell.csv.util;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;

public class ValueMappingTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private ValueMapping testDefaultMapping;
	private ValueMapping testDefaultMapping2;
	private ValueMapping testJavascriptMapping;
	private ValueMapping testDefaultMapping3;

	@Before
	public void setUp() throws Exception {
		testDefaultMapping = ValueMapping.newMapping("Default", "anInput", "anotherField", "", "");
		testDefaultMapping2 = ValueMapping.newMapping("Default", "anInput", "anotherField", "inputValue", "");
		testDefaultMapping3 = ValueMapping.newMapping("Default", "anInput3", "anotherField3", "inputValue", "");
		testJavascriptMapping = ValueMapping.newMapping("Javascript", "aDifferentInput", "aDifferentField",
				"return inputValue.substring(0, 1);", "");
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
	public final void testExtractMappingsDefault() throws Exception {
		List<ValueMapping> extractMappings = ValueMapping.extractMappings(
				new StringReader("OldField,NewField,Shown,Language,Mapping\ninputField,outputField,,,\n"));

		assertEquals(1, extractMappings.size());
		assertEquals("inputField", extractMappings.get(0).getInputField());
		assertEquals("outputField", extractMappings.get(0).getOutputField());
		assertEquals(ValueMappingLanguage.DEFAULT, extractMappings.get(0).getLanguage());
		assertEquals(ValueMapping.DEFAULT_MAPPING, extractMappings.get(0).getMapping());
	}

	@Test
	public final void testExtractMappingsJavascript() throws Exception {
		List<ValueMapping> extractMappings = ValueMapping.extractMappings(new StringReader(
				"OldField,NewField,Shown,Language,Mapping\ninputField2,outputField2,,Javascript,\"return inputValue.trim();\"\n"));

		assertEquals(1, extractMappings.size());
		assertEquals("inputField2", extractMappings.get(0).getInputField());
		assertEquals("outputField2", extractMappings.get(0).getOutputField());
		assertEquals(ValueMappingLanguage.JAVASCRIPT, extractMappings.get(0).getLanguage());
		assertEquals("return inputValue.trim();", extractMappings.get(0).getMapping());
	}

	@Test
	public final void testExtractMappingsMultiple() throws Exception {
		List<ValueMapping> extractMappings = ValueMapping.extractMappings(new StringReader(
				"OldField,NewField,Shown,Language,Mapping\ninputField2,outputField2,,Javascript,\"return inputValue.trim();\"\ninputField,outputField,,,\n"));

		assertEquals(2, extractMappings.size());

		assertEquals("inputField2", extractMappings.get(0).getInputField());
		assertEquals("outputField2", extractMappings.get(0).getOutputField());
		assertEquals(ValueMappingLanguage.JAVASCRIPT, extractMappings.get(0).getLanguage());
		assertEquals("return inputValue.trim();", extractMappings.get(0).getMapping());

		assertEquals("inputField", extractMappings.get(1).getInputField());
		assertEquals("outputField", extractMappings.get(1).getOutputField());
		assertEquals(ValueMappingLanguage.DEFAULT, extractMappings.get(1).getLanguage());
		assertEquals(ValueMapping.DEFAULT_MAPPING, extractMappings.get(1).getMapping());
	}

	@Test
	public final void testMapLine() {
		List<String> mapLine = ValueMapping.mapLine(Arrays.asList("anInput", "anInput3", "aDifferentInput"),
				Arrays.asList("testValue1", "testValue2", "xyzabc"),
				Arrays.asList(testDefaultMapping, testDefaultMapping3, testJavascriptMapping));

		assertEquals(3, mapLine.size());
		assertEquals("testValue1", mapLine.get(0));
		assertEquals("testValue2", mapLine.get(1));
		assertEquals("x", mapLine.get(2));
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
		assertEquals("return inputValue.substring(0, 1);", testJavascriptMapping.getMapping());
	}

	@Test
	public final void testGetOutputField() {
		assertEquals("anotherField", testDefaultMapping.getOutputField());
		assertEquals("anotherField", testDefaultMapping2.getOutputField());
		assertEquals("aDifferentField", testJavascriptMapping.getOutputField());
	}
}
