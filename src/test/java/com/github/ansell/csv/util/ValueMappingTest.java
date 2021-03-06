package com.github.ansell.csv.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;
import com.github.ansell.jdefaultdict.JDefaultDict;

public class ValueMappingTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ValueMapping testDefaultMapping;
    private ValueMapping testDefaultMapping2;
    private ValueMapping testDefaultMapping3;
    private ValueMapping testDefaultMapping4;
    private ValueMapping testJavascriptFilteredLineNumber;
    private ValueMapping testJavascriptLineNumber;
    private ValueMapping testJavascriptMapping;
    private ValueMapping testJavascriptPrimaryKeyMapping;
    private ValueMapping testJavascriptPrimaryKeyMappingFunction;
    private ValueMapping testJavascriptValueCountIncrement;
    private ValueMapping testJavascriptValueCountGet;
    private ValueMapping testJavascriptReplaceLineEndings;
    private ValueMapping testJavascriptMapLineConsumerAndReturn;
    private ValueMapping testJavascriptMapLineConsumerFilter;
    private ValueMapping testPreviousMapping;
    private ValueMapping testDateMatching;
    private ValueMapping testDateMapping;
    private JDefaultDict<String, Set<String>> testPrimaryKeys;
    private JDefaultDict<String, JDefaultDict<String, AtomicInteger>> testValueCounts;

    private static final BiConsumer<List<String>, List<String>> UNEXPECTED_LINE_CONSUMER = (l,
            m) -> {
        fail("Not expecting mapLineConsumer to be called in this test.");
    };

    @Before
    public void setUp() throws Exception {
        testDefaultMapping = ValueMapping.newMapping("Default", "anInput", "anotherField", "", "",
                "");
        testDefaultMapping2 = ValueMapping.newMapping("Default", "anInput", "anotherField",
                "inputValue", "", "");
        testDefaultMapping3 = ValueMapping.newMapping("Default", "anInput3", "anotherField3",
                "inputValue", "", "");
        testDefaultMapping4 = ValueMapping.newMapping("Default", "anInput3", "anotherFieldNotShown",
                "inputValue", "no", "");
        testJavascriptMapping = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField", "return inputValue.substring(0, 1);", "", "");
        testPreviousMapping = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField2",
                "return previousLine.isEmpty() ? 'no-previous' : previousLine.get(outputHeaders.indexOf(outputField));",
                "", "");
        testDateMatching = ValueMapping.newMapping("Javascript", "dateInput", "usefulDate",
                "return dateMatches(inputValue, Format.ISO_LOCAL_DATE) ? inputValue : 'fix-your-date-format';",
                "", "");
        testDateMapping = ValueMapping.newMapping("Javascript", "dateInput", "usefulDate",
                "return dateMatches(inputValue, Format.ISO_LOCAL_DATE) ? dateConvert(inputValue, Format.ISO_LOCAL_DATE, Format.ISO_WEEK_DATE) : 'fix-your-date-format';",
                "", "");
        testJavascriptPrimaryKeyMapping = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField",
                "return !primaryKeys.get(\"Primary\").add(inputValue) ? filter() : inputValue;", "",
                "");
        testJavascriptPrimaryKeyMappingFunction = ValueMapping.newMapping("Javascript",
                "aDifferentInput", "aDifferentField", "return primaryKeyFilter(inputValue);", "",
                "");
        testJavascriptValueCountIncrement = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField",
                "return Integer.toString(incrementCount(inputField, inputValue));", "", "");
        testJavascriptValueCountGet = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField", "return Integer.toString(getCount(inputField, inputValue));", "",
                "");
        testJavascriptLineNumber = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField", "return Integer.toString(lineNumber);", "", "");
        testJavascriptFilteredLineNumber = ValueMapping.newMapping("Javascript", "aDifferentInput",
                "aDifferentField",
                "return lineNumber % 2 != 0 ? Integer.toString(filteredLineNumber) : filter();", "",
                "");
        testJavascriptMapLineConsumerAndReturn = ValueMapping.newMapping("Javascript",
                "aDifferentInput", "aDifferentField",
                "mapLineConsumer(line, Arrays.asList(inputValue, \"ABC-\" + inputValue)); return inputValue;",
                "", "");
        testJavascriptMapLineConsumerFilter = ValueMapping.newMapping("Javascript",
                "aDifferentInput", "aDifferentField",
                "mapLineConsumer(line, Arrays.asList(inputValue, \"ABC-\" + inputValue)); filter(); return inputValue;",
                "", "");
        testJavascriptReplaceLineEndings = ValueMapping.newMapping("Javascript", "anInput",
                "aDifferentField", "return replaceLineEndingsWith(inputValue, \"\");", "", "");

        testPrimaryKeys = new JDefaultDict<>(k -> new HashSet<>());
        testValueCounts = new JDefaultDict<>(k -> new JDefaultDict<>(v -> new AtomicInteger(0)));
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
        final List<ValueMapping> extractMappings = ValueMapping.extractMappings(new StringReader(
                "OldField,NewField,Shown,Language,Mapping\ninputField,outputField,,,\n"));

        assertEquals(1, extractMappings.size());
        assertEquals("inputField", extractMappings.get(0).getInputField());
        assertEquals("outputField", extractMappings.get(0).getOutputField());
        assertEquals(ValueMappingLanguage.DEFAULT, extractMappings.get(0).getLanguage());
        assertEquals(ValueMapping.DEFAULT_MAPPING, extractMappings.get(0).getMapping());
    }

    @Test
    public final void testExtractMappingsJavascript() throws Exception {
        final List<ValueMapping> extractMappings = ValueMapping.extractMappings(new StringReader(
                "OldField,NewField,Shown,Language,Mapping\ninputField2,outputField2,,Javascript,\"return inputValue.trim();\"\n"));

        assertEquals(1, extractMappings.size());
        assertEquals("inputField2", extractMappings.get(0).getInputField());
        assertEquals("outputField2", extractMappings.get(0).getOutputField());
        assertEquals(ValueMappingLanguage.JAVASCRIPT, extractMappings.get(0).getLanguage());
        assertEquals("return inputValue.trim();", extractMappings.get(0).getMapping());
    }

    @Test
    public final void testExtractMappingsMultiple() throws Exception {
        final List<ValueMapping> extractMappings = ValueMapping.extractMappings(new StringReader(
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
        final List<String> mapLine = ValueMapping.mapLine(
                Arrays.asList("anInput", "anInput3", "aDifferentInput", "anInput4"),
                Arrays.asList("testValue1", "testValue2", "xyzabc", "defghi"),
                Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testDefaultMapping, testDefaultMapping3, testDefaultMapping4,
                        testJavascriptMapping, testPreviousMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(4, mapLine.size());
        assertEquals("testValue1", mapLine.get(0));
        assertEquals("testValue2", mapLine.get(1));
        assertEquals("x", mapLine.get(2));
        assertEquals("no-previous", mapLine.get(3));
    }

    @Test
    public final void testMapLineWithPrevious() {
        final List<String> mapLine = ValueMapping.mapLine(
                Arrays.asList("anInput", "anInput3", "aDifferentInput", "anInput4"),
                Arrays.asList("testValue1A", "testValue2A", "xyzabcdefg", "defghijkl"),
                Arrays.asList("testValue1", "testValue2", "xyzabc", "defghi"),
                Arrays.asList("testValue1", "testValue2", "x", "no-previous"),
                Arrays.asList(testDefaultMapping, testDefaultMapping3, testDefaultMapping4,
                        testJavascriptMapping, testPreviousMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(4, mapLine.size());
        assertEquals("testValue1A", mapLine.get(0));
        assertEquals("testValue2A", mapLine.get(1));
        assertEquals("x", mapLine.get(2));
        assertEquals("defghi", mapLine.get(3));
    }

    @Test
    public final void testMapLinePrimaryKey() {
        final List<String> mapLine1 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMapping, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine1.size());
        assertEquals("testKey1", mapLine1.get(0));
        assertEquals("testValue1", mapLine1.get(1));

        final List<String> mapLine2 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue2"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMapping, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine2.size());
        assertEquals("testKey2", mapLine2.get(0));
        assertEquals("testValue2", mapLine2.get(1));

        // Map a duplicate now and verify that it is filtered
        thrown.expect(LineFilteredException.class);
        ValueMapping.mapLine(Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue3"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMapping, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);
    }

    @Test
    public final void testMapLineValueCount() {
        final List<String> mapLine1 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptValueCountGet, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine1.size());
        assertEquals("0", mapLine1.get(0));
        assertEquals("testValue1", mapLine1.get(1));

        final List<String> mapLine2 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue2"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptValueCountIncrement, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine2.size());
        assertEquals("1", mapLine2.get(0));
        assertEquals("testValue2", mapLine2.get(1));

        final List<String> mapLine3 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue3"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptValueCountGet, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine3.size());
        assertEquals("1", mapLine3.get(0));
        assertEquals("testValue3", mapLine3.get(1));

        final List<String> mapLine4 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue4"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptValueCountGet, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine4.size());
        assertEquals("1", mapLine4.get(0));
        assertEquals("testValue4", mapLine4.get(1));
    }

    @Test
    public final void testMapLinePrimaryKeyFunction() {
        final List<String> mapLine1 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMappingFunction, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine1.size());
        assertEquals("testKey1", mapLine1.get(0));
        assertEquals("testValue1", mapLine1.get(1));

        final List<String> mapLine2 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue2"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMappingFunction, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine2.size());
        assertEquals("testKey2", mapLine2.get(0));
        assertEquals("testValue2", mapLine2.get(1));

        // Map a duplicate now and verify that it is filtered
        thrown.expect(LineFilteredException.class);
        ValueMapping.mapLine(Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue3"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptPrimaryKeyMappingFunction, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 1, 1, UNEXPECTED_LINE_CONSUMER);
    }

    @Test
    public final void testMapLineLineNumber() {
        final List<String> mapLine1 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptLineNumber, testDefaultMapping), testPrimaryKeys,
                testValueCounts, 123, 101, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine1.size());
        assertEquals("123", mapLine1.get(0));
        assertEquals("testValue1", mapLine1.get(1));

        final List<String> mapLine2 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue2"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptFilteredLineNumber, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 123, 101, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine2.size());
        assertEquals("101", mapLine2.get(0));
        assertEquals("testValue2", mapLine2.get(1));

        // Map an even line number now and verify that it is filtered
        thrown.expect(LineFilteredException.class);
        ValueMapping.mapLine(Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey2", "testValue3"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptFilteredLineNumber, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 124, 101, UNEXPECTED_LINE_CONSUMER);
    }

    @Test
    public final void testMapLineConsumerAndReturn() {
        final List<List<String>> results = new ArrayList<>();
        final BiConsumer<List<String>, List<String>> mapLineConsumer = (l, m) -> {
            results.add(m);
        };
        final List<String> mapLine1 = ValueMapping.mapLine(
                Arrays.asList("aDifferentInput", "anInput"),
                Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(testJavascriptMapLineConsumerAndReturn, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 123, 101, mapLineConsumer);

        assertEquals(2, mapLine1.size());
        assertEquals("testKey1", mapLine1.get(0));
        assertEquals("testValue1", mapLine1.get(1));
        assertEquals(1, results.size());
        assertEquals("testKey1", results.get(0).get(0));
        assertEquals("ABC-testKey1", results.get(0).get(1));
    }

    @Test
    public final void testMapLineConsumerFilter() {
        final List<List<String>> results = new ArrayList<>();
        final BiConsumer<List<String>, List<String>> mapLineConsumer = (l, m) -> {
            results.add(m);
        };

        try {
            ValueMapping.mapLine(Arrays.asList("aDifferentInput", "anInput"),
                    Arrays.asList("testKey1", "testValue1"), Collections.emptyList(),
                    Collections.emptyList(),
                    Arrays.asList(testJavascriptMapLineConsumerFilter, testDefaultMapping),
                    testPrimaryKeys, testValueCounts, 123, 101, mapLineConsumer);
            fail("Did not receive LineFilteredException");
        } catch (final LineFilteredException e) {
            // Expected exception
        }
        assertEquals(1, results.size());
        assertEquals("testKey1", results.get(0).get(0));
        assertEquals("ABC-testKey1", results.get(0).get(1));
    }

    @Test
    public final void testReplaceLineEndings() {
        final List<String> mapLine = ValueMapping.mapLine(Arrays.asList("anInput"),
                Arrays.asList(
                        "This is what \ryou get \nwhen you \r\nreplace new lines\n correctly\n!"),
                Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testJavascriptReplaceLineEndings, testDefaultMapping),
                testPrimaryKeys, testValueCounts, 123, 101, UNEXPECTED_LINE_CONSUMER);

        assertEquals(2, mapLine.size());
        // Test with and without the mapping to verify it can be roundtripped
        // with the newlines in the same result set
        assertEquals("This is what you get when you replace new lines correctly!", mapLine.get(0));
        assertEquals("This is what \ryou get \nwhen you \r\nreplace new lines\n correctly\n!",
                mapLine.get(1));
    }

    @Test
    public final void testMapLineDateMatchInvalid() {
        final List<String> mapLine = ValueMapping.mapLine(Arrays.asList("dateInput"),
                Arrays.asList("testNotADate"), Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testDateMatching), testPrimaryKeys, testValueCounts, 1, 1,
                UNEXPECTED_LINE_CONSUMER);

        assertEquals(1, mapLine.size());
        assertEquals("fix-your-date-format", mapLine.get(0));
    }

    @Test
    public final void testMapLineDateMatchValid() {
        final List<String> mapLine = ValueMapping.mapLine(Arrays.asList("dateInput"),
                Arrays.asList("2013-01-30"), Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testDateMatching), testPrimaryKeys, testValueCounts, 1, 1,
                UNEXPECTED_LINE_CONSUMER);

        assertEquals(1, mapLine.size());
        assertEquals("2013-01-30", mapLine.get(0));
    }

    @Test
    public final void testMapLineDateMatchInvalidConvert() {
        final List<String> mapLine = ValueMapping.mapLine(Arrays.asList("dateInput"),
                Arrays.asList("testNotADate"), Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testDateMapping), testPrimaryKeys, testValueCounts, 1, 1,
                UNEXPECTED_LINE_CONSUMER);

        assertEquals(1, mapLine.size());
        assertEquals("fix-your-date-format", mapLine.get(0));
    }

    @Test
    public final void testMapLineDateMatchValidConvert() {
        final List<String> mapLine = ValueMapping.mapLine(Arrays.asList("dateInput"),
                Arrays.asList("2013-01-30"), Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(testDateMapping), testPrimaryKeys, testValueCounts, 1, 1,
                UNEXPECTED_LINE_CONSUMER);

        assertEquals(1, mapLine.size());
        assertEquals("2013-W05-3", mapLine.get(0));
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

    @Test
    public final void testDateFormatter() {
        final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("H[H][:][m][m]");

        LocalTime.parse("7", timeFormatter);
        LocalTime.parse("07:19", timeFormatter);
        LocalTime.parse("07", timeFormatter);
        LocalTime.parse("07:0", timeFormatter);
        LocalTime.parse("07:1", timeFormatter);
        LocalTime.parse("15:5", timeFormatter);
        LocalTime.parse("15:50", timeFormatter);
        // The following requires HHmm and doesn't work with the pattern above
        // LocalTime.parse("1300", timeFormatter);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/M[M]/yyyy");

        LocalDate.parse("17/2/2016", dateFormatter);

        final DateTimeFormatter dateFormatter2 = DateTimeFormatter.ofPattern("d[d] MMM yyyy")
                .withLocale(Locale.US);

        // The following never seems to work with a four digit year, with and
        // without brackets for optionals
        // LocalDate.parse("3 Sep 07", dateFormatter2);
        LocalDate.parse("3 Sep 2007", dateFormatter2);

        final DateTimeFormatter dateFormatter3 = DateTimeFormatter.ofPattern("dd-MMM-yy")
                .withLocale(Locale.US);

        LocalDate.parse("07-Oct-76", dateFormatter3);

        final DateTimeFormatter dateFormatter4 = new DateTimeFormatterBuilder()
                .parseCaseInsensitive().appendPattern("d/M/yy K'.'mma").toFormatter(Locale.US);
        final LocalDateTime parsedDate4 = LocalDateTime.parse("28/5/17 1.10pm", dateFormatter4);

        assertEquals(28, parsedDate4.getDayOfMonth());
        assertEquals(Month.MAY, parsedDate4.getMonth());
        assertEquals(2017, parsedDate4.getYear());
        assertEquals(13, parsedDate4.getHour());
        assertEquals(10, parsedDate4.getMinute());
    }
}
