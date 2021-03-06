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
package com.github.ansell.csv.map;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.JSONStream;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMappingContext;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Maps from a JSON file to a CSV file based on the supplied mapping
 * definitions.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONMapper {

    /**
     * Private constructor for static only class
     */
    private JSONMapper() {
    }

    public static void main(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class)
                .required().describedAs("The input JSON file to be mapped.");
        final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg()
                .ofType(File.class).required().describedAs("The mapping file.");
        final OptionSpec<File> output = parser.accepts("output").withRequiredArg()
                .ofType(File.class)
                .describedAs("The mapped CSV file, or the console if not specified.");
        final OptionSpec<String> basePathOption = parser.accepts("base-path").withRequiredArg()
                .ofType(String.class).required().describedAs(
                        "The base path in the JSON document to locate the array of objects to be summarised");
        final OptionSpec<Boolean> appendToExistingOption = parser.accepts("append-to-existing")
                .withRequiredArg().ofType(Boolean.class).describedAs("Append to an existing file")
                .defaultsTo(false);

        OptionSet options = null;

        try {
            options = parser.parse(args);
        } catch (final OptionException e) {
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            throw e;
        }

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            return;
        }

        final Path inputPath = input.value(options).toPath();
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException(
                    "Could not find input JSON file: " + inputPath.toString());
        }

        final Path mappingPath = mapping.value(options).toPath();
        if (!Files.exists(mappingPath)) {
            throw new FileNotFoundException(
                    "Could not find mapping CSV file: " + mappingPath.toString());
        }

        final JsonPointer basePath = JsonPointer.compile(basePathOption.value(options));

        final ObjectMapper jsonMapper = new ObjectMapper();

        // Double up for now on the append option, as we always want to write
        // headers,
        // except when we are appending to an existing file, in which case we
        // check that
        // the headers already exist
        final boolean writeHeaders = !appendToExistingOption.value(options);

        final OpenOption[] writeOptions = new OpenOption[1];
        // Append if needed, otherwise verify that the file is created from
        // scratch
        writeOptions[0] = writeHeaders ? StandardOpenOption.CREATE_NEW : StandardOpenOption.APPEND;

        try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
                final BufferedReader readerInput = Files.newBufferedReader(inputPath);) {
            final List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
            final List<String> outputHeaders = ValueMapping.getOutputFieldsFromList(map);

            Writer writer = null;
            try {
                if (options.has(output)) {
                    // If we aren't planning on writing headers, we parse just
                    // the header line
                    if (!writeHeaders) {
                        CSVStream.parse(
                                Files.newBufferedReader(output.value(options).toPath(),
                                        StandardCharsets.UTF_8),
                                h -> {
                                    // Headers must match exactly with those we
                                    // are planning to write out
                                    if (!outputHeaders.equals(h)) {
                                        throw new IllegalArgumentException(
                                                "Could not append to file as its existing headers did not match: existing=["
                                                        + h + "] new=[" + outputHeaders + "]");
                                    }
                                }, (h, l) -> l, l -> {
                                });
                    }

                    writer = Files.newBufferedWriter(output.value(options).toPath(),
                            StandardCharsets.UTF_8, writeOptions);
                } else {
                    writer = new BufferedWriter(
                            new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
                }

                runMapper(readerInput, map, writer, basePath, jsonMapper, writeHeaders);
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    public static void runMapper(Reader input, List<ValueMapping> map, Writer output,
            JsonPointer basePath, ObjectMapper jsonMapper, boolean writeHeaders)
            throws ScriptException, IOException {

        final List<String> inputHeaders = ValueMapping.getInputFieldsFromList(map);
        final List<String> outputHeaders = ValueMapping.getOutputFieldsFromList(map);
        final Map<String, String> defaultValues = ValueMapping.getDefaultValuesFromList(map);
        final Map<String, Optional<JsonPointer>> fieldRelativePaths = map.stream()
                .collect(Collectors.toMap(ValueMapping::getOutputField,
                        nextMapping -> nextMapping.getInputField().trim().isEmpty()
                                ? Optional.empty()
                                : Optional.of(JsonPointer.compile(nextMapping.getInputField()))));
        final CsvSchema schema = CSVStream.buildSchema(outputHeaders, writeHeaders);
        final Writer writer = output;

        try (final SequenceWriter csvWriter = CSVStream.newCSVWriter(writer, schema);) {
            final List<String> previousLine = new ArrayList<>();
            final List<String> previousMappedLine = new ArrayList<>();
            final JDefaultDict<String, Set<String>> primaryKeys = new JDefaultDict<>(
                    k -> new HashSet<>());
            final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts = new JDefaultDict<>(
                    k -> new JDefaultDict<>(v -> new AtomicInteger(0)));
            final AtomicInteger lineNumber = new AtomicInteger(0);
            final AtomicInteger filteredLineNumber = new AtomicInteger(0);
            final long startTime = System.currentTimeMillis();
            final BiConsumer<List<String>, List<String>> mapLineConsumer = Unchecked
                    .biConsumer((l, m) -> {
                        previousLine.clear();
                        previousLine.addAll(l);
                        previousMappedLine.clear();
                        previousMappedLine.addAll(m);
                        csvWriter.write(m);
                    });
            JSONStream.parse(input, h -> {
            }, (node, headers, line) -> {
                final int nextLineNumber = lineNumber.incrementAndGet();
                if (nextLineNumber % 1000 == 0) {
                    final double secondsSinceStart = (System.currentTimeMillis() - startTime)
                            / 1000.0d;
                    System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n",
                            nextLineNumber, secondsSinceStart, nextLineNumber / secondsSinceStart);
                }
                final int nextFilteredLineNumber = filteredLineNumber.incrementAndGet();
                try {
                    final List<String> mapLine = ValueMapping.mapLine(new ValueMappingContext(
                            inputHeaders, line, previousLine, previousMappedLine, map, primaryKeys,
                            valueCounts, nextLineNumber, nextFilteredLineNumber, mapLineConsumer,
                            outputHeaders, defaultValues, Optional.of(node)));
                    mapLineConsumer.accept(line, mapLine);
                } catch (final LineFilteredException e) {
                    // Swallow line filtered exception and return null below to
                    // eliminate it
                    // We expect streamCSV to operate in sequential order, print
                    // a warning if it doesn't
                    final boolean success = filteredLineNumber.compareAndSet(nextFilteredLineNumber,
                            nextFilteredLineNumber - 1);
                    if (!success) {
                        System.out.println("Line numbers may not be consistent");
                    }
                }
                return null;
            }, l -> {
            }, basePath, fieldRelativePaths, defaultValues, jsonMapper, outputHeaders);
        }
    }

}
