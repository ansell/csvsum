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
package com.github.ansell.csv.join;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.ValueMapping;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Merges lines from one CSV file into another based on the supplied mapping
 * definitions.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVJoiner {

    /**
     * Private constructor for static only class
     */
    private CSVJoiner() {
    }

    public static void main(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class)
                .required().describedAs("The input CSV file to be mapped.");
        final OptionSpec<String> inputPrefix = parser.accepts("input-prefix").withRequiredArg()
                .ofType(String.class).defaultsTo("")
                .describedAs("A prefix to be used for the input file.");
        final OptionSpec<File> otherInput = parser.accepts("other-input").withRequiredArg()
                .ofType(File.class).required()
                .describedAs("The other input CSV file to be merged.");
        final OptionSpec<String> otherPrefix = parser.accepts("other-prefix").withRequiredArg()
                .ofType(String.class).defaultsTo("")
                .describedAs("A prefix to be used for the other file.");
        final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg()
                .ofType(File.class).required().describedAs("The mapping file.");
        final OptionSpec<File> output = parser.accepts("output").withRequiredArg()
                .ofType(File.class)
                .describedAs("The mapped CSV file, or the console if not specified.");
        final OptionSpec<Boolean> leftOuterJoin = parser.accepts("left-outer-join")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.TRUE)
                .describedAs("True to use left outer join and false to use a full outer join");

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
                    "Could not find input CSV file: " + inputPath.toString());
        }

        final Path otherInputPath = otherInput.value(options).toPath();
        if (!Files.exists(otherInputPath)) {
            throw new FileNotFoundException(
                    "Could not find other input CSV file: " + otherInputPath.toString());
        }

        final Path mappingPath = mapping.value(options).toPath();
        if (!Files.exists(mappingPath)) {
            throw new FileNotFoundException(
                    "Could not find mappng CSV file: " + mappingPath.toString());
        }

        final Writer writer;
        if (options.has(output)) {
            writer = Files.newBufferedWriter(output.value(options).toPath(),
                    StandardCharsets.UTF_8);
        } else {
            writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        }

        try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
                final BufferedReader readerInput = Files.newBufferedReader(inputPath);
                final BufferedReader readerOtherInput = Files.newBufferedReader(otherInputPath);) {
            final List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
            CSVUtil.runJoiner(readerInput, readerOtherInput, map, writer,
                    inputPrefix.value(options), otherPrefix.value(options),
                    leftOuterJoin.value(options));
        } finally {
            writer.close();
        }
    }
}
