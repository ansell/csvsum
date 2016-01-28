/**
 * 
 */
package com.github.ansell.csvsum;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Summarises CSV files to easily debug and identify likely parse issues before
 * pushing them through a more heavy tool or process.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVSummariser {

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be summarised.");

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
			throw new FileNotFoundException("Could not find input CSV file: " + inputPath.toString());
		}

		JDefaultDict<String, AtomicInteger> emptyCounts = new JDefaultDict<String, AtomicInteger>(
				k -> new AtomicInteger());
		JDefaultDict<String, AtomicInteger> nonEmptyCounts = new JDefaultDict<String, AtomicInteger>(
				k -> new AtomicInteger());

		List<String> headers = new ArrayList<String>();

		CSVUtil.streamCSV(Files.newBufferedReader(inputPath), h -> headers.addAll(h), (h, l) -> {
			for (int i = 0; i < h.size(); i++) {
				if (l.get(i).trim().isEmpty()) {
					emptyCounts.get(h.get(i)).incrementAndGet();
				} else {
					nonEmptyCounts.get(h.get(i)).incrementAndGet();
				}
			}
			return l;
		} , l -> {
		});

		headers.forEach(h -> System.out
				.println(h + " : s\tempty=\t" + emptyCounts.get(h).get() + " \tnon-empty=\t" + nonEmptyCounts.get(h).get()));
	}

}
