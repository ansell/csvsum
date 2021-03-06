# Summariser and Mapper for CSV and JSON files

CSV summariser to quickly find out what you need to know about that random CSV file you were given, and a mapper to make the CSV file fit with what you need. It also provides a mapper and summariser for JSON files.

[![Build Status](https://travis-ci.org/ansell/csvsum.svg?branch=master)](https://travis-ci.org/ansell/csvsum) [![Coverage Status](https://coveralls.io/repos/ansell/csvsum/badge.svg?branch=master)](https://coveralls.io/r/ansell/csvsum?branch=master)

# Setup

Install Maven and Git

Download the Git repository.

Set the relevant programs to be executable.

    chmod a+x ./csvsum
    chmod a+x ./csvmap
    chmod a+x ./csvjoin
    chmod a+x ./accessmap
    chmod a+x ./csvupload

# CSV Summariser


## Usage

Run csvsum with --help to get usage details:

    ./csvsum --help

Run csvsum with a sample csv file:

    ./csvsum --input src/test/resources/com/github/ansell/csvsum/test-single-header-one-line.csv

# CSV Mapper

## Usage

Run csvmap with --help to get usage details:

    ./csvmap --help

Run csvmap with a sample csv file:

    ./csvmap --input src/test/resources/com/github/ansell/csvmap/test-source.csv --mapping src/test/resources/com/github/ansell/csvmap/test-mappings.csv

The Language field in the mapping CSV file is one of: Javascript, Groovy, Lua.

The Mapping field in the mapping CSV file is either empty or a script in the Language.

If the Mapping field is empty, or is "inputValue", then the interpreters are not run for this field, and the values from the OldField are directly sent to NewField.

If the Mapping field contains a script, it is executed in the context of the following named parameters:

* inputHeaders : The headers from the input file as a list, in the order they were in the original file.
* inputField : The field that was designated to be the source for this mapping. Note that this parameter doesn't need to be used, and arbitrary fields can be merged using inputheaders and line.
* inputValue : The value of the field from inputField.
* outputHeaders : The headers used in the output file
* outputField : The label of the output field that the results of this row will be assigned to.
* line : The values from the current line being processed from the input file, in the same order as the inputHeaders list.
* mapLine : The mapped values for the current line so far
* previousLine : The values from the previous line, or an empty list i
f this is the first line or the lines are all empty
* previousMappedLine : The mapped values from the previous line, or an empty list if the previous line was filtered or the output document is empty
* primaryKeys : An optionally useful JDefaultDict Set of String's that can be used to deduplicate collections. If not explicitly used, it will not grow in size and hence large collections can still stream with minimal memory requirements.
* mapLineConsumer : A function that can be called with a List<String> that contains an intermediate result set. Note that the result set must be the same size as all of the other lines or the CSV file will be corrupted. This is only for advanced use when it is much simpler to do this than to have multiple mapping files for a single input file.

Javascript mappings have access to some global functions included from Java:

* Integer : The java.lang.Integer class
* Double : The java.lang.Double class
* Long : The java.lang.Long class
* Format : The java.time.format.DateTimeFormatter class
* DateTimeFormatterBuilder : The java.time.format.DateTimeFormatterBuilder class
* LocalDate : The java.time.LocalDate class
* LocalTime : The java.time.LocalTime class
* LocalDateTime : The java.time.LocalDateTime class
* ChronoUnit : The java.time.temporal.ChronoUnit class
* Math : The java.lang.Math class
* String : The java.lang.String class
* MessageDigest : The java.security.MessageDigest class
* BigInteger : The java.math.BigInteger class
* Arrays : The java.util.Arrays class
* WGS84 : The com.github.ansell.shp.WGS84 class from https://github.com/ansell/utm2wgs
* UTM : The com.github.ansell.shp.UTM class from https://github.com/ansell/utm2wgs

Other Java classes can be accessed by assigning their type to a global variable using the syntax:

    var ClassName = Java.type("org.com.JavaClassName");

Javascript mappings also have access to helper functions to simplify common mappings:

* col : Called using the syntax col('columnName'), and returns the value for that column on the current line
* outCol : Called using the syntax outCol('outputColumnName'), and returns the value that has been mapped so far for that column on the current line.
* filter : Called using the syntax filter(), and will make the line not appear in the results and short-circuit processing of the line for mapping purposes
* newDateFormat : Called using the syntax newDateFormat(formatPattern), where formatPattern is a String containing a DateTimeFormatter pattern such as dd-MM-yyyy
* dateMatches : Called using the syntax dateMatches(inputValue, dateFormat), where dateFormat is a DateTimeFormatter instance, such as Format.ISO_LOCAL_DATE
* dateConvert : Called using the syntax dateConvert(inputValue, inputFormat, outputFormat), where inputFormat and outputFormat are instances of DateTimeFormatter, such as Format.ISO_LOCAL_DATE. This function also accepts an optional fourth parameter specifying the class to use, which can be any of LocalDate, LocalTime, LocalDateTime, or other similar functions that support the parse method. The default is to use LocalDate.
* primaryKeyFilter : Called using the syntax primaryKeyFilter(inputValue) or primaryKeyFilter(inputValue, primaryKeyField), where inputValue is the primary key string and the optional primaryKeyField defaults to "Primary". The function returns the string if and only if it is unique so far, otherwise it calls filter() to filter out the line.
* digest : Called using the syntax digest(value, algorithm), where algorithm defaults to 'SHA-256' if it is not specified.

Javascript mappings must either return a value, call mapLineConsumer to create multiple output lines from the current line, or call filter() to ignore the current line.

# CSV Joiner

CSV Joiner inherits the functionality of CSV Mapper, so all of the functions and languages available to the CSV Mapper program are available here.

In addition, it adds another supported language, CsvJoin. Keys from the primary input are linked using a row in the mapping column that point from either 1-1 or Many-1 relationships to the other input. 1-Many relationships are not supported due to the way rows are generated based on a base file. The base file is always the first input, which has rows from other-input merged into it using either a left outer join pattern or a full outer join pattern, depending on whether true or false is specified for --left-outer-join.

## Usage

Run csvjoin with --help to get usage details:

    ./csvjoin --help

Run csvmap with a sample csv file:

    ./csvjoin --input src/test/resources/com/github/ansell/csvjoin/test-source.csv --other-input src/test/resources/com/github/ansell/csvjoin/test-source-other.csv --mapping src/test/resources/com/github/ansell/csvjoin/test-mapping.csv

# CSV to SQL Uploader

CSV to SQL Uploader uploads from a CSV file to a SQL Database, including dropping and creating the table as necessary.

It adds another supported language, DBSchema to denote the field type for each field in the table. Stub versions of the DBSchema mappings can be created using the --output-mapping flag for csvsum.

## Usage

Run csvupload with --help to get usage details:

    ./csvupload --help

Run csvmap with a sample csv file:

    ./csvupload --database "jdbc:..." --input src/test/resources/com/github/ansell/csvsum/test-single-header-one-line.csv --table test-table --mapping src/test/resources/com/github/ansell/csvupload/test-mapping-single-header-one-line.csv

# Maven

    <dependency>
        <groupId>com.github.ansell.csv.sum</groupId>
        <artifactId>csvsum</artifactId>
        <version>0.6.0</version>
    </dependency>

# Changelog

## 2018-11-09
* Move accessmap to a new repository to cleanup dependencies here

## 2018-01-19
* Release 0.6.0
* Add jsonpretty to pretty print JSON documents using a streaming converter
* Add jsonmap
* Add jsonsum
* Cleanup CSVSorter API

## 2018-01-15
* Add incrementCount and getCount functions to ValueMapping

## 2017-10-17
* Release 0.5.1
* Add support for default value interpolation to replace missing values (Used by dwca-utils which has native support for meta.xml)

## 2017-06-07
* Add support for specifying quote/line ending/field separator characters in the CSVSummariser Java API. No support yet in the CLI

## 2017-04-03
* Release 0.4.1

## 2017-03-31
* Add progress output for csvsum/csvmap/csvjoin
* Add JSON utility functions to allow programmatic iteration of JSON files

## 2017-03-30
* Add support for header substitution for files with no header line

## 2017-03-03
* Add HTTP POST JSON utility method, JSONUtil.queryJSONPost(url, postVariables, jpath)

## 2017-01-17
* Add --show-sample-counts to csvsum to annotate sample values with their counts

## 2016-09-09
* Release 0.4.0
* Add dependency on csvstream and remove/redirect code locally to it

## 2016-09-05
* Add dependency and javascript links to utm2wgs, including UTM and WGS84

## 2016-07-25
* Release 0.3.1
* Support Decimal type for csvupload

## 2016-06-21
* Add functionality to parse a CSV file and upload it to a SQL database
* Bump to 0.3.0 to take into account the new functionality and minor API changes
* Add derby and postgresql JDBC drivers as dependencies for the runtime to optionally use out of the box

## 2016-05-04
* Release 0.2.0

## 2016-05-02
* Change mapLineConsumer to take two arguments, the unmapped and the mapped lines

## 2016-04-20
* Release 0.1.0
* Add mapLineConsumer to csvmap
* Add Arrays class as an import to csvmap

## 2016-03-03
* Rename csvmerge to csvjoin to indicate its purpose, while freeing up csvmerge to be implemented separately
* Add capability to do a full outer join for csvjoin in addition to left outer join

## 2016-03-30
* Release 0.0.7
* Add primaryKeys argument to mapping function parameters to deduplicate sets where necessary
* Add primaryKeyFilter function to simplify usage of the primaryKeys set

## 2016-03-16
* Release 0.0.6
* Improve performance of csvmerge

## 2016-03-15
* Add outputHeaders, previousLine and previousMappedLine to mapping function parameters
* Add dateMatches and dateConvert functions

## 2016-03-11
* Release 0.0.5
* Add csvmerge program to merge two CSV files
* Add new column to mapping files to define which fields are shown. Those with "no" in the shown column will not be in the result file.

## 2016-03-04
* Add helper function for getting the mapped value for a column in the current line, "outCol('outputColumnName')"

## 2016-03-02
* Add ability to filter lines in Javascript using a new function, "filter()"
* Add helper function for getting the value for a column in the current line, "col('columnName')"
* Add parallel processing of Access lines in different threads to take advantage of multiple cores available
* Write lines for Access using a different thread to reduce bottlenecks in the main thread

## 2016-02-24
* Add mapper for Microsoft Access databases to CSV files
* Output dates from Access to ISO8601 instead of the Jackcess/Java default

## 2016-02-17
* Release 0.0.4

## 2016-02-16
* Refactor package names to separate them
* Add requirement for javascript and lua mappings to include the return keyword in the mapping

## 2016-02-11
* Release 0.0.3
* Add CSVMapper to map CSV files to other CSV files using a given map
* Support Javascript (Nashorn), Groovy and Lua as scripting languages for the mapping

## 2016-02-10
* Add possiblePrimaryKey to output based on if there are unique values found for each row for a field
* Add possibleInteger and possibleFloatingPoint checks for fields which have values and that look like integers or floating point values
* Add --samples=-1 to emit all values
* Improve memory efficiency for large sample value dumps using shared StringBuilder and lamda

## 2016-02-08

* Release 0.0.2
* Add --output to output to a CSV file
* Output summary to CSV file
* Add --samples to specify the number of sample values to include for each field
