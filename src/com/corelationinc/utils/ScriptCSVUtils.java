package com.corelationinc.utils;

import com.corelationinc.script.ScriptException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author stosti
 */
public class ScriptCSVUtils {

    /**
     * Container class for CSV information. Once CSVResultSet has been generated
     * rows can be iterated using the hasNext() and next() functions. The
     * current row's columns can be iterated with the getString()
     */
    public static class CSVResultSet {

        Iterator<Row> setIterator = null;
        Row currentRow = null;

        private final List<Row> rows;
        private final List<String> headers;

        private CSVResultSet() {
            rows = new ArrayList<>();
            headers = new ArrayList<>();
        }

        private CSVResultSet(List<String> headers) {
            rows = new ArrayList<>();
            this.headers = headers;
        }

        /**
         * Returns headers as a list of strings, ordered from left to right.
         *
         * @return
         */
        public List<String> getHeaders() {
            return headers;
        }

        /**
         * Determines if any rows remain in the set.
         *
         * @return
         */
        public boolean hasNext() {
            if (setIterator == null) {
                setIterator = rows.iterator();
            }
            return setIterator.hasNext();
        }

        /**
         * Advances internal row counter. Returns true if another row exists.
         *
         * @return
         */
        public boolean next() {
            if (setIterator == null) {
                setIterator = rows.iterator();
            }
            try {
                currentRow = setIterator.next();
            } catch (NoSuchElementException e) {
                return false;
            }
            return true;
        }

        /**
         * Returns the number of columns in the current row.
         *
         * @return @throws ScriptException
         */
        public int currentRowSize() throws ScriptException {
            if (currentRow == null) {
                throw new ScriptException("Attempted to fetch uninitialized row.");
            }
            return currentRow.size();
        }

        /**
         * Returns the current column value as a String. Advances an internal
         * counter
         *
         * @param index
         * @return
         * @throws ScriptException
         */
        public String getString(int index) throws ScriptException {
            if (currentRow == null) {
                throw new ScriptException("Attempted to fetch uninitialized row.");
            }
            return currentRow.getString(index);
        }

        /**
         * Returns the contents of the current row, from left to right.
         *
         * @return @throws ScriptException
         */
        public List<String> getCurrentRow() throws ScriptException {
            if (currentRow == null) {
                throw new ScriptException("Attempted to fetch uninitialized row.");
            }
            return currentRow.elements;
        }

        private void addRow(Row row) {
            rows.add(row);
        }

        private static class Row {

            List<String> elements = null;
            Iterator<String> rowIterator = null;

            public Row(List<String> elements) {
                this.elements = elements;
            }

            public boolean hasNext() {
                if (rowIterator == null) {
                    rowIterator = elements.iterator();
                }
                return rowIterator.hasNext();
            }

            public String next() {
                if (hasNext()) {
                    return rowIterator.next();
                }
                return null;
            }

            public int size() {
                return elements.size();
            }

            public String getString(int index) throws ScriptException {
                if (index >= elements.size() || index < 0) {
                    throw new ScriptException("Attempted to fetch an index which is outside of valid range." + index + "   " + elements.size());
                }
                return elements.get(index);
            }
        }
    }

    /**
     * Enumeration representation of supported 'CSV' delimiters
     */
    public static enum DELIMITER {

        /**
         * Comma
         */
        COMMA,
        /**
         * Pipe
         */
        PIPE,
        /**
         * Tab
         */
        TAB,
        /**
         * Colon
         */
        COLON

    }

    /**
     * Returns the regular expression to be used against the file for different
     * delimiters.
     *
     * @param delimiter - the delimiter to be used.
     * @return - the regular expression as a string.
     */
    private static String getRegexExpression(DELIMITER delimiter) {
        switch (delimiter) {
            case COMMA:
                return ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            case PIPE:
                return "\\|";
            case TAB:
                return "\t(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            case COLON:
                return ":(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            default:
                return ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        }
    }

    /**
     * Reads in a file at the given location and parses it as a CSV file using
     * specified delimiter. The header row toggle determines if the first row of
     * the file will be interpreted as a header set or data.
     *
     * @param filePath
     * @param delimiter
     * @param hasHeaderRow
     * @return
     * @throws ScriptException
     */
    public static CSVResultSet parse(String filePath, DELIMITER delimiter, boolean hasHeaderRow) throws ScriptException {
        String regex = getRegexExpression(delimiter);  // regex expression to split on delimiters.
        try (BufferedReader bfr = new BufferedReader(new FileReader(filePath))) {
            if (bfr == null) {
                throw new ScriptException("Bad file name: " + filePath);
            }

            String line = bfr.readLine();
            CSVResultSet rset = new CSVResultSet();
            if (hasHeaderRow) {
                String[] tokens = line.split(regex, -2);
                rset = new CSVResultSet(Arrays.asList(tokens));
                line = bfr.readLine();
            }

            while (line != null) {
                if (line.length() <= 0 || line.trim().isEmpty()) {
                    line = bfr.readLine();
                    continue;
                }
                List<String> tokens = parseString(regex, line);
                CSVResultSet.Row row = new CSVResultSet.Row(tokens);
                rset.addRow(row);
                line = bfr.readLine();
            }
            return rset;
        } catch (IOException ex) {
            throw new ScriptException("Error while reading file: " + filePath + "\n" + ex.getMessage());
        }
    }

    public static CSVResultSet getLine(String filePath, boolean hasHeaderRow) throws ScriptException {
        try (BufferedReader bfr = new BufferedReader(new FileReader(filePath))) {
            if (bfr == null) {
                throw new ScriptException("Bad file name: " + filePath);
            }

            String line = bfr.readLine();
            CSVResultSet rset = new CSVResultSet();
            if (hasHeaderRow) {
                rset.addRow(new CSVResultSet.Row(Arrays.asList(line)));
                line = bfr.readLine();
            }

            while (line != null) {
                if (line.length() <= 0 || line.trim().isEmpty()) {
                    line = bfr.readLine();
                    continue;
                }
                CSVResultSet.Row row = new CSVResultSet.Row(Arrays.asList(line));
                rset.addRow(row);
                line = bfr.readLine();
            }
            return rset;
        } catch (IOException ex) {
            throw new ScriptException("Error while reading file: " + filePath + "\n" + ex.getMessage());
        }
    }

    private static List<String> parseString(String regex, String line) {
        String[] tokens = line.split(regex, -1);
        List<String> cleanTokens = new ArrayList<>();
        for (String token : tokens) {
            cleanTokens.add(token.replace("\"", "")); //remove quotes from values
        }
        return cleanTokens;
    }

    /* 
     * Formats given Objects into a csv delimited row.
     */
    public static String formatToCSVData(Object... objects) {
        if (objects == null || objects.length == 0) {
            return "";
        }

        String output = "";
        for (Object object : objects) {
            if (object == null) {
                object = "";
            }
            output += formatToCSVData(object.toString(), false);
        }

        if (output.length() == 0) {
            return "";
        }
        return output.substring(0, output.length() - 1); //trim trailing comma
    }

    private static String formatToCSVData(String oldData, boolean isLastCol) {
        String newData = "";

        if (oldData == null || oldData.isEmpty()) {
            if (isLastCol) {
                return newData;
            } else {
                return newData + ",";
            }
        } else {
            newData = oldData;
        }

        if (newData.contains("\"")) {
            newData = newData.replace("\"", "\"\"");
        }

        if (newData.contains(",")) {
            newData = "\"" + newData + "\"";
        }

        if (!isLastCol) {
            newData += ",";
        }

        return newData;
    }
}
