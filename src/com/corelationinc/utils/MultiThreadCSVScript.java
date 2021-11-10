package com.corelationinc.utils;

import com.corelationinc.script.*;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.stream.XMLStreamException;
import com.corelationinc.utils.ScriptCSVUtils;

/**
 *
 * @author Salvatore Tosti
 */
public abstract class MultiThreadCSVScript extends TaskManager {

    Script script = null;
    private Connection connection = null;
    ScriptCSVUtils.CSVResultSet rset = null;
    Date postingDate = null;

    private DATA_TYPE[] nextTaskDataTypes = null;

    private boolean applyTaskLimit = false;
    private int taskLimit = Integer.MAX_VALUE;
    private int currentTaskLimit = 0;
    private int currentRowNumber = 1;

    ConcurrentHashMap<String, ReportOutput> reports;

    private Iterator<String> iteratorCache = null;

    boolean hasHeaders = false;
    private Map<Integer, String> headerMap = null;
    boolean readFileFromImport = false;
    ScriptCSVUtils.DELIMITER delimiter = ScriptCSVUtils.DELIMITER.COMMA;
    String fileName = "";
    boolean hasDelimiter = true;

    /*
     * Establishes a single database connection.
     * For use in before / after tasks.
     */
    final protected Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = this.script.openDatabaseConnection();
        }
        return connection;
    }

    protected MultiThreadCSVScript(Script value) {
        super(value);
        this.script = value;
    }

    @Override
    public final void beforeTasks() throws Exception {
        fileName = getNextArgumentString();
        beforeTasksHook();
    }

    protected void beforeTasksHook() throws Exception {
    }

    @Override
    public final void afterTasks() throws Exception {
        afterTasksHook();
        closeAllReports();
    }

    protected void afterTasksHook() throws Exception {
    }

    final protected void limitTasks(int taskCount) {
        applyTaskLimit = true;
        taskLimit = taskCount;
    }

    public final Iterator<String> getArgumentIterator() {
        return this.script.getArgumentIterator();
    }

    private Iterator<String> getArgumentIteratorCache() {
        if (iteratorCache == null) {
            iteratorCache = getArgumentIterator();
        }
        return iteratorCache;
    }

    public Date getNextArgumentDate() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();

        if (iterator == null) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid date using format 'yyyy-mm-dd'");
        } else if (!iterator.hasNext()) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid date using format 'yyyy-mm-dd'");
        }

        String str = iterator.next();
        if (str == null || str.isEmpty()) {
            throw new ScriptException("Blank date entered, please enter valid date using format 'yyyy-mm-dd'");
        }
        Date date = null;
        try {
            date = Date.valueOf(str);
        } catch (IllegalArgumentException e) {
            throw new ScriptException("Invalid date entered, please enter valid date using format 'yyyy-mm-dd'");
        }
        return date;
    }

    public int getNextArgumentInt() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();

        if (iterator == null) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid integer.");
        } else if (!iterator.hasNext()) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid integer.");
        }

        String str = iterator.next();
        if (str == null || str.isEmpty()) {
            throw new ScriptException("Blank integer entered, please enter valid integer.");
        }

        try {
            int value = Integer.valueOf(str);
            return value;
        } catch (IllegalArgumentException e) {
            throw new ScriptException("Blank integer entered, please enter valid integer.");
        }
    }

    public String getNextArgumentString() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();

        if (iterator == null) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid string.");
        } else if (!iterator.hasNext()) {
            throw new ScriptException("Insufficient number of arguments provided, please enter valid string.");
        }

        String str = iterator.next();
        if (str == null || str.isEmpty()) {
            throw new ScriptException("Blank string entered, please enter valid string.");
        }

        return str;
    }

    public List<String> getRemainingArguments() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();

        if (iterator == null) {
            throw new ScriptException("Insufficient number of arguments provided.");
        }

        List<String> argumentList = new ArrayList<>();
        while (iterator.hasNext()) {
            argumentList.add(iterator.next());
        }
        return argumentList;
    }

    @Override
    public final Task nextTask() throws Exception {
        if (applyTaskLimit) {
            if (currentTaskLimit >= taskLimit) {
                return null;
            }
            currentTaskLimit++;
        }
        
        OrderedDataCollection data = getNextRow();
        if (data == null) {
            return null;
        }
        MultiThreadCSVTask task = getTask();
        task.setDataCollection(data);
        task.setRowNumber(currentRowNumber++);
        return task;
    }

    /* CSV Info setters */
    protected final void hasHeaders() {
        hasHeaders = true;
    }

    protected final void readFileFromImport() {
        readFileFromImport = true;
    }

    protected final void hasCommaDelimiter() {
        delimiter = ScriptCSVUtils.DELIMITER.COMMA;
    }

    protected final void hasPipeDelimiter() {
        delimiter = ScriptCSVUtils.DELIMITER.PIPE;
    }

    protected final void hasTabDelimiter() {
        delimiter = ScriptCSVUtils.DELIMITER.TAB;
    }
    
    protected final void notDelimited() {
        hasDelimiter = false;
    }
    
    final protected Date getPostingDate() throws Exception {
        return this.postingDate != null
                ? this.postingDate
                : Date.valueOf(this.script.retrievePostingDateString(this.getConnection()));
    }

    protected final Map<Integer, String> getHeaderMap() throws ScriptException {
        if (this.rset == null) {
            throw new ScriptException("Attempted to fetch header map before results have been initialized.");
        }
        if (!this.hasHeaders) {
            throw new ScriptException("Attempted to fetch header map where header read is disabled.");
        }
        if (this.headerMap != null) {
            return this.headerMap;
        }

        this.headerMap = new HashMap<>();
        int i = 0;
        for (String header : rset.getHeaders()) {
            this.headerMap.put(i++, header);
        }
        return this.headerMap;
    }

    protected abstract MultiThreadCSVTask getTask() throws ScriptException;

    protected enum DATA_TYPE {

        SERIAL,
        MONEY,
        DATE,
        STRING,
        RATE,
        TIMESTAMP,
        LONG
    }

    private class DataElement {

        DATA_TYPE type;
        String value;

        protected DataElement(DATA_TYPE type, String value) {
            this.value = value;
            this.type = type;
        }

        private Serial getSerial() throws ScriptException {
            if (type == DATA_TYPE.LONG) {
                Serial s = new Serial();
                s.fromString(value);
                return s;
            } else {
                throw new ScriptException("Attempted to get a Serial from a non-Serial DataElement!");
            }
        }

        private Money getMoney() throws ScriptException {
            if (type == DATA_TYPE.MONEY) {
                Money money = new Money();
                money.fromString(value);
                return money;
            } else {
                throw new ScriptException("Attempted to get a Money from a non-Money DataElement!");
            }
        }

        private Date getDate() throws ScriptException {
            if (type == DATA_TYPE.DATE) {
                if (value.isEmpty()) {
                    return null;
                } else {
                    return new Date(Long.parseLong(value));
                }
            } else {
                throw new ScriptException("Attempted to get a Date from a non-Date DataElement!");
            }
        }

        private String getString() throws ScriptException {
            if (type == DATA_TYPE.STRING) {
                return value;
            } else {
                throw new ScriptException("Attempted to get a String from a non-String DataElement!");
            }
        }

        private Rate getRate() throws ScriptException {
            if (type == DATA_TYPE.RATE) {
                return new Rate(value);
            } else {
                throw new ScriptException("Attempted to get a Rate from a non-Rate DataElement!");
            }
        }

        private Timestamp getTimestamp() throws ScriptException {
            if (type == DATA_TYPE.TIMESTAMP) {
                if (value.isEmpty()) {
                    return null;
                } else {
                    return Timestamp.valueOf(value);
                }

            } else {
                throw new ScriptException("Attempted to get a Timestamp from a non-Timestamp DataElement!");
            }
        }

        private Long getLong() throws ScriptException {
            if (type == DATA_TYPE.LONG) {
                return Long.parseLong(value);
            } else {
                throw new ScriptException("Attempted to get a Long from a non-Long DataElement!");
            }
        }

    }

    private DATA_TYPE[] getDataTypesCache() throws SQLException, ScriptException {
        if (nextTaskDataTypes == null) {
            DATA_TYPE[] dataTypes = new DATA_TYPE[this.rset.currentRowSize()];
            for (int i = 0; i < this.rset.currentRowSize(); i++) {
                dataTypes[i] = DATA_TYPE.STRING;
            }
            nextTaskDataTypes = dataTypes;
        }
        return nextTaskDataTypes;
    }

    private OrderedDataCollection getNextRow() throws SQLException, ScriptException {
        if (this.rset == null) {
            String filePath = this.script.getDatabaseHomePathName();
            if (this.readFileFromImport) {
                if (this.script.getDatabaseHomePathName().endsWith(File.separator)) {
                    filePath += "import" + File.separator;
                } else {
                    filePath += File.separator + "import" + File.separator;
                }
            }
            filePath += this.fileName;
            if(hasDelimiter){   //true by default unless hasNoDelimiter() is called
                
                this.rset = ScriptCSVUtils.parse(filePath, delimiter, this.hasHeaders);
            } else{
                
                this.rset = ScriptCSVUtils.getLine(filePath, hasHeaders);
            }
        }
        if (!this.rset.next()) {
            return null;
        }

        OrderedDataCollection coll = new OrderedDataCollection();
        DATA_TYPE[] dataTypes = getDataTypesCache();
        ResultMapping resultMapping = new ResultMapping(dataTypes);
        for (Entry<Integer, DATA_TYPE> entry : resultMapping.getEntrySet()) {
            int index = entry.getKey();
            switch (entry.getValue()) {
                case STRING:
                    String string = rset.getString(index - 1); //resultMappings are base-1, not base-0
                    coll.addString(index, string);
                    break;
                default:
                    throw new ScriptException("Attempted to create OrderedDataCollection entry for unknown type.");
            }
        }
        return coll;
    }

    public abstract class MultiThreadCSVTask extends Task {

        private MultiThreadCSVScript manager = null;
        private OrderedDataCollection dataCollection = null;
        private Connection taskConnection = null;

        List<PreparedStatement> statementList = new ArrayList<>();

        private int rowNumber = 0;

        private void setRowNumber(int rowNumber) {
            this.rowNumber = rowNumber;
        }

        protected int getRowNumber() {
            return this.rowNumber;
        }

        protected final Map<Integer, String> getHeaderMap() throws ScriptException {
            if (rset == null) {
                throw new ScriptException("Attempted to fetch header map before results have been initialized.");
            }
            if (!hasHeaders) {
                throw new ScriptException("Attempted to fetch header map where header read is disabled.");
            }
            if (headerMap != null) {
                return headerMap;
            }

            headerMap = new HashMap<>();
            int i = 0;
            for (String header : rset.getHeaders()) {
                headerMap.put(i++, header);
            }
            return headerMap;
        }

        /**
         * Returns connection associated with task. Connection is cached on for
         * subsequent calls.
         *
         * @return
         * @throws SQLException
         */
        protected Connection getConnection() throws SQLException {
            if (taskConnection == null) {
                taskConnection = this.getDatabaseConnection();
            }
            return taskConnection;
        }

        protected int allocatedStatementCount() {
            return statementList.size();
        }

        public MultiThreadCSVTask(MultiThreadCSVScript manager) {
            this.manager = manager;
        }

        private OrderedDataCollection getDataCollection() {
            return dataCollection;
        }

        @Override
        public abstract void perform() throws Exception;

        @Override
        public abstract void output() throws Exception;

        protected void enableOutput() {
            setOutputEnabled(true);
        }

        protected void disableOutput() {
            setOutputEnabled(false);
        }

        private void setDataCollection(OrderedDataCollection coll) {
            dataCollection = coll;
        }

//        protected Date getDate() throws ScriptException {
//            return getDataCollection().getDate();
//        }
//
//        protected Money getMoney() throws ScriptException {
//            return getDataCollection().getMoney();
//        }
//
//        protected Serial getSerial() throws ScriptException {
//            return getDataCollection().getSerial();
//        }
//
        protected String getString() throws ScriptException {
            return getDataCollection().getString();
        }
//
//        protected Rate getRate() throws ScriptException {
//            return getDataCollection().getRate();
//        }
//
//        protected Timestamp getTimestamp() throws ScriptException {
//            return getDataCollection().getTimestamp();
//        }
//
//        protected Long getLong() throws ScriptException {
//            return getDataCollection().getLong();
//        }
    }

    protected class OrderedDataCollection {

        private final HashMap<Integer, DataElement> map;

        private int iteratorIndex = 1;

        protected OrderedDataCollection() {
            map = new HashMap<>();
        }

        private void add(int index, DataElement element) throws ScriptException {
            if (map.get(index) != null) {
                throw new ScriptException("Attempted to add element for an existing index!");
            }
            map.put(index, element);
        }

        private DataElement get(int index) throws ScriptException {
            DataElement element = map.get(index);
            if (element == null) {
                throw new ScriptException("Attempted to get element for an index which does not exist!");
            }
            return element;
        }

        protected Date getDate(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getDate();
        }

        protected Date getDate() throws ScriptException {
            return getDate(iteratorIndex++);
        }

        protected Money getMoney(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getMoney();
        }

        protected Money getMoney() throws ScriptException {
            return getMoney(iteratorIndex++);
        }

        protected Serial getSerial(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getSerial();
        }

        protected Serial getSerial() throws ScriptException {
            return getSerial(iteratorIndex++);
        }

        private void addString(int index, String string) throws ScriptException {
            DataElement element = new DataElement(DATA_TYPE.STRING, string);
            add(index, element);
        }

        protected String getString(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getString();
        }

        protected String getString() throws ScriptException {
            return getString(iteratorIndex++);
        }

        protected Rate getRate(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getRate();
        }

        protected Rate getRate() throws ScriptException {
            return getRate(iteratorIndex++);
        }

        protected Timestamp getTimestamp(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getTimestamp();
        }

        protected Timestamp getTimestamp() throws ScriptException {
            return getTimestamp(iteratorIndex++);
        }

        protected long getLong(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getLong();
        }

        protected long getLong() throws ScriptException {
            return getLong(iteratorIndex++);
        }

    }

    class ResultMapping {

        private final HashMap<Integer, MultiThreadCSVScript.DATA_TYPE> map;

        protected ResultMapping() {
            map = new HashMap<>();
        }

        protected ResultMapping(DATA_TYPE[] types) throws ScriptException {
            map = new HashMap<>();
            addMappings(types);

        }

        protected final void addMappings(DATA_TYPE[] types) throws ScriptException {

            for (int i = 0; i < types.length; i++) {
                addMapping(i + 1, types[i]);
            }
        }

        protected final void addMapping(int index, DATA_TYPE type) throws ScriptException {
            if (map.get(index) != null) {
                throw new ScriptException("Attempted to add mapping for an existing index!");
            }
            map.put(index, type);
        }

        private Set<Entry<Integer, DATA_TYPE>> getEntrySet() {
            return map.entrySet();
        }

    }

    protected enum SCRIPT_POSTING_MODE {

        VERIFY,
        POST
    }

    protected enum REPORT_TYPE {

        XML,
        TEXT
    }

    final protected XMLSerialize openXMLReport(String title, SCRIPT_POSTING_MODE postingMode) throws SQLException, ScriptException, XMLStreamException {
        ReportOutput output = addOutputReport(title, REPORT_TYPE.XML, postingMode);
        return output.getXMLSerialize();
    }

    private PrintStream openTextReport(String title) throws SQLException, ScriptException, XMLStreamException {
        ReportOutput output = addOutputReport(title, REPORT_TYPE.TEXT, SCRIPT_POSTING_MODE.VERIFY);
        return output.getPrintStream();
    }

    final protected PrintStream getTextReport(String title) throws ScriptException, SQLException, XMLStreamException {
        ReportOutput output = getReport(title);
        if (output == null) {
            output = addOutputReport(title, REPORT_TYPE.TEXT, SCRIPT_POSTING_MODE.POST);
        }
        return output.getPrintStream();
    }

    final protected XMLSerialize getXMLReport(String title) throws ScriptException, SQLException, XMLStreamException {
        ReportOutput output = getReport(title);
        if (output == null) {
            output = addOutputReport(title, REPORT_TYPE.XML, SCRIPT_POSTING_MODE.POST);
        }
        return output.getXMLSerialize();
    }

    final protected void writeToTextReport(String title, String message) throws ScriptException, SQLException, XMLStreamException {
        PrintStream os = getTextReport(title);
        os.println(message);
    }

    private ReportOutput addOutputReport(String title, REPORT_TYPE reportType, SCRIPT_POSTING_MODE postingMode) throws SQLException, ScriptException, XMLStreamException {
        if (reports == null) {
            reports = new ConcurrentHashMap<>();
        }
        if (reports.get(title) != null) {
            throw new ScriptException("Attempted to add a report that has already exists: " + title);
        }
        ReportOutput output = new ReportOutput(title, reportType, postingMode);
        reports.put(title, output);
        return output;
    }

    final protected void closeReport(String title) throws ScriptException {
        if (reports == null) {
            throw new ScriptException("Attempted to close a report that has not been initialized: " + title);
        }

        ReportOutput output = getReport(title);
        if (output == null) {
            throw new ScriptException("Attempted to close a report that has not been initialized: " + title);
        }

        output.closeReport();
    }

    private ReportOutput getReport(String title) {
        if (reports == null) {
            reports = new ConcurrentHashMap<>();
        }

        ReportOutput output = reports.get(title);
        if (output == null) {
            return null;
        } else {
            return output;
        }
    }

    final protected void closeAllReports() throws ScriptException {
        if (reports == null) {
            return;
            //throw new ScriptException("Attempted to close all reports, but none have been initialized.");
        }

        for (Entry<String, ReportOutput> entry : reports.entrySet()) {
            entry.getValue().closeReport();
        }
    }

    protected class ReportOutput {

        String title;
        Object outputObject;
        REPORT_TYPE reportType;

        ReportOutput(String title, REPORT_TYPE type, SCRIPT_POSTING_MODE postingMode) throws SQLException, ScriptException, XMLStreamException {
            if (type == REPORT_TYPE.XML) {
                XMLSerialize xml = openXMLReport(title, postingMode);
                outputObject = (Object) xml;
                reportType = type;
            } else if (type == REPORT_TYPE.TEXT) {
                PrintStream os = openTextReport(title);
                outputObject = (Object) os;
                reportType = type;
            }
        }

        protected XMLSerialize getXMLSerialize() throws ScriptException {
            if (reportType == REPORT_TYPE.XML) {
                return (XMLSerialize) outputObject;
            } else {
                throw new ScriptException("Attempted to fetch an XML report output object from a non-XML report!");
            }
        }

        protected PrintStream getPrintStream() throws ScriptException {
            if (reportType == REPORT_TYPE.TEXT) {
                return (PrintStream) outputObject;
            } else {
                throw new ScriptException("Attempted to fetch a PrintStream report output object from a non-text report!");
            }
        }

        protected Object getOuputObject() {
            return outputObject;
        }

        private PrintStream openTextReport(String reportTitle) throws ScriptException {
            Report report = script.openReport(reportTitle, Report.Format.txt);
            report.setPostingOption(false);
            PrintStream os = new PrintStream(report.getBufferedOutputStream());
            return os;
        }

        private XMLSerialize openXMLReport(String reportTitle, SCRIPT_POSTING_MODE reportMode) throws SQLException, ScriptException, XMLStreamException {
            String postingDate = script.retrievePostingDateString(getConnection());
            Report report = script.openReport(reportTitle, Report.Format.xml);
            if (reportMode == SCRIPT_POSTING_MODE.VERIFY) {
                report.setPostingOption(false);
            } else {
                report.setPostingOption(true);

            }

            XMLSerialize xml = new XMLSerialize();
            xml.setXMLWriter(report.getBufferedOutputStream());
            xml.putStartDocument();
            xml.putBatchQuery(postingDate);
            return xml;
        }

        private void closeReport() throws ScriptException {
            switch (reportType) {
                case XML:
                    try {
                        XMLSerialize xml = getXMLSerialize();
                        xml.put();
                        xml.putEndDocument();
                    } catch (XMLStreamException e) {
                        throw new ScriptException("Error while closing XML Document for " + title + ".");
                    }
                    break;
                case TEXT:
                    PrintStream os = getPrintStream();
                    os.close();
                    break;
                default:
                    break;
            }
        }

    }

}
