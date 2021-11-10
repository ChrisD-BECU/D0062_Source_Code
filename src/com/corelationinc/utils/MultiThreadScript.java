package com.corelationinc.utils;

import com.corelationinc.script.*;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.stream.XMLStreamException;

/**
 *
 * @author Salvatore Tosti
 */
public abstract class MultiThreadScript extends TaskManager {

    protected Script script = null;
    private Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rset = null;
    private DATA_TYPE[] nextTaskDataTypes = null;

    private boolean applyTaskLimit = false;
    private int taskLimit = Integer.MAX_VALUE;
    private int currentTaskLimit = 0;

    ConcurrentHashMap<String, ReportOutput> reports;

    private Iterator<String> iteratorCache = null;
    String delimeter = null;

    List<Object> parameters = new ArrayList<>();

    private Date postingDate = null;

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

    final protected Date getPostingDate() throws Exception {
        return this.postingDate != null
                ? this.postingDate
                : Date.valueOf(this.script.retrievePostingDateString(this.getConnection()));
    }

    protected MultiThreadScript(Script value) {
        super(value);
        this.script = value;
    }

    @Override
    public void afterTasks() throws Exception {
        closeAllReports();
    }

    final protected void limitTasks(int taskCount) {
        applyTaskLimit = true;
        taskLimit = taskCount;
    }

    //<editor-fold defaultstate="collapsed" desc="Argument Iterator Methods">
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

    public Date getNextOptionalArgumentDate() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();
        Date date = null;
        if (iterator.hasNext()) {
            String str = iterator.next();
            if (str == null || str.isEmpty()) {
                return date;
            }

            try {
                date = Date.valueOf(str);
            } catch (IllegalArgumentException e) {
                throw new ScriptException("Invalid date entered, please enter valid date using format 'yyyy-mm-dd'");
            }
        }
        return date;
    }

    public Rate getNextArgumentRate() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();

        if (iterator == null) {
            throw new ScriptException("Insufficient number of arguments provided, please enter a valid rate, e.g. from 0 to 100.");
        } else if (!iterator.hasNext()) {
            throw new ScriptException("Insufficient number of arguments provided, please enter a valid rate, e.g. from 0 to 100.");
        }

        String str = iterator.next();
        if (str == null || str.isEmpty()) {
            throw new ScriptException("Blank rate entered, please enter valid rate.");
        }
        Rate rate = null;
        try {
            rate = new Rate(str);
        } catch (IllegalArgumentException e) {
            throw new ScriptException("Invalid rate entered:" + str + ", please enter a valide rate, e.g.,  0 to 100.");
        }
        return rate;
    }

    public Rate getNextOptionalArgumentRate() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();
        Rate rate = null;
        if (iterator.hasNext()) {
            String str = iterator.next();
            if (str == null || str.isEmpty()) {
                return rate;
            }
            try {
                rate = new Rate(str);
            } catch (IllegalArgumentException e) {
                throw new ScriptException("Invalid rate entered:" + str + ", please enter a valide rate, e.g.,  0 to 100.");
            }

        }
        return rate;
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

    public int getNextOptionalArgumentInt() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();
        int value = Integer.MIN_VALUE;

        if (iterator.hasNext()) {
            String str = iterator.next();
            if (str == null || str.isEmpty()) {
                return value;
            }
            try {
                value = Integer.valueOf(str);
            } catch (IllegalArgumentException e) {
                throw new ScriptException("Blank integer entered, please enter valid integer.");
            }

        }
        return value;
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

    public String getNextOptionalArgumentString() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();
        String str = null;
        if (iterator.hasNext()) {
            str = iterator.next();
            if (str == null || str.isEmpty()) {
                return str;
            }
        }
        return str;
    }

    public Money getNextArgumentMoney() throws ScriptException {
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

        try {
            Money moneyArg = new Money(str);
            return moneyArg;
        } catch (ScriptException e) {
            throw new ScriptException("Invalid Money argument entered: " + str + ". Please enter valid Money in the form of '0.00'.");
        }

    }

    public Money getNextOptionalArgumentMoney() throws ScriptException {
        Iterator<String> iterator = getArgumentIteratorCache();
        Money moneyArg = null;

        if (iterator.hasNext()) {
            String str = iterator.next();
            if (str == null || str.isEmpty()) {
                return moneyArg;
            }

            try {
                moneyArg = new Money(str);
            } catch (ScriptException e) {
                throw new ScriptException("Invalid Money argument entered: " + str + ". Please enter valid Money in the form of '0.00'.");
            }
        }
        return moneyArg;

    }

    public void setNextArgumentListDelimeter(String delimeter) {
        this.delimeter = delimeter;
    }

    public List<String> getNextArgumentStringList() throws ScriptException {
        String wholeArg = getNextArgumentString();
        String[] argArray = null;
        if (delimeter != null) {
            argArray = wholeArg.split(this.delimeter + "\\s*");
        } else {
            argArray = wholeArg.split(",\\s*");
        }
        return Arrays.asList(argArray);

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
    //</editor-fold>

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
        MultiThreadTask task = getTask();
        task.setDataCollection(data);
        return task;
    }

    private static boolean isCollection(Object ob) {
        if (ob == null) {
            return false;
        }
        return ob instanceof Collection;
    }

    private static boolean isArray(Object ob) {
        if (ob == null) {
            return false;
        }
        return ob.getClass().isArray();
    }

    protected final void setNextTaskParameters(Object... objs) {
        List<Object> parametersList = new ArrayList<>();
        for (Object obj : objs) {
            if (isCollection(obj)) {
                Collection c = (Collection) obj;
                for (Object o : c) {
                    parametersList.add(o);
                }
            } else if (isArray(obj)) {
                Object[] arr = (Object[]) obj;
                parametersList.addAll(Arrays.asList(arr));
            } else {
                parametersList.add(obj);
            }
        }
        parameters = parametersList;
    }

    protected final void setNextTaskParametersList(List objs) {
        parameters = objs;
    }

    private void setParameters(PreparedStatement stmt) throws SQLException, ScriptException {
        int i = 1;
        for (Object parameter : parameters) {

            String className = parameter.getClass().getName();

            switch (className) {
                case "java.math.BigDecimal":
                    stmt.setBigDecimal(i, (BigDecimal) parameter);
                    break;
                case "java.sql.Date":
                    stmt.setDate(i, (Date) parameter);
                    break;
                case "java.util.Date":
                    stmt.setDate(i, (Date) parameter);
                    break;
                case "java.sql.Timestamp":
                    stmt.setTimestamp(i, (Timestamp) parameter);
                    break;
                case "java.lang.Integer":
                    stmt.setInt(i, (Integer) parameter);
                    break;
                case "java.lang.String":
                    stmt.setString(i, (String) parameter);
                    break;
                case "com.corelationinc.script.Serial":
                    Serial serial = (Serial) parameter;
                    serial.set(stmt, i);
                    break;
                case "com.corelationinc.script.Money":
                    Money money = (Money) parameter;
                    money.set(stmt, i);
                    break;
                case "com.corelationinc.script.Rate":
                    Rate rate = (Rate) parameter;
                    rate.set(stmt, i);
                    break;
                default:
                    throw new ScriptException("Unsupported parameter type: " + className);
            }
            i++;
        }
    }

    protected abstract String getNextTaskSQL() throws ScriptException;

    protected abstract MultiThreadTask getTask() throws ScriptException;

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

        private int getInt() throws ScriptException {
            return getLong().intValue();
        }

    }

    private static DATA_TYPE getColumnType(String columnClassName, int scale) {
        switch (columnClassName) {
            case "java.math.BigDecimal":
                return isMoneyField(scale) ? DATA_TYPE.MONEY : DATA_TYPE.RATE;
            case "java.sql.Date":
                return DATA_TYPE.DATE;
            case "java.util.Date":
                return DATA_TYPE.DATE;
            case "java.sql.Timestamp":
                return DATA_TYPE.TIMESTAMP;
            case "java.lang.Long":
                return DATA_TYPE.LONG;
            case "java.lang.Integer":
                return DATA_TYPE.LONG;
            default:
                return DATA_TYPE.STRING;
        }
    }

    private static boolean isMoneyField(int scale) {
        return scale == 2;
    }

    private DATA_TYPE[] getDataTypesCache(PreparedStatement stmt) throws SQLException {
        if (nextTaskDataTypes == null) {
            nextTaskDataTypes = getDataTypes(stmt);
        }
        return nextTaskDataTypes;

    }

    private DATA_TYPE[] getDataTypes(PreparedStatement stmt) throws SQLException {
        ResultSetMetaData meta = stmt.getMetaData();
        int columnCount = meta.getColumnCount();
        DATA_TYPE[] dataTypes = new DATA_TYPE[columnCount];
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String columnClassName = meta.getColumnClassName(i);
            int columnScale = meta.getScale(i);
            DATA_TYPE columnDataType = getColumnType(columnClassName, columnScale);
            dataTypes[i - 1] = columnDataType;
        }
        return dataTypes;
    }

    private OrderedDataCollection getNextRow() throws SQLException, ScriptException {
        if (this.rset == null) {
            String sql = getNextTaskSQL();
            this.stmt = getConnection().prepareStatement(sql);
            setParameters(this.stmt);
            this.rset = this.stmt.executeQuery();
        }
        if (!this.rset.next()) {
            this.rset.close();
            this.stmt.close();
            return null;
        }

        OrderedDataCollection coll = new OrderedDataCollection();
        DATA_TYPE[] dataTypes = getDataTypesCache(this.stmt);
        ResultMapping resultMapping = new ResultMapping(dataTypes);
        for (Entry<Integer, DATA_TYPE> entry : resultMapping.getEntrySet()) {
            int index = entry.getKey();
            switch (entry.getValue()) {
                case DATE:
                    Date date = rset.getDate(index);
                    coll.addDate(index, date);
                    break;
                case MONEY:
                    Money money = Money.get(rset, index);
                    coll.addMoney(index, money);
                    break;
                case SERIAL:
                    Serial serial = Serial.get(rset, index);
                    coll.addSerial(index, serial);
                    break;
                case STRING:
                    String string = rset.getString(index);
                    coll.addString(index, string);
                    break;
                case RATE:
                    Rate rate = Rate.get(rset, index);
                    coll.addRate(index, rate);
                    break;
                case TIMESTAMP:
                    Timestamp timestamp = rset.getTimestamp(index);
                    coll.addTimestamp(index, timestamp);
                    break;
                case LONG:
                    long lng = rset.getLong(index);
                    coll.addLong(index, lng);
                    break;
                default:
                    throw new ScriptException("Attempted to create OrderedDataCollection entry for unknown type.");
            }
        }
        return coll;
    }

    public abstract class MultiThreadTask extends Task {

        private MultiThreadScript manager = null;
        private OrderedDataCollection dataCollection = null;
        private Connection taskConnection = null;

        List<PreparedStatement> statementList = new ArrayList<>();

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

        public MultiThreadTask(MultiThreadScript manager) {
            this.manager = manager;
        }

        private OrderedDataCollection getDataCollection() {
            return dataCollection;
        }

        protected void enableOutput() {
            setOutputEnabled(true);
        }

        protected void disableOutput() {
            setOutputEnabled(false);
        }

        private void setDataCollection(OrderedDataCollection coll) {
            dataCollection = coll;
        }

        protected Date getDate() throws ScriptException {
            return getDataCollection().getDate();
        }

        protected Money getMoney() throws ScriptException {
            return getDataCollection().getMoney();
        }

        protected Serial getSerial() throws ScriptException {
            return getDataCollection().getSerial();
        }

        protected String getString() throws ScriptException {
            return getDataCollection().getString();
        }

        protected Rate getRate() throws ScriptException {
            return getDataCollection().getRate();
        }

        protected Timestamp getTimestamp() throws ScriptException {
            return getDataCollection().getTimestamp();
        }

        protected Long getLong() throws ScriptException {
            return getDataCollection().getLong();
        }

        protected int getInt() throws ScriptException {
            return getDataCollection().getInt();
        }

        protected DATA_TYPE getType() throws ScriptException {
            return getDataCollection().get(getDataCollection().getCurrentIndex()).type;
        }
    }

    protected class OrderedDataCollection {

        private final HashMap<Integer, DataElement> map;

        private int iteratorIndex = 1;

        protected OrderedDataCollection() {
            map = new HashMap<>();
        }

        protected int getCurrentIndex() {
            return iteratorIndex;
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

        private void addDate(int index, Date date) throws ScriptException {
            DataElement element = null;
            if (date == null) {
                element = new DataElement(DATA_TYPE.DATE, "");
            } else {
                element = new DataElement(DATA_TYPE.DATE, String.valueOf(date.getTime()));
            }
            add(index, element);
        }

        protected Date getDate(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getDate();
        }

        protected Date getDate() throws ScriptException {
            return getDate(iteratorIndex++);
        }

        private void addMoney(int index, Money money) throws ScriptException {
            DataElement element = new DataElement(DATA_TYPE.MONEY, money.toKeyBridgeString());
            add(index, element);
        }

        protected Money getMoney(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getMoney();
        }

        protected Money getMoney() throws ScriptException {
            return getMoney(iteratorIndex++);
        }

        private void addSerial(int index, Serial serial) throws ScriptException {
            DataElement element = new DataElement(DATA_TYPE.LONG, serial.toKeyBridgeString());
            add(index, element);
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

        private void addRate(int index, Rate rate) throws ScriptException {
            DataElement element = new DataElement(DATA_TYPE.RATE, rate.toKeyBridgeString());
            add(index, element);
        }

        protected Rate getRate(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getRate();
        }

        protected Rate getRate() throws ScriptException {
            return getRate(iteratorIndex++);
        }

        private void addTimestamp(int index, Timestamp timestamp) throws ScriptException {
            if (timestamp == null) {
                DataElement element = new DataElement(DATA_TYPE.TIMESTAMP, "");
                add(index, element);

            } else {
                DataElement element = new DataElement(DATA_TYPE.TIMESTAMP, timestamp.toString());
                add(index, element);
            }

        }

        protected Timestamp getTimestamp(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getTimestamp();
        }

        protected Timestamp getTimestamp() throws ScriptException {
            return getTimestamp(iteratorIndex++);
        }

        private void addLong(int index, long lng) throws ScriptException {
            DataElement element = new DataElement(DATA_TYPE.LONG, String.valueOf(lng));
            add(index, element);
        }

        protected long getLong(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getLong();
        }

        protected long getLong() throws ScriptException {
            return getLong(iteratorIndex++);
        }

        protected int getInt(int index) throws ScriptException {
            DataElement element = get(index);
            return element.getInt();
        }

        protected int getInt() throws ScriptException {
            return getInt(iteratorIndex++);
        }

    }

    class ResultMapping {

        private final HashMap<Integer, MultiThreadScript.DATA_TYPE> map;

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

    //<editor-fold defaultstate="collapsed" desc="Report Methods">
    protected enum SCRIPT_POSTING_MODE {

        VERIFY,
        POST
    }

    protected enum REPORT_TYPE {

        XML,
        TEXT
    }

    final protected PrintStream getTextReport(String title) throws Exception {
        ReportOutput output = getReport(title);
        if (output == null) {
            output = addOutputReport(title, REPORT_TYPE.TEXT, SCRIPT_POSTING_MODE.POST);
        }
        return output.getPrintStream();
    }

    final protected XMLSerialize getXMLReport(String title) throws Exception {
        ReportOutput output = getReport(title);
        if (output == null) {
            output = addOutputReport(title, REPORT_TYPE.XML, SCRIPT_POSTING_MODE.POST);
        }
        return output.getXMLSerialize();
    }

    final protected XMLSerialize getXMLReport(String title, SCRIPT_POSTING_MODE mode) throws Exception {
        ReportOutput output = getReport(title);
        if (output == null) {
            output = addOutputReport(title, REPORT_TYPE.XML, mode);
        }
        return output.getXMLSerialize();
    }

    final protected void writeToTextReport(String title, String message) throws Exception {
        PrintStream os = getTextReport(title);
        os.println(message);
    }

    private ReportOutput addOutputReport(String title, REPORT_TYPE reportType, SCRIPT_POSTING_MODE postingMode) throws Exception {
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
    //</editor-fold>

    protected class ReportOutput {

        String title;
        Object outputObject;
        REPORT_TYPE reportType;

        ReportOutput(String title, REPORT_TYPE type, SCRIPT_POSTING_MODE postingMode) throws Exception {
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

        private XMLSerialize openXMLReport(String reportTitle, SCRIPT_POSTING_MODE reportMode) throws Exception {
            Report report = script.openReport(reportTitle, Report.Format.xml);
            if (reportMode == SCRIPT_POSTING_MODE.VERIFY) {
                report.setPostingOption(false);
            } else {
                report.setPostingOption(true);

            }

            XMLSerialize xml = new XMLSerialize();
            xml.setXMLWriter(report.getBufferedOutputStream());
            xml.putStartDocument();
            xml.putBatchQuery(getPostingDate().toString());
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
