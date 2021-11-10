package com.corelationinc.utils;

import com.corelationinc.script.Money;
import com.corelationinc.script.Rate;
import com.corelationinc.script.ScriptException;
import com.corelationinc.script.Serial;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The SQLRun classes serve as a replacement for a standard SQL statement setup.
 * To use the SQLSingleRun class, create a new instance, passing a connection,
 * SQL statement string and then any necessary parameters. (parameters will be
 * inserted into the statement in the order they are passed to the constructor.)
 * Then call .next() on the SQLRun object until it runs out of results. This
 * will advance the internal result set, data can then be accessed using the
 * 'getX' function. Once the result set is exhausted, you can close the
 * statement using the .close(), or a try block can be used to automatically
 * close the SQLSingleRun object.
 * <br>Ex.</br>
 *
 * <br>SQLSingleRun run = new SQLSingleRun(getConnection(), "SELECT SERIAL FROM
 * CORE.SHARE WHERE OPEN_DATE BETWEEN ? AND ?", new Date("1900-01-01"), new
 * Date("2100-01-01")); </br>
 * <br>
 * while(run.next()){ </br>
 * <br> Serial serial = run.getSerial(); </br>
 * <br> //do processing here
 * </br>
 * <br>} </br>
 * <br>run.close(); </br>
 *
 * @author stosti
 */
public class SQLSingleRun implements AutoCloseable {

    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rset = null;
    String sql = null;
    DATA_TYPE[] nextTaskDataTypes = null;
    OrderedDataCollection currentData = null;
    ResultSetMetaData metaData = null;

    /**
     * Prepares and runs provided SQL statement. Optional parameters can be
     * provided, they will be set in the order they are provided.
     * <br></br>
     * <p>
     * Supported parameter types:</p>
     * <br>java.math.BigDecimal</br>
     * <br>java.sql.Date</br>
     * <br>java.sql.Timestambr</br>
     * <br>java.lang.Integer</br>
     * <br>java.lang.String</br>
     * <br>com.corelationinc.script.Serial</br>
     * <br>com.corelationinc.script.Money</br>
     * <br>com.corelationinc.script.Rate</br>
     *
     *
     * @param connection
     * @param sql
     * @param parameters
     * @throws SQLException
     * @throws ScriptException
     */
    public SQLSingleRun(Connection connection, String sql, Object... parameters) throws SQLException, ScriptException {
        construct(connection, sql, parameters);
    }

    /**
     * Prepares and runs provided SQL statement. Optional parameters are
     * provided in a list, they will be set in the order they appear within the
     * list.
     * <br></br>
     * <p>
     * Supported parameter types:</p>
     * <br>java.math.BigDecimal</br>
     * <br>java.sql.Date</br>
     * <br>java.sql.Timestambr</br>
     * <br>java.lang.Integer</br>
     * <br>java.lang.String</br>
     * <br>com.corelationinc.script.Serial</br>
     * <br>com.corelationinc.script.Money</br>
     * <br>com.corelationinc.script.Rate</br>
     *
     * @param connection
     * @param sql
     * @param parameters
     * @throws SQLException
     * @throws ScriptException
     */
    public SQLSingleRun(Connection connection, String sql, List<Object> parameters) throws SQLException, ScriptException {
        construct(connection, sql, parameters.toArray(new Object[0]));
    }

    private void construct(Connection connection, String sql, Object[] parameters) throws SQLException, ScriptException {
        stmt = connection.prepareStatement(sql);
        setParameters(stmt, parameters);
        rset = stmt.executeQuery();
        if (!rset.next()) {
            rset.close();
            stmt.close();
        } else {
            metaData = rset.getMetaData();
        }
    }

    @Override
    public void close() throws SQLException {
        if(this.stmt != null) {
            this.stmt.close();
        }
    }

    /**
     * Advances internal counter, allows access to next row's results.
     * Automatically closes SQL statement once counter is advance to final row
     * in result set.
     *
     * @return @throws SQLException
     * @throws ScriptException
     */
    public boolean next() throws SQLException, ScriptException {
        if (rset.isClosed()) {
            return false;
        }
        OrderedDataCollection coll = new OrderedDataCollection();
        DATA_TYPE[] dataTypes = getDataTypesCache(this.stmt);
        ResultMapping resultMapping = new ResultMapping(dataTypes);
        for (Map.Entry<Integer, DATA_TYPE> entry : resultMapping.getEntrySet()) {
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
        this.currentData = coll;
        if (!rset.next()) {
            rset.close();
            stmt.close();
        }
        return true;
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    private OrderedDataCollection getDataCollection() {
        return this.currentData;
    }

    /**
     * Returns next item in the current result row as a Date.
     *
     * @return @throws ScriptException
     */
    public Date getDate() throws ScriptException {
        return getDataCollection().getDate();
    }

    /**
     * Returns next item in the current result row as a Money.
     *
     * @return @throws ScriptException
     */
    public Money getMoney() throws ScriptException {
        return getDataCollection().getMoney();
    }

    /**
     * Returns next item in the current result row as a Serial.
     *
     * @return @throws ScriptException
     */
    public Serial getSerial() throws ScriptException {
        return getDataCollection().getSerial();
    }

    /**
     * Returns next item in the current result row as a String.
     *
     * @return @throws ScriptException
     */
    public String getString() throws ScriptException {
        return getDataCollection().getString();
    }

    /**
     * Returns next item in the current result row as a Rate.
     *
     * @return @throws ScriptException
     */
    public Rate getRate() throws ScriptException {
        return getDataCollection().getRate();
    }

    /**
     * Returns next item in the current result row as a Timestamp.
     *
     * @return @throws ScriptException
     */
    public Timestamp getTimestamp() throws ScriptException {
        return getDataCollection().getTimestamp();
    }

    /**
     * Returns next item in the current result row as a Long.
     *
     * @return @throws ScriptException
     */
    public Long getLong() throws ScriptException {
        return getDataCollection().getLong();
    }

    /**
     * Returns next item in the current result row as an int.
     *
     * @return @throws ScriptException
     */
    public int getInt() throws ScriptException {
        return getLong().intValue();
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

    private int setParametersInternal(PreparedStatement stmt, Object parameter, int index) throws SQLException, ScriptException {
        if (parameter instanceof Collection) {
            Collection<Object> coll = (Collection) parameter;
            for (Object obj : coll) {
                index = setParametersInternal(stmt, obj, index);

            }
            return index;
        }
        String className = parameter.getClass().getName();
        switch (className) {
            case "java.math.BigDecimal":
                stmt.setBigDecimal(index, (BigDecimal) parameter);
                break;
            case "java.sql.Date":
                stmt.setDate(index, (Date) parameter);
                break;
            case "java.sql.Timestamp":
                stmt.setTimestamp(index, (Timestamp) parameter);
                break;
            case "java.lang.Integer":
                stmt.setInt(index, (Integer) parameter);
                break;
            case "java.lang.String":
                stmt.setString(index, (String) parameter);
                break;
            case "com.corelationinc.script.Serial":
                Serial serial = (Serial) parameter;
                serial.set(stmt, index);
                break;
            case "com.corelationinc.script.Money":
                Money money = (Money) parameter;
                money.set(stmt, index);
                break;
            case "com.corelationinc.script.Rate":
                Rate rate = (Rate) parameter;
                rate.set(stmt, index);
                break;
            default:
                throw new ScriptException("Unsupported parameter type: " + className);
        }
        index++;
        return index;
    }

    private void setParameters(PreparedStatement stmt, Object[] parameters) throws SQLException, ScriptException {
        int index = 1;
        for (Object obj : parameters) {
            index = setParametersInternal(stmt, obj, index);
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

    private enum DATA_TYPE {

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

    private class OrderedDataCollection {

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

    }

    private class ResultMapping {

        private final HashMap<Integer, DATA_TYPE> map;

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

        private Set<Map.Entry<Integer, DATA_TYPE>> getEntrySet() {
            return map.entrySet();
        }

    }
}
