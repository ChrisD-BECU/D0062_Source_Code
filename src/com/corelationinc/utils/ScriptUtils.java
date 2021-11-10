package com.corelationinc.utils;

import com.corelationinc.script.Money;
import com.corelationinc.script.ScriptException;
import com.corelationinc.script.Serial;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author stosti
 */
public class ScriptUtils {


    /**
     * Returns a string which contains a given number of question marks
     * separated by commas. Ex. createInStatementVariables(3) yields "?,?,?".
     *
     * @param variableCount number of question marks
     * @return string consisting of question marks and commas
     */
    public static String createInStatementVariables(int variableCount) {
        if (variableCount <= 0) {
            return "";
        }

        String questionMarks = "";
        for (int i = 0; i < variableCount; i++) {
            questionMarks += "?,";
        }

        return questionMarks.substring(0, questionMarks.length() - 1);
    }

    /**
     * Returns a string which contains a number of question marks, separated by
     * commas, equal to the number of items in a given collection. Ex. (Where
     * the variable 'list' is a List that contains three items)
     * createInStatementVariables(list) yields "?,?,?".
     *
     * @param collection
     * @return
     */
    public static String createInStatementVariables(Collection collection) {
        int i = createInstatementVariablesInternal(collection, 0);
        return createInStatementVariables(i);
    }

    /**
     * Returns a string which contains a number of question marks, separated by
     * commas, equal to the number of items in a given array. Ex. (Where the
     * variable 'array' is an array that contains three items)
     * createInStatementVariables(array) yields "?,?,?".
     *
     * @param array
     * @return
     */
    public static String createInStatementVariables(Object[] array) {
        int i = createInstatementVariablesInternal(array, 0);
        return createInStatementVariables(i);
    }

    private static int createInstatementVariablesInternal(Object object, int currCounter) {
        if (object instanceof Collection) {
            Collection<Object> coll = (Collection) object;
            for (Object obj : coll) {
                currCounter = createInstatementVariablesInternal(obj, currCounter);
            }
            return currCounter;
        }
        if (object instanceof Object[]) {
            Object[] coll = (Object[]) object;
            for (Object obj : coll) {
                currCounter = createInstatementVariablesInternal(obj, currCounter);
            }
            return currCounter;
        }
        currCounter++;
        return currCounter;

    }

    /**
     * Returns a Map of all open courtesy pay restrictions, descriptions are
     * keys and serials are values.
     *
     * @param connection - Database connection
     * @return Map<String, Serial>
     * @throws Exception - If the map is empty
     */
    public static Map<String, Serial> getCourtesyPayRestrictions(Connection connection) throws Exception {

        String sql = "SELECT DESCRIPTION, SERIAL FROM CORE.COURTESY_PAY_RESTRICTION WHERE STATUS = 'O' ORDER BY SERIAL";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            HashMap<String, Serial> tempMap = new HashMap();
            while (rset.next()) {
                tempMap.put(rset.getString(1), Serial.get(rset, 2));
            }
            if (tempMap.isEmpty()) {
                throw new ScriptException("Could not find any open courtesy pay restriction records.");
            }
            return Collections.unmodifiableMap(tempMap);
        }
    }


    public enum LOAN_TYPE_CATEGORY {

        CLOSED_END,
        OPEN_END,
        LINE_OF_CREDIT,
        CREDIT_CARD
    }


    public static Map<Serial, String> getSerialDescriptionMap(Connection connection, String table, List<String> descriptions) throws SQLException {
        Map<Serial, String> shareTypeMap = new HashMap<>();
        String sql = " SELECT "
                + "     SERIAL, "
                + "     DESCRIPTION"
                + " FROM"
                + "     CORE." + table + ""
                + " WHERE "
                + "     DESCRIPTION IN ( " + ScriptUtils.createInStatementVariables(descriptions) + " ) ";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int i = 1;
            for (String share : descriptions) {
                stmt.setString(i++, share);
            }
            ResultSet rset = stmt.executeQuery();
            while (rset.next()) {
                shareTypeMap.put(Serial.get(rset, 1), rset.getString(2));
            }
            return shareTypeMap;
        }
    }

    public static Map<Serial, String> getSerialDescriptionMap(Connection connection, String table, String[] descriptions) throws SQLException {
        return ScriptUtils.getSerialDescriptionMap(connection, table, Arrays.asList(descriptions));
    }

}
