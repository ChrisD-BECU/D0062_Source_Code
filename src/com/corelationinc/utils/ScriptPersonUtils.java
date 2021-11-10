package com.corelationinc.utils;

import com.corelationinc.script.Serial;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author stosti
 */
public class ScriptPersonUtils {


    /**
     * Determines if a person is at least the given number of years old.
     * <br></br>
     * Note: If the person does not have a birth-date populated, this function
     * returns true.
     * <br></br>
     * PERSON.BIRTH_DATE + X YEARS <= ENV.POSTING_DATE
     *
     * @param connection current connection to the database.
     * @param personSerial person serial to pass to the query to check the
     * members age.
     * @param age minimum age of the person in years.
     * @return true if the member is over given age
     * @throws SQLException
     */
    public static boolean isAtLeastAge(Connection connection, Serial personSerial, int ageInyears) throws SQLException {
        String sql = "SELECT"
                + "    1"
                + " FROM"
                + "    CORE.PERSON AS PERSON INNER JOIN"
                + "    CORE.ENV AS ENV ON"
                + "        ENV.SERIAL > 0"
                + " WHERE"
                + "    ((PERSON.BIRTH_DATE + ? Years <= ENV.POSTING_DATE) OR (PERSON.BIRTH_DATE IS NULL))AND"
                + "    PERSON.SERIAL = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, ageInyears);
            personSerial.set(stmt, 2);
            ResultSet rset = stmt.executeQuery();
            return rset.next();
        }
    }
}
