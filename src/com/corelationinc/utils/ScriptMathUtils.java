package com.corelationinc.utils;

import com.corelationinc.script.Money;
import com.corelationinc.script.Rate;
import com.corelationinc.script.ScriptException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 *
 * @author JTowner
 */
public class ScriptMathUtils {

    public static String CLASS_NAME = "ScriptMathUtils";

    private static class Variable {

        private final Object var;
        private final String varName;
        private final String methodHandler;

        public Variable(Object var, String varName, String methodHandler) throws ScriptException {
            if (varName == null || varName.isEmpty()) {
                throw new ScriptException("Cannot pass a null or empty variable name");
            }

            if (methodHandler == null || methodHandler.isEmpty()) {
                throw new ScriptException("Cannot pass a null or empty variable name");
            }

            this.var = var;
            this.varName = varName;
            this.methodHandler = methodHandler;
        }

        public void verifyContents() throws ScriptException {
            if (this.var == null) {
                throw new ScriptException("Variable Name: " + this.varName + " cannot be null in method: " + this.methodHandler);
            }
        }
    }

    private static void verifyArguments(Variable... varList) throws ScriptException {
        if (varList.length > 0) {
            for (Variable var : varList) {
                var.verifyContents();
            }
        }
    }

    /**
     * This method attempts to divide a money object by a rate object. The scale
     * is defaulted to 2 and the rounding mode is defaulted to HALF_UP
     *
     * @param dividend - the amount to be divided
     * @param divisor - the rate at which to divide the amount by
     * @return - the result of the division as a Money object
     * @throws Exception - if null variables are passed or 0 is passed as the
     * divisor
     */
    public static Money divide(Money dividend, Rate divisor) throws Exception {
        return new Money(divide(
                new BigDecimal(dividend.toKeyBridgeString()),
                new BigDecimal(divisor.toKeyBridgeString()),
                2, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to divide a money object by a rate object. The scale
     * is defaulted to 2 and the rounding mode is defaulted to HALF_UP
     *
     * @param dividend - the amount to be divided
     * @param divisor - the amount to divide the first amount by
     * @return - the result of the division as a Money object
     * @throws Exception - if null variables are passed or 0 is passed as the
     * divisor
     */
    public static Money divide(Money dividend, Money divisor) throws Exception {

        return new Money(divide(
                new BigDecimal(dividend.toKeyBridgeString()),
                new BigDecimal(divisor.toKeyBridgeString()),
                2, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to divide two big decimal objects together
     *
     * @param dividend - the big decimal to be divided by
     * @param divisor - the big decimal to divide by
     * @param scale - the scale of the division
     * @param roundMode - how to round by
     * @return - the result of the division as a Money object
     * @throws Exception - if null variables are passed or 0 is passed as the
     * divisor
     */
    public static BigDecimal divide(BigDecimal dividend, BigDecimal divisor, int scale, RoundingMode roundMode) throws Exception {
        String methodHandler = ScriptMathUtils.CLASS_NAME + ".divide";
        verifyArguments(
                new Variable(dividend, "dividend", methodHandler),
                new Variable(divisor, "divisor", methodHandler));

        if (divisor.compareTo(BigDecimal.ZERO) == 0) {
            throw new ScriptException("Variable Name: divisor cannot be 0 in method: " + ScriptMathUtils.CLASS_NAME + ".divide");
        }

        return dividend.divide(divisor, scale, roundMode);
    }

    public static Rate divide(Rate dividend, BigDecimal divisor) throws Exception {
        return new Rate(divide(
                new BigDecimal(dividend.toKeyBridgeString()),
                divisor,
                5, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to multiply a money object by a rate object
     *
     * @param multiplicand - the amount to be multiplied
     * @param multiplier - the rate to multiply by
     * @return - the result of the multiplication as a Money object
     * @throws Exception - if null variables are passed
     */
    public static Money multiply(Money multiplicand, Rate multiplier) throws Exception {
        return new Money(
                multiply(
                        new BigDecimal(multiplicand.toKeyBridgeString()),
                        new BigDecimal(multiplier.toKeyBridgeString()),
                        2, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to multiply a money object by a rate object
     *
     * @param multiplicand - the amount to be multiplied
     * @param multiplier - the amount to multiply by
     * @return - the result of the multiplication as a Money object
     * @throws Exception - if null variables are passed
     */
    public static Money multiply(Money multiplicand, Money multiplier) throws Exception {
        return new Money(
                multiply(
                        new BigDecimal(multiplicand.toString()),
                        new BigDecimal(multiplier.toString()),
                        2, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to multiply a money object by a rate object
     *
     * @param multiplicand - the big decimal to be multiplied
     * @param multiplier - the big decimal to multiply by
     * @param scale - the scale of the result
     * @param roundMode - the rounding mode for the result
     * @return - the result of the multiplication as a BigDecimal object
     * @throws Exception - if null variables are passed
     */
    public static BigDecimal multiply(BigDecimal multiplicand, BigDecimal multiplier, int scale, RoundingMode roundMode) throws Exception {
        String methodHandler = ScriptMathUtils.CLASS_NAME + ".multiply";
        verifyArguments(
                new Variable(multiplicand, "multiplicand", methodHandler),
                new Variable(multiplier, "multiplier", methodHandler));

        return multiplicand.multiply(multiplier).setScale(scale, roundMode);
    }

    /**
     * This method attempts to add a rate object by a rate object
     *
     * @param amount - the amount to be added
     * @param augend - the amount to add by
     * @return - the result of the addition as a Rate object
     * @throws Exception - if null variables are passed
     */
    public static Rate add(Rate amount, Rate augend) throws Exception {
        return new Rate(
                add(
                        new BigDecimal(amount.toString()),
                        new BigDecimal(augend.toString()),
                        5, RoundingMode.HALF_UP).toString());
    }

    /**
     * This method attempts to add a rate object by a rate object
     *
     * @param amount - the big decimal to be added
     * @param augend - the big decimal to add by
     * @param scale - the scale of the result
     * @param roundMode - the rounding mode for the result
     * @return - the result of the addition as a BigDecimal object
     * @throws Exception - if null variables are passed
     */
    public static BigDecimal add(BigDecimal amount, BigDecimal augend, int scale, RoundingMode roundMode) throws Exception {
        String methodHandler = ScriptMathUtils.CLASS_NAME + ".add";
        verifyArguments(
                new Variable(amount, "amount", methodHandler),
                new Variable(augend, "augend", methodHandler));

        return amount.add(augend).setScale(scale, roundMode);
    }
}
