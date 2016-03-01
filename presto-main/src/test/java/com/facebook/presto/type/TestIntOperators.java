/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntType.INT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestIntOperators
        extends AbstractTestFunctions
{
    @Test
    public void testiteral()
            throws Exception
    {
        assertFunction("INT'37'", INT, 37);
        assertFunction("INT'17'", INT, 17);
    }

    @Test
    public void testUnaryPlus()
            throws Exception
    {
        assertFunction("+INT'37'", INT, 37);
        assertFunction("+INT'17'", INT, 17);
    }

    @Test
    public void testUnaryMinus()
            throws Exception
    {
        assertFunction("INT'-37'", INT, -37);
        assertFunction("INT'-17'", INT, -17);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("INT'37' + INT'37'", INT, 37 + 37);
        assertFunction("INT'37' + INT'17'", INT, 37 + 17);
        assertFunction("INT'17' + INT'37'", INT, 17 + 37);
        assertFunction("INT'17' + INT'17'", INT, 17 + 17);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("INT'37' - INT'37'", INT, 0);
        assertFunction("INT'37' - INT'17'", INT, 37 - 17);
        assertFunction("INT'17' - INT'37'", INT, 17 - 37);
        assertFunction("INT'17' - INT'17'", INT, 0);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("INT'37' * INT'37'", INT, 37 * 37);
        assertFunction("INT'37' * INT'17'", INT, 37 * 17);
        assertFunction("INT'17' * INT'37'", INT, 17 * 37);
        assertFunction("INT'17' * INT'17'", INT, 17 * 17);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("INT'37' / INT'37'", INT, 1);
        assertFunction("INT'37' / INT'17'", INT, 37 / 17);
        assertFunction("INT'17' / INT'37'", INT, 17 / 37);
        assertFunction("INT'17' / INT'17'", INT, 1);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("INT'37' % INT'37'", INT, 0);
        assertFunction("INT'37' % INT'17'", INT, 37 % 17);
        assertFunction("INT'17' % INT'37'", INT, 17 % 37);
        assertFunction("INT'17' % INT'17'", INT, 0);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(INT'37')", INT, -37);
        assertFunction("-(INT'17')", INT, -17);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("INT'37' = INT'37'", BOOLEAN, true);
        assertFunction("INT'37' = INT'17'", BOOLEAN, false);
        assertFunction("INT'17' = INT'37'", BOOLEAN, false);
        assertFunction("INT'17' = INT'17'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("INT'37' <> INT'37'", BOOLEAN, false);
        assertFunction("INT'37' <> INT'17'", BOOLEAN, true);
        assertFunction("INT'17' <> INT'37'", BOOLEAN, true);
        assertFunction("INT'17' <> INT'17'", BOOLEAN, false);
    }

    @Test
    public void testessThan()
            throws Exception
    {
        assertFunction("INT'37' < INT'37'", BOOLEAN, false);
        assertFunction("INT'37' < INT'17'", BOOLEAN, false);
        assertFunction("INT'17' < INT'37'", BOOLEAN, true);
        assertFunction("INT'17' < INT'17'", BOOLEAN, false);
    }

    @Test
    public void testessThanOrEqual()
            throws Exception
    {
        assertFunction("INT'37' <= INT'37'", BOOLEAN, true);
        assertFunction("INT'37' <= INT'17'", BOOLEAN, false);
        assertFunction("INT'17' <= INT'37'", BOOLEAN, true);
        assertFunction("INT'17' <= INT'17'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("INT'37' > INT'37'", BOOLEAN, false);
        assertFunction("INT'37' > INT'17'", BOOLEAN, true);
        assertFunction("INT'17' > INT'37'", BOOLEAN, false);
        assertFunction("INT'17' > INT'17'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("INT'37' >= INT'37'", BOOLEAN, true);
        assertFunction("INT'37' >= INT'17'", BOOLEAN, true);
        assertFunction("INT'17' >= INT'37'", BOOLEAN, false);
        assertFunction("INT'17' >= INT'17'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("INT'37' BETWEEN INT'37' AND INT'37'", BOOLEAN, true);
        assertFunction("INT'37' BETWEEN INT'37' AND INT'17'", BOOLEAN, false);

        assertFunction("INT'37' BETWEEN INT'17' AND INT'37'", BOOLEAN, true);
        assertFunction("INT'37' BETWEEN INT'17' AND INT'17'", BOOLEAN, false);

        assertFunction("INT'17' BETWEEN INT'37' AND INT'37'", BOOLEAN, false);
        assertFunction("INT'17' BETWEEN INT'37' AND INT'17'", BOOLEAN, false);

        assertFunction("INT'17' BETWEEN INT'17' AND INT'37'", BOOLEAN, true);
        assertFunction("INT'17' BETWEEN INT'17' AND INT'17'", BOOLEAN, true);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(INT'37' as bigint)", BIGINT, 37L);
        assertFunction("cast(INT'17' as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(INT'37' as varchar)", VARCHAR, "37");
        assertFunction("cast(INT'17' as varchar)", VARCHAR, "17");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(INT'37' as double)", DOUBLE, 37.0);
        assertFunction("cast(INT'17' as double)", DOUBLE, 17.0);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(INT'37' as boolean)", BOOLEAN, true);
        assertFunction("cast(INT'17' as boolean)", BOOLEAN, true);
        assertFunction("cast(INT'0' as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37' as int)", INT, 37);
        assertFunction("cast('17' as int)", INT, 17);
    }
}
