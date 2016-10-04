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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;

public class TestDecimalOperators
        extends AbstractTestDecimalFunctions
{
    @Test
    public void testAdd()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '137.7' + DECIMAL '17.1'", decimal("0154.8"));
        assertDecimalFunction("DECIMAL '-1' + DECIMAL '-2'", decimal("-03"));
        assertDecimalFunction("DECIMAL '1' + DECIMAL '2'", decimal("03"));
        assertDecimalFunction("DECIMAL '.1234567890123456' + DECIMAL '.1234567890123456'", decimal("0.2469135780246912"));
        assertDecimalFunction("DECIMAL '-.1234567890123456' + DECIMAL '-.1234567890123456'", decimal("-0.2469135780246912"));
        assertDecimalFunction("DECIMAL '1234567890123456' + DECIMAL '1234567890123456'", decimal("02469135780246912"));

        // long long -> long
        assertDecimalFunction("DECIMAL '123456789012345678' + DECIMAL '123456789012345678'", decimal("0246913578024691356"));
        assertDecimalFunction("DECIMAL '.123456789012345678' + DECIMAL '.123456789012345678'", decimal("0.246913578024691356"));
        assertDecimalFunction("DECIMAL '1234567890123456789' + DECIMAL '1234567890123456789'", decimal("02469135780246913578"));
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' + DECIMAL '12345678901234567890123456789012345678'", decimal("24691357802469135780246913578024691356"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' + DECIMAL '12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999999' + DECIMAL '99999999999999999999999999999999999999'", decimal("00000000000000000000000000000000000000"));

        // short short -> long
        assertDecimalFunction("DECIMAL '99999999999999999' + DECIMAL '99999999999999999'", decimal("199999999999999998"));
        assertDecimalFunction("DECIMAL '99999999999999999' + DECIMAL '.99999999999999999'", decimal("099999999999999999.99999999999999999"));

        // long short -> long
        assertDecimalFunction("DECIMAL '123456789012345678901234567890' + DECIMAL '.12345678'", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL '.123456789012345678901234567890' + DECIMAL '12345678'", decimal("12345678.123456789012345678901234567890"));

        // short long -> long
        assertDecimalFunction("DECIMAL '.12345678' + DECIMAL '123456789012345678901234567890'", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL '12345678' + DECIMAL '.123456789012345678901234567890'", decimal("12345678.123456789012345678901234567890"));

        // overflow tests
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidCast("DECIMAL '.1' + DECIMAL '99999999999999999999999999999999999999'", "Cannot cast DECIMAL '99999999999999999999999999999999999999' to DECIMAL(38, 1)");
        assertInvalidFunction("DECIMAL '1' + DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidCast("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '.1'", "Cannot cast DECIMAL '99999999999999999999999999999999999999' to DECIMAL(38, 1)");
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' + DECIMAL '-99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '107.7' - DECIMAL '17.1'", decimal("0090.6"));
        assertDecimalFunction("DECIMAL '-1' - DECIMAL '-2'", decimal("01"));
        assertDecimalFunction("DECIMAL '1' - DECIMAL '2'", decimal("-01"));
        assertDecimalFunction("DECIMAL '.1234567890123456' - DECIMAL '.1234567890123456'", decimal("0.0000000000000000"));
        assertDecimalFunction("DECIMAL '-.1234567890123456' - DECIMAL '-.1234567890123456'", decimal("0.0000000000000000"));
        assertDecimalFunction("DECIMAL '1234567890123456' - DECIMAL '1234567890123456'", decimal("00000000000000000"));

        // long long -> long
        assertDecimalFunction("DECIMAL '1234567890123456789' - DECIMAL '1234567890123456789'", decimal("00000000000000000000"));
        assertDecimalFunction("DECIMAL '.1234567890123456789' - DECIMAL '.1234567890123456789'", decimal("0.0000000000000000000"));
        assertDecimalFunction("DECIMAL '12345678901234567890' - DECIMAL '12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' - DECIMAL '12345678901234567890123456789012345678'", decimal("00000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' - DECIMAL '12345678901234567890'", decimal("-024691357802469135780"));

        // short short -> long
        assertDecimalFunction("DECIMAL '99999999999999999' - DECIMAL '99999999999999999'", decimal("000000000000000000"));
        assertDecimalFunction("DECIMAL '99999999999999999' - DECIMAL '.99999999999999999'", decimal("099999999999999998.00000000000000001"));

        // long short -> long
        assertDecimalFunction("DECIMAL '123456789012345678901234567890' - DECIMAL '.00000001'", decimal("123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL '.000000000000000000000000000001' - DECIMAL '87654321'", decimal("-87654320.999999999999999999999999999999"));

        // short long -> long
        assertDecimalFunction("DECIMAL '.00000001' - DECIMAL '123456789012345678901234567890'", decimal("-123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL '12345678' - DECIMAL '.000000000000000000000000000001'", decimal("12345677.999999999999999999999999999999"));

        // overflow tests
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' - DECIMAL '1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidCast("DECIMAL '.1' - DECIMAL '99999999999999999999999999999999999999'", "Cannot cast DECIMAL '99999999999999999999999999999999999999' to DECIMAL(38, 1)");
        assertInvalidFunction("DECIMAL '-1' - DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidCast("DECIMAL '99999999999999999999999999999999999999' - DECIMAL '.1'", "Cannot cast DECIMAL '99999999999999999999999999999999999999' to DECIMAL(38, 1)");
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' - DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '12' * DECIMAL '3'", decimal("036"));
        assertDecimalFunction("DECIMAL '12' * DECIMAL '-3'", decimal("-036"));
        assertDecimalFunction("DECIMAL '-12' * DECIMAL '3'", decimal("-036"));
        assertDecimalFunction("DECIMAL '1234567890123456' * DECIMAL '3'", decimal("03703703670370368"));
        assertDecimalFunction("DECIMAL '.1234567890123456' * DECIMAL '3'", decimal("0.3703703670370368"));
        assertDecimalFunction("DECIMAL '.1234567890123456' * DECIMAL '.3'", decimal(".03703703670370368"));

        // short short -> long
        assertDecimalFunction("DECIMAL '12345678901234567' * DECIMAL '12345678901234567'", decimal("0152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * DECIMAL '12345678901234567'", decimal("-0152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * DECIMAL '-12345678901234567'", decimal("0152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '.12345678901234567' * DECIMAL '.12345678901234567'", decimal(".0152415787532388345526596755677489"));

        // long short -> long
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '3'", decimal("37037036703703703670370370367037037034"));
        assertDecimalFunction("DECIMAL '1234567890123456789.0123456789012345678' * DECIMAL '3'", decimal("3703703670370370367.0370370367037037034"));
        assertDecimalFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '3'", decimal(".37037036703703703670370370367037037034"));

        // short long -> long
        assertDecimalFunction("DECIMAL '3' * DECIMAL '12345678901234567890123456789012345678'", decimal("37037036703703703670370370367037037034"));
        assertDecimalFunction("DECIMAL '3' * DECIMAL '1234567890123456789.0123456789012345678'", decimal("3703703670370370367.0370370367037037034"));
        assertDecimalFunction("DECIMAL '3' * DECIMAL '.12345678901234567890123456789012345678'", decimal(".37037036703703703670370370367037037034"));

        // long long -> long
        assertDecimalFunction("CAST(3 AS DECIMAL(38,0)) * CAST(2 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000006"));
        assertDecimalFunction("CAST(3 AS DECIMAL(38,0)) * CAST(DECIMAL '0.2' AS DECIMAL(38,1))", decimal("0000000000000000000000000000000000000.6"));
        assertDecimalFunction("DECIMAL '.1234567890123456789' * DECIMAL '.1234567890123456789'", decimal(".01524157875323883675019051998750190521"));

        // scale exceeds max precision
        assertInvalidFunction("DECIMAL '.1234567890123456789' * DECIMAL '.12345678901234567890'", "DECIMAL scale must be in range [0, precision]");
        assertInvalidFunction("DECIMAL '.1' * DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL scale must be in range [0, precision]");

        // runtime overflow tests
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '-9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '-9'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '1' / DECIMAL '3'", decimal("0"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '3'", decimal("0.3"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '0.1'", decimal("10.0"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '9.0'", decimal("00.1"));
        assertDecimalFunction("DECIMAL '500.00' / DECIMAL '0.1'", decimal("5000.00"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '0.3'", decimal("0333.33"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '0.30'", decimal("00333.33"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '-0.30'", decimal("-00333.33"));
        assertDecimalFunction("DECIMAL '200.00' / DECIMAL '0.3'", decimal("0666.67"));
        assertDecimalFunction("DECIMAL '200.00000' / DECIMAL '0.3'", decimal("0666.66667"));
        assertDecimalFunction("DECIMAL '200.00000' / DECIMAL '-0.3'", decimal("-0666.66667"));
        assertDecimalFunction("DECIMAL '10' / DECIMAL '.00000001'", decimal("1000000000.00000000"));
        assertDecimalFunction("DECIMAL '99999999999999999' / DECIMAL '1'", decimal("99999999999999999"));

        // long short -> long
        assertDecimalFunction("DECIMAL '200000000000000000000000000000000000' / DECIMAL '0.30'", decimal("666666666666666666666666666666666666.67"));
        assertDecimalFunction("DECIMAL '200000000000000000000000000000000000' / DECIMAL '-0.30'", decimal("-666666666666666666666666666666666666.67"));
        assertDecimalFunction("DECIMAL '-.20000000000000000000000000000000000000' / DECIMAL '0.30'", decimal("-.66666666666666666666666666666666666667"));
        assertDecimalFunction("DECIMAL '-.20000000000000000000000000000000000000' / DECIMAL '-0.30'", decimal(".66666666666666666666666666666666666667"));
        assertDecimalFunction("DECIMAL '.20000000000000000000000000000000000000' / DECIMAL '0.30'", decimal(".66666666666666666666666666666666666667"));

        // short long -> long
        assertDecimalFunction("DECIMAL '1' / DECIMAL '.000000000000000001'", decimal("1000000000000000000.000000000000000000"));
        assertDecimalFunction("DECIMAL '-1' / DECIMAL '.000000000000000001'", decimal("-1000000000000000000.000000000000000000"));

        // long long -> long
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999999' / DECIMAL '11111111111111111111111111111111111111'", decimal("00000000000000000000000000000000000009"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999999' / DECIMAL '-11111111111111111111111111111111111111'", decimal("-00000000000000000000000000000000000009"));
        assertDecimalFunction("DECIMAL '9999999999999999999999.9' / DECIMAL '1111111111111111111111.100'", decimal("0000000000000000000000009.000"));

        // runtime overflow
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' / DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' / DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' / DECIMAL '.12345678901234567890123456789012345678'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '1' / DECIMAL '.12345678901234567890123456789012345678'", NUMERIC_VALUE_OUT_OF_RANGE);

        // division by zero tests
        assertInvalidFunction("DECIMAL '1' / DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' / DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' / DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' / DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '1' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '10' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '0' % DECIMAL '3'", decimal("0"));
        assertDecimalFunction("DECIMAL '10.0' % DECIMAL '3'", decimal("1.0"));
        assertDecimalFunction("DECIMAL '10.0' % DECIMAL '3.000'", decimal("1.000"));
        assertDecimalFunction("DECIMAL '7' % DECIMAL '3.0000000000000000'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % DECIMAL '3.0000000000000000'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % DECIMAL '3'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(17,0))", decimal("1"));
        assertDecimalFunction("DECIMAL '.1' % DECIMAL '.03'", decimal(".01"));
        assertDecimalFunction("DECIMAL '.0001' % DECIMAL '.03'", decimal(".0001"));
        assertDecimalFunction("DECIMAL '-10' % DECIMAL '3'", decimal("-1"));
        assertDecimalFunction("DECIMAL '10' % DECIMAL '-3'", decimal("1"));
        assertDecimalFunction("DECIMAL '-10' % DECIMAL '-3'", decimal("-1"));

        // short long -> short
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(38,0))", decimal("1"));
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(38,16))", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % CAST(3 AS DECIMAL(38,16))", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '-7.0000000000000000' % CAST(3 AS DECIMAL(38,16))", decimal("-1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % CAST(-3 AS DECIMAL(38,16))", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '-7.0000000000000000' % CAST(-3 AS DECIMAL(38,16))", decimal("-1.0000000000000000"));

        // short long -> long
        assertDecimalFunction("DECIMAL '7' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '.01' % DECIMAL '3.0000000000000000000000000000000000000'", decimal(".0100000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("-1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7' % DECIMAL '-3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7' % DECIMAL '-3.0000000000000000000000000000000000000'", decimal("-1.0000000000000000000000000000000000000"));

        // long short -> short
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '3.0000000000000000'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999997' % DECIMAL '3'", decimal("-1"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '-3'", decimal("1"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999997' % DECIMAL '-3'", decimal("-1"));

        // long short -> long
        assertDecimalFunction("DECIMAL '7.000000000000000000000000000000000000' % DECIMAL '3'", decimal("1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7.000000000000000000000000000000000000' % DECIMAL '3'", decimal("-1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7.000000000000000000000000000000000000' % DECIMAL '-3'", decimal("1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7.000000000000000000000000000000000000' % DECIMAL '-3'", decimal("-1.000000000000000000000000000000000000"));

        // long long -> long
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(34,0)) % CAST(3 AS DECIMAL(38,0))", decimal("0000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(34,0))", decimal("0000000000000000000000000000000001"));
        assertDecimalFunction("CAST(-7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(38,0))", decimal("-00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(-3 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(-7 AS DECIMAL(38,0)) % CAST(-3 AS DECIMAL(38,0))", decimal("-00000000000000000000000000000000000001"));

        // division by zero tests
        assertInvalidFunction("DECIMAL '1' % DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' % DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' % DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' % DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' % CAST(0 AS DECIMAL(38,0))", DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        // short
        assertDecimalFunction("-DECIMAL '1' ", decimal("-1"));
        assertDecimalFunction("-DECIMAL '-1' ", decimal("1"));
        assertDecimalFunction("-DECIMAL '123456.00000010' ", decimal("-123456.00000010"));
        assertDecimalFunction("-DECIMAL '0' ", decimal("0"));

        // long
        assertDecimalFunction("-DECIMAL '12345678901234567890123456789012345678'", decimal("-12345678901234567890123456789012345678"));
        assertDecimalFunction("-DECIMAL '-12345678901234567890123456789012345678'", decimal("12345678901234567890123456789012345678"));
        assertDecimalFunction("-DECIMAL '123456789012345678.90123456789012345678'", decimal("-123456789012345678.90123456789012345678"));
    }
}
