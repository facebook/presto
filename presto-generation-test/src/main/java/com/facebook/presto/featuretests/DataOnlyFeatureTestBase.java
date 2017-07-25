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
package com.facebook.presto.featuretests;

import com.facebook.presto.framework.ConnectorTestFramework;
import com.facebook.presto.test.ConnectorTest;
import org.testng.annotations.Test;

import static com.facebook.presto.test.ConnectorFeature.READ_DATA;
import static com.facebook.presto.test.ConnectorFeature.WRITE_DATA;

@ConnectorTest(testName = "DataOnlyFeature", requiredFeatures = {READ_DATA})
public abstract class DataOnlyFeatureTestBase
        extends ConnectorTestFramework
{
    protected DataOnlyFeatureTestBase(ConnectorSupplier widgetSupplier, int connectorTestCount)
    {
        super(widgetSupplier, connectorTestCount);
    }

    @Test
    public void testDataOnlyFeature()
    {
        get().dataOnlyFeature();
    }

    /*
     * A test that logically belongs with the other DataOnlyFeatureTest
     * tests, but requires something in addition to what the rest of the
     * tests require.
     *
     * Annotating tests like this avoids the need for concrete subclasses
     * to @Override unsupported tests with empty implementations manually.
     */
    @Test
    @ConnectorTest(requiredFeatures = {WRITE_DATA})
    public void testWithAdditionalRestrictions()
    {
        get().dataOnlyFeature();
        get().advancedFeature();
    }
}
