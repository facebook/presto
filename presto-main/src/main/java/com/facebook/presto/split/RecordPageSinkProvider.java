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
package com.facebook.presto.split;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordPageSink;
import com.facebook.presto.spi.TransactionalConnectorPageSinkProvider;
import com.facebook.presto.spi.TransactionalConnectorRecordSinkProvider;
import com.facebook.presto.spi.transaction.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class RecordPageSinkProvider
        implements TransactionalConnectorPageSinkProvider
{
    private final TransactionalConnectorRecordSinkProvider recordSinkProvider;

    public RecordPageSinkProvider(TransactionalConnectorRecordSinkProvider recordSinkProvider)
    {
        this.recordSinkProvider = requireNonNull(recordSinkProvider, "recordSinkProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return new RecordPageSink(recordSinkProvider.getRecordSink(transactionHandle, session, outputTableHandle));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return new RecordPageSink(recordSinkProvider.getRecordSink(transactionHandle, session, insertTableHandle));
    }
}
