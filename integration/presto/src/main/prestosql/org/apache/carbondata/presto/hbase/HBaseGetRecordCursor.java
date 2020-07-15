/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package org.apache.carbondata.presto.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;
import org.apache.carbondata.presto.hbase.serializers.HBaseRowSerializer;
import org.apache.carbondata.presto.hbase.split.HBaseSplit;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import static java.util.Objects.requireNonNull;

/**
 * HBaseGetRecordCursor
 *
 * @since 2020-03-18
 */
public class HBaseGetRecordCursor
        extends HBaseRecordCursor
{
    private static final Logger LOG = Logger.get(HBaseGetRecordCursor.class);

    private Connection conn;

    private Result[] results;

    private int currentRecordIndex;

    public HBaseGetRecordCursor(
            List<HiveColumnHandle> columnHandles,
            HBaseSplit hBaseSplit,
            Connection connection,
            HBaseRowSerializer serializer,
            List<Type> columnTypes,
            HbaseColumn rowIdName,
            String[] fieldToColumnName,
            String defaultValue)
    {
        super(columnHandles, columnTypes, serializer, rowIdName, fieldToColumnName, defaultValue, hBaseSplit);
        startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;

        this.rowIdName = rowIdName;

        this.split = hBaseSplit;
        this.conn = connection;
        try (Table table =
                connection.getTable(TableName.valueOf(split.getTable().getNamespace(), split.getTable().getName()))) {
            List<Object> rowKeys = new ArrayList<>();
            Type type = null;
            if (hBaseSplit.getRanges().containsKey(hBaseSplit.getTable().getRow().getHbaseColumns()[0].getColName())) {
                for (Range range : hBaseSplit.getRanges().get(hBaseSplit.getTable().getRow().getHbaseColumns()[0].getColName())) {
                    type = range.getType();
                    Object object = range.getSingleValue();
                    if (object instanceof Slice) {
                        rowKeys.add( ((Slice) object).getBase());
                    }
                    else {
                        rowKeys.add(range.getSingleValue());
                    }
                }
            }

            this.results = getResults(rowKeys, table, type);
        }
        catch (IOException e) {
            LOG.error(e, e.getMessage());
            this.close();
        }
        this.bytesRead = 0L;
    }

    private Result[] getResults(List<Object> rowKeys, Table table, Type type) {
        List<Get> gets =
                rowKeys.stream()
                        .map(
                                rowKey -> {
                                    Get get = new Get(serializer.setObjectBytes(type, rowKey));
                                    for (HiveColumnHandle hch : columnHandles) {
                                            if (!this.rowIdName.getColName().equals(hch.getName())) {
                                                get.addColumn(
                                                        Bytes.toBytes(Utils.getHbaseColumn(split.getTable(), hch).getCf()),
                                                        Bytes.toBytes(hch.getName()));
                                            }
                                    }
                                    try {
                                        get.setTimeRange(split.getTimestamp(), Long.MAX_VALUE);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return get;
                                })
                        .collect(Collectors.toList());

        try {
            return table.get(gets);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return new Result[0];
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (this.currentRecordIndex >= this.results.length) {
            return false;
        }
        else {
            Result record = this.results[this.currentRecordIndex];
            serializer.reset();
            if (record.getRow() != null) {
                serializer.deserialize(record, this.defaultValue, split.getTable());
            }
            this.currentRecordIndex++;
            return true;
        }
    }

    @Override
    public void close() {}
}
