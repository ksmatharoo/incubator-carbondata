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

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;
import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;

import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

/**
 * Utils
 *
 * @since 2020-03-18
 */
public class Utils
{
    private static final Logger LOG = Logger.get(Utils.class);

    private Utils() {}

    /**
     * Whether sql constraint contains conditions like "rowKey='xxx'" or "rowKey in ('xxx','xxx')"
     *
     * @param tupleDomain TupleDomain
     * @return true if this sql is batch get.
     */
    public static boolean isBatchGet(TupleDomain<HiveColumnHandle> tupleDomain, HbaseColumn[] rowIds)
    {
        String[] rowIdNames = Arrays.stream(rowIds).map(HbaseColumn::getColName).toArray(String[]::new);
        if (tupleDomain != null && tupleDomain.getDomains().isPresent()) {
            Map<HiveColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            HiveColumnHandle[] columnHandle =
                    domains.keySet().stream()
                            .filter((key -> Utils.contains(key.getName(), rowIdNames))).toArray(HiveColumnHandle[]::new);

            if (columnHandle.length == rowIds.length) {
                for (HiveColumnHandle handle : columnHandle) {
                    for (Range range : domains.get(handle).getValues().getRanges().getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * createTypeByName
     *
     * @param type String
     * @return Type
     */
    public static Type createTypeByName(String type)
    {
        Type result = null;
        try {
            Class clazz = Class.forName(type);
            Field[] fields = clazz.getFields();

            for (Field field : fields) {
                if (type.equals(field.getType().getName())) {
                    Object object = field.get(clazz);
                    if (object instanceof Type) {
                        return (Type) object;
                    }
                }
            }
        }
        catch (ClassNotFoundException e) {
            LOG.error("createTypeByName failed... cause by : %s", e);
        }
        catch (IllegalAccessException e) {
            LOG.error("createTypeByName failed... cause by : %s", e);
        }

        return Optional.ofNullable(result).orElse(result);
    }

    public static HbaseColumn getHbaseColumn(HbaseCarbonTable table, HiveColumnHandle handle){
        return table.getsMap().get(handle.getName());
    }

    public static boolean containsAll(HbaseCarbonTable table, List<HiveColumnHandle> handles, String[] rowKeys){
        for (int i = 0; i < handles.size(); i++) {
            if(!handles.get(i).getName().equalsIgnoreCase(rowKeys[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean contains(String colName,  String[] vals) {
        for (int i = 0; i < vals.length; i++) {
            if (vals[i].equals(colName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * check file exist
     *
     * @param path file path
     * @return true/false
     */
    public static boolean isFileExist(String path)
    {
        File file = new File(path);
        return file.exists();
    }
}
