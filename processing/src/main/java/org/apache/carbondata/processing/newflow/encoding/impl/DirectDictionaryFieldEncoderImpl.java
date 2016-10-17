/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

public class DirectDictionaryFieldEncoderImpl extends AbstractDictionaryFieldEncoderImpl {

  private DirectDictionaryGenerator directDictionaryGenerator;

  private int index;

  public DirectDictionaryFieldEncoderImpl(DataField dataField, int index) {
    DirectDictionaryGenerator directDictionaryGenerator =
        DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dataField.getColumn().getDataType());
    this.directDictionaryGenerator = directDictionaryGenerator;
    this.index = index;
  }

  @Override public Integer encode(CarbonRow row) {
    return directDictionaryGenerator.generateDirectSurrogateKey(row.getString(index));
  }

  @Override public int getColumnCardinality() {
    return Integer.MAX_VALUE;
  }
}
