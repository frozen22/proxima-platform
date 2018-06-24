/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.beam;

import java.io.Serializable;
import lombok.Value;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

@Value
public class Event implements Serializable, RowConvertible {

  public static final RowType ROW_TYPE = RowSqlType.builder()
      .withVarcharField("name")
      .withBooleanField("important")
      .build();

  private final String name;

  private final boolean important;

  @Override
  public Row toRow() {
    return Row.withRowType(ROW_TYPE)
        .addValues(name, important)
        .build();
  }
}
