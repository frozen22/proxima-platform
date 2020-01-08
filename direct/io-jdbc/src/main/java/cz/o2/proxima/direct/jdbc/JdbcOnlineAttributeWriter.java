/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class JdbcOnlineAttributeWriter extends AbstractOnlineAttributeWriter {
  private final JdbcDataAccessor accessor;

  protected JdbcOnlineAttributeWriter(
      JdbcDataAccessor accessor, EntityDescriptor entityDesc, URI uri) {
    super(entityDesc, uri);
    this.accessor = accessor;
  }

  @Override
  public void close() {
    //accessor.getDataSource().close();
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    HikariDataSource dataSource = accessor.getDataSource();
    try (PreparedStatement stmt =
        dataSource
            .getConnection()
            .prepareStatement("INSERT INTO DUMMYTABLE (id, attribute) VALUES(?,?)")) {
      stmt.setString(1, data.getKey());
      if (!data.isDelete() && data.getValue() != null) {
        stmt.setString(2, new String(data.getValue()));
        log.debug("Execute statement {}", stmt);
        stmt.executeUpdate();
        statusCallback.commit(true, null);
      } else {
        // @TODO
        statusCallback.commit(false, new UnsupportedOperationException("Unknown"));
      }
    } catch (SQLException e) {
      log.error(e.getMessage(), e);
      statusCallback.commit(false, e);
    }
  }
}
