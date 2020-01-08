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

import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class JdbcDataAccessorTest {
  final Repository repository =
      ConfigRepository.Builder.ofTest(ConfigFactory.defaultApplication()).build();

  final AttributeDescriptor<Byte[]> attr;
  final EntityDescriptor entity;

  final Map<String, Object> config;

  final JdbcDataAccessor accessor;

  public JdbcDataAccessorTest() throws URISyntaxException {
    attr =
        AttributeDescriptor.newBuilder(repository)
            .setEntity("dummy")
            .setName("attribute")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    entity = EntityDescriptor.newBuilder().setName("dummy").addAttribute(attr).build();
    config = new HashMap<>();
    // config.put(JdbcDataAccessor.JDBC_DRIVER_CFG, "org.hsqldb.jdbc.JDBCDataSource");
    config.put(JdbcDataAccessor.JDBC_USERNAME_CFG, "SA");
    accessor =
        new JdbcDataAccessor(
            entity,
            URI.create(JdbcDataAccessor.JDBC_URI_STORAGE_PREFIX + "jdbc:hsqldb:mem:testdb"),
            config);
    config.put(JdbcDataAccessor.JDBC_PASSWORD_CFG, "");
  }

  @Before
  public void setup() throws SQLException {
    HikariDataSource dataSource = accessor.getDataSource();
    Statement statement = dataSource.getConnection().createStatement();
    final String createTableSql =
        "CREATE TABLE DUMMYTABLE (id VARCHAR(255) NOT NULL, attribute VARCHAR(255), PRIMARY KEY (id) )";
    statement.execute(createTableSql);
    log.info("Table created.");
    statement.close();
  }

  @After
  public void cleanup() throws SQLException {
    HikariDataSource dataSource = accessor.getDataSource();
    Statement statement = dataSource.getConnection().createStatement();
    final String sql = "DROP TABLE DUMMYTABLE";
    statement.execute(sql);
    statement.close();
  }

  @Test
  public void serializableTest() throws IOException {
    TestUtils.assertSerializable(accessor);
    TestUtils.assertSerializable(accessor.newWriter());
    TestUtils.assertSerializable(accessor.newRandomAccessReader());
  }

  @Test
  public void writeSuccessfullyTest() {
    StreamElement element =
        StreamElement.update(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "12345",
            attr.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    assertTrue(writeElement(accessor,element).get());
  }

  @Test
  public void readSuccessfullyTest() {
    StreamElement element =
        StreamElement.update(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "12345",
            attr.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    assertTrue(writeElement(accessor,element).get());

    Optional<KeyValue<Byte[]>> keyValue = accessor.newRandomAccessReader().get("12345", attr);
    assertTrue(keyValue.isPresent());
    log.debug("KV: {}", keyValue.get());
    assertEquals(attr, keyValue.get().getAttrDescriptor());
    assertEquals("value", new String(keyValue.get().getValueBytes()));


  }


  private AtomicBoolean writeElement(JdbcDataAccessor accessor, StreamElement element) {
    try (AttributeWriterBase writer = accessor.newWriter()) {
      AtomicBoolean success = new AtomicBoolean(false);
      writer
          .online()
          .write(
              element,
              (status, exc) -> {
                success.set(status);
              });
      return success;
    }
  }
}
