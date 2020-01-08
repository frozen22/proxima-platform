/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class JdbcDataAccessor extends AbstractStorage implements DataAccessor {
  static final String JDBC_URI_STORAGE_PREFIX = "jdbc://";
  static final String JDBC_DRIVER_CFG = "driverClassName";
  static final String JDBC_USERNAME_CFG = "username";
  static final String JDBC_PASSWORD_CFG = "password";

  private final Map<String, Object> cfg;
  private final String jdbcUri;

  private final EntityDescriptor entityDescriptor;
  private final URI uri;

  private transient HikariDataSource dataSource;

  protected JdbcDataAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = cfg;
    this.jdbcUri = uri.toString().substring(JDBC_URI_STORAGE_PREFIX.length());
    this.entityDescriptor = entityDesc;
    this.uri = uri;

    /* @TODO
    for (Map.Entry<String, Object> entry : cfg.entrySet()) {
      log.debug("Setting property {} to value {}.", entry.getKey(), entry.getValue());
      dataSourceConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
    }
     */
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(newWriter());
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.of(newRandomAccessReader());
  }

  AttributeWriterBase newWriter() {
    return new JdbcOnlineAttributeWriter(this, entityDescriptor, uri);
  }

  RandomAccessReader newRandomAccessReader() {
    return new JdbcOnlineAttributeReader(this, entityDescriptor, uri);
  }

  HikariDataSource getDataSource() {
    if (dataSource == null) {
      HikariConfig dataSourceConfig = new HikariConfig();
      dataSourceConfig.setPoolName(
          String.format("jdbc-pool-%s", this.getEntityDescriptor().getName()));
      if (cfg.containsKey(JDBC_DRIVER_CFG)) {
        dataSourceConfig.setDataSourceClassName(cfg.get(JDBC_DRIVER_CFG).toString());
      }
      log.info("Creating JDBC storage from url: {}", this.jdbcUri);
      dataSourceConfig.setJdbcUrl(this.jdbcUri);
      if (cfg.containsKey(JDBC_USERNAME_CFG)) {
        dataSourceConfig.setUsername(cfg.get(JDBC_USERNAME_CFG).toString());
      }
      if (cfg.containsKey(JDBC_PASSWORD_CFG)) {
        dataSourceConfig.setPassword(cfg.get(JDBC_PASSWORD_CFG).toString());
      }
      this.dataSource = new HikariDataSource(dataSourceConfig);
    }
    return this.dataSource;
  }
}
