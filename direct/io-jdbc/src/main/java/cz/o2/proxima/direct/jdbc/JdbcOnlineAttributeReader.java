package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.util.Pair;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Slf4j
public class JdbcOnlineAttributeReader extends AbstractStorage implements RandomAccessReader {

  private final JdbcDataAccessor accessor;

  public JdbcOnlineAttributeReader(
      JdbcDataAccessor accessor, EntityDescriptor entityDesc, URI uri) {
    super(entityDesc, uri);
    this.accessor = accessor;
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    return null;
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {
    HikariDataSource dataSource = accessor.getDataSource();
    try {
      try (PreparedStatement stmt =
          dataSource.getConnection().prepareStatement("SELECT id, attribute FROM DUMMYTABLE where id = ? LIMIT 1")) {
        stmt.setString(1, key);
        log.debug("Execute statement {}", stmt);
        try (ResultSet resultSet = stmt.executeQuery()) {
          resultSet.next();//TODO
          return Optional.of(
              KeyValue.of(
                  getEntityDescriptor(),
                  desc,
                  key,
                  desc.getName(),
                  new Offsets.Raw(key),
                  resultSet.getString("attribute").getBytes()));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Optional.empty();
  }

  @Override
  public void scanWildcardAll(
      String key,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<?>> consumer) {}

  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {}

  @Override
  public void listEntities(
      @Nullable RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {}

  @Override
  public void close() throws IOException {}
}
