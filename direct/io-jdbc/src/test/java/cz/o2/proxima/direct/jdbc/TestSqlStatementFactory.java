package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class TestSqlStatementFactory implements SqlStatementFactory {
  public final void setup(EntityDescriptor entity, URI uri) {

  }
  public PreparedStatement createGetStatement(HikariDataSource dataSource, String attribute, AttributeDescriptor<?> desc) {
    try {
      return dataSource.getConnection().prepareStatement(String.format("SELECT %s FROM DUMMYTABLE WHERE id = ? LIMIT 1", attribute));
    } catch (SQLException e) {
      log.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }
  public PreparedStatement createInsertStatement(HikariDataSource dataSource, StreamElement element) {

  }
  public PreparedStatement createDeleteStatement(HikariDataSource dataSource, StreamElement element) {

  }
  public PreparedStatement createDeleteWildcardStatement(HikariDataSource dataSource, StreamElement element) {

  }
}
