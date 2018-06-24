package cz.o2.proxima.beam;

import org.apache.beam.sdk.values.Row;

public class AttachedRowExtractor<T extends RowConvertible> implements RowExtractor<T> {

  @Override
  public Row extract(T message) {
    return message.toRow();
  }
}
