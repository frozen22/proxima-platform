package cz.o2.proxima.beam;

import org.apache.beam.sdk.values.Row;

public interface RowConvertible {

  Row toRow();
}
