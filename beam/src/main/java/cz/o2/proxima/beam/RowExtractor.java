package cz.o2.proxima.beam;

import java.io.Serializable;
import org.apache.beam.sdk.values.Row;

@FunctionalInterface
public interface RowExtractor<T> extends Serializable {

  Row extract(T message);
}
