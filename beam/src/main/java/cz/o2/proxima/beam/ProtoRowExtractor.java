package cz.o2.proxima.beam;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

@NotThreadSafe
public class ProtoRowExtractor<T extends Message> implements RowExtractor<T> {

  @Override
  public Row extract(T message) {
    return null;
  }

  private RowType buildType(Set<Descriptors.FieldDescriptor> fieldSet) {

    final Descriptors.FieldDescriptor[] fields =
        (Descriptors.FieldDescriptor[]) fieldSet.toArray();

    Arrays.sort(fields, Comparator.comparing(Descriptors.FieldDescriptor::getName));

    final RowSqlType.Builder typeBuilder = RowSqlType.builder();

    for (Descriptors.FieldDescriptor desc : fields) {
      switch (desc.getJavaType()) {
        case INT:
          typeBuilder.withIntegerField(desc.getName());
          break;
//              case ENUM:
//              case LONG:
        case FLOAT:
          typeBuilder.withFloatField(desc.getName());
          break;
        case DOUBLE:
          typeBuilder.withDoubleField(desc.getName());
          break;
        case STRING:
          typeBuilder.withVarcharField(desc.getName());
          break;
        case BOOLEAN:
          typeBuilder.withBooleanField(desc.getName());
          break;
//              case MESSAGE:
//              case BYTE_STRING:
//                break;
        default:
          throw new IllegalArgumentException(
              "Unsupported java type '" + desc.getJavaType() + "'.");
      }
    }

    return typeBuilder.build();
  }
}
