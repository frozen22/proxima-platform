/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.tools.groovy.internal.ClassloaderUtils;
import groovy.lang.GroovyClassLoader;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.control.SourceUnit;

/** {@link GroovyClassLoader} keeping track of generated bytecode. */
public class ToolsClassLoader extends GroovyClassLoader {

  class Collector extends ClassCollector {

    Collector(InnerLoader cl, CompilationUnit unit, SourceUnit su) {
      super(cl, unit, su);
    }

    @Override
    protected Class createClass(byte[] code, ClassNode classNode) {
      bytecodes.put(classNode.getName(), code);
      return super.createClass(code, classNode);
    }
  }

  ToolsClassLoader() {
    super(Thread.currentThread().getContextClassLoader(), ClassloaderUtils.createConfiguration());
  }

  final Map<String, byte[]> bytecodes = new ConcurrentHashMap<>();

  @Override
  protected ClassCollector createCollector(CompilationUnit unit, SourceUnit su) {
    ClassCollector collector = super.createCollector(unit, su);
    return new Collector((InnerLoader) collector.getDefiningClassLoader(), unit, su);
  }

  public Set<String> getDefinedClasses() {
    return bytecodes.keySet().stream().collect(Collectors.toSet());
  }

  public byte[] getClassByteCode(String name) {
    return Objects.requireNonNull(bytecodes.get(name));
  }
}
