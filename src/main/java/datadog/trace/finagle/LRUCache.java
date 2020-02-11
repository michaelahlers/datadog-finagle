package datadog.trace.finagle;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Simple LRU cache with oldest-insertion semantics */
public class LRUCache<T> {
  private final int maxElements;

  private Set<T> backingSet =
      Collections.newSetFromMap(
          new LinkedHashMap<T, Boolean>() {
            protected boolean removeEldestEntry(Map.Entry<T, Boolean> eldest) {
              return size() > maxElements;
            }
          });

  public LRUCache(int maxElements) {
    this.maxElements = maxElements;
  }

  public boolean contains(T element) {
    return backingSet.contains(element);
  }

  /** @return True if the set contained the element before the call to add */
  public boolean add(T element) {
    return backingSet.add(element);
  }
}
