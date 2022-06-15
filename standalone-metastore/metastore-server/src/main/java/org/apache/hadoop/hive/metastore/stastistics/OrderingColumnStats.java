/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.stastistics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public abstract class OrderingColumnStats extends AbstractColumnStats {

  private static byte[] EMPTY_HLL = new byte[] {'H', 'L', 'L'};

  @JsonProperty("numDVs")
  public abstract long numDVs();

  @Value.Default
  @JsonProperty("bitVector")
  public byte[] bitVector() {
    return EMPTY_HLL;
  }

  protected <T> Optional<T> mergeLowValues (Optional<T> low1, Optional<T> low2, Comparator<T> comparator) {
    return Stream.of(low1, low2)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .min(comparator);
  }

  protected <T> Optional<T> mergeHighValues(Optional<T> high1, Optional<T> high2, Comparator<T> comparator) {
    return Stream.of(high1, high2)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .max(comparator);
  }

  @JsonIgnore
  protected long mergeNDVs(OrderingColumnStats o) {
    return Math.max(this.numDVs(), o.numDVs());
  }

  @JsonIgnore
  public static Optional<NumDistinctValueEstimator> getMergedBitVector(byte[] bv1, byte[] bv2) {
    if (bv1 != null && !Arrays.equals(bv1, EMPTY_HLL) && bv2 != null && !Arrays.equals(bv2, EMPTY_HLL)) {
      NumDistinctValueEstimator oldEst = NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(bv1);
      NumDistinctValueEstimator newEst = NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(bv2);
      final long ndv;
      if (oldEst.canMerge(newEst)) {
        oldEst.mergeEstimators(newEst);
        return Optional.of(oldEst);
      }
    }
    return Optional.empty();
  }
}
