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
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface NDVColumnStats extends ColumnStats {

  @JsonProperty("numDVs")
  long numDVs();

  @Value.Default
  @JsonProperty("bitVector")
  default byte[] bitVector() {
    return EMPTY_HLL;
  }

  @Value.Lazy
  @JsonIgnore
  @Value.Auxiliary
  default Optional<NumDistinctValueEstimator> getNDVEstimator() {
    if (bitVector() != null && !Arrays.equals(bitVector(), EMPTY_HLL)) {
      return Optional.of(NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(bitVector()));
    }
    return Optional.empty();
  }

  @JsonIgnore
  default long mergeNDVs(NDVColumnStats o) {
    return Math.max(this.numDVs(), o.numDVs());
  }

  @JsonIgnore
  default Optional<NumDistinctValueEstimator> getMergedBitVector(NDVColumnStats o) {
    Optional<NumDistinctValueEstimator> thisEstimatorOptional = getNDVEstimator();
    Optional<NumDistinctValueEstimator> otherEstimatorOptional = o.getNDVEstimator();

    if (thisEstimatorOptional.isPresent() && otherEstimatorOptional.isPresent()) {
      final long ndv;
      NumDistinctValueEstimator oldEst = thisEstimatorOptional.get();
      NumDistinctValueEstimator newEst = otherEstimatorOptional.get();
      if (oldEst.canMerge(newEst)) {
        NumDistinctValueEstimator mergedEst = NumDistinctValueEstimatorFactory.getEmptyNumDistinctValueEstimator(oldEst);
        mergedEst.mergeEstimators(oldEst);
        mergedEst.mergeEstimators(newEst);
        return Optional.of(mergedEst);
      }
      // if they can't be merged, the current behaviour is to keep the first estimator
      return Optional.of(oldEst);
    }

    return Stream.of(thisEstimatorOptional, otherEstimatorOptional)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }
}
