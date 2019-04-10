/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubole.presto.kinesis.decoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.qubole.presto.kinesis.KinesisColumnHandle;
import com.qubole.presto.kinesis.KinesisFieldValueProvider;

public interface KinesisRowDecoder
{
    /**
     * Returns the row decoder specific name.
     */
    String getName();

    /**
     * Decodes a given set of bytes into field values.
     *
     * @param data The row data (Kinesis message) to decode.
     * @param fieldValueProviders Must be a mutable set. Any field value provider created by this row decoder is put into this set.
     * @param columnHandles List of column handles for which field values are required.
     * @param fieldDecoders Map from column handles to decoders. This map should be used to look up the field decoder that generates the field value provider for a given column handle.
     * @return true if the row was decoded successfully, false if it could not be decoded (was corrupt). TODO - reverse this boolean.
     */
    boolean decodeRow(
            byte[] data,
            Set<KinesisFieldValueProvider> fieldValueProviders,
            List<KinesisColumnHandle> columnHandles,
            Map<KinesisColumnHandle, KinesisFieldDecoder<?>> fieldDecoders);
}
