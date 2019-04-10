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
package com.qubole.presto.kinesis;

import io.airlift.slice.Slice;

import com.facebook.presto.spi.PrestoException;

import static com.qubole.presto.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;

public abstract class KinesisFieldValueProvider
{
    public abstract boolean accept(KinesisColumnHandle columnHanle);

    public boolean getBoolean()
    {
        throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
    }

    public long getLong()
    {
        throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to long not supported");
    }

    public double getDouble()
    {
        throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
    }

    public Slice getSlice()
    {
        throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to Slice not supported");
    }

    public abstract boolean isNull();
}
