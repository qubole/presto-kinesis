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
package com.qubole.presto.kinesis.decoder.json;

import static com.google.common.base.Preconditions.checkNotNull;

import com.qubole.presto.kinesis.KinesisColumnHandle;
import io.airlift.slice.Slice;

import java.util.Locale;
import java.util.Set;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.qubole.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import static com.qubole.presto.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;

public class MillisecondsSinceEpochJsonKinesisFieldDecoder
        extends JsonKinesisFieldDecoder
{
    @VisibleForTesting
    static final String NAME = "milliseconds-since-epoch";

    /**
     * Todo - configurable time zones and locales.
     */
    @VisibleForTesting
    static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTime().withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return NAME;
    }

    @Override
    public KinesisFieldValueProvider decode(JsonNode value, KinesisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        return new MillisecondsSinceEpochJsonKinesisValueProvider(value, columnHandle);
    }

    public static class MillisecondsSinceEpochJsonKinesisValueProvider
            extends JsonKinesisValueProvider
    {
        public MillisecondsSinceEpochJsonKinesisValueProvider(JsonNode value, KinesisColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
        }

        @Override
        public double getDouble()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
        }

        @Override
        public long getLong()
        {
            return isNull() ? 0L : value.asLong();
        }

        @Override
        public Slice getSlice()
        {
            return isNull() ? EMPTY_SLICE : utf8Slice(FORMATTER.print(value.asLong()));
        }
    }
}
