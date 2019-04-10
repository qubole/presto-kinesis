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
package com.qubole.presto.kinesis.decoder.raw;

import com.qubole.presto.kinesis.KinesisColumnHandle;
import com.qubole.presto.kinesis.decoder.KinesisFieldDecoder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.qubole.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static com.qubole.presto.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;

public class RawKinesisFieldDecoder
        implements KinesisFieldDecoder<byte[]>
{
    public enum FieldType
    {
        BYTE(Byte.SIZE),
        SHORT(Short.SIZE),
        INT(Integer.SIZE),
        LONG(Long.SIZE),
        FLOAT(Float.SIZE),
        DOUBLE(Double.SIZE);

        private final int size;

        FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }

        static FieldType forString(String value)
        {
            if (value != null) {
                for (FieldType fieldType : values()) {
                    if (value.toUpperCase(Locale.ENGLISH).equals(fieldType.name())) {
                        return fieldType;
                    }
                }
            }

            return null;
        }
    }

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return RawKinesisRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KinesisFieldValueProvider decode(byte[] value, KinesisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        String mapping = columnHandle.getMapping();
        FieldType fieldType = columnHandle.getDataFormat() == null ? FieldType.BYTE : FieldType.forString(columnHandle.getDataFormat());

        int start = 0;
        int end = value.length;

        if (mapping != null) {
            List<String> fields = ImmutableList.copyOf(Splitter.on(':').limit(2).split(mapping));
            if (!fields.isEmpty()) {
                start = Integer.parseInt(fields.get(0));
                checkState(start >= 0 && start < value.length, "Found start %s, but only 0..%s is legal", start, value.length);
                if (fields.size() > 1) {
                    end = Integer.parseInt(fields.get(1));
                    checkState(end > 0 && end <= value.length, "Found end %s, but only 1..%s is legal", end, value.length);
                }
            }
        }

        checkState(start <= end, "Found start %s and end %s. start must be smaller than end", start, end);

        return new RawKinesisValueProvider(ByteBuffer.wrap(value, start, end - start), columnHandle, fieldType);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class RawKinesisValueProvider
            extends KinesisFieldValueProvider
    {
        protected final ByteBuffer value;
        protected final KinesisColumnHandle columnHandle;
        protected final FieldType fieldType;
        protected final int size;

        public RawKinesisValueProvider(ByteBuffer value, KinesisColumnHandle columnHandle, FieldType fieldType)
        {
            this.columnHandle = checkNotNull(columnHandle, "columnHandle is null");
            this.fieldType = checkNotNull(fieldType, "fieldType is null");
            this.size = value.limit() - value.position();
            checkState(size >= fieldType.getSize(), "minimum byte size is %s, found %s,", fieldType.getSize(), size);
            this.value = value;
        }

        @Override
        public final boolean accept(KinesisColumnHandle columnHandle)
        {
            return this.columnHandle.equals(columnHandle);
        }

        @Override
        public final boolean isNull()
        {
            return size == 0;
        }

        @Override
        public boolean getBoolean()
        {
            if (isNull()) {
                return false;
            }
            switch (fieldType) {
                case BYTE:
                    return value.get() != 0;
                case SHORT:
                    return value.getShort() != 0;
                case INT:
                    return value.getInt() != 0;
                case LONG:
                    return value.getLong() != 0;
                default:
                    throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, format("conversion %s to boolean not supported", fieldType));
            }
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }
            switch (fieldType) {
                case BYTE:
                    return value.get();
                case SHORT:
                    return value.getShort();
                case INT:
                    return value.getInt();
                case LONG:
                    return value.getLong();
                default:
                    throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, format("conversion %s to long not supported", fieldType));
            }
        }

        @Override
        public double getDouble()
        {
            if (isNull()) {
                return 0.0d;
            }
            switch (fieldType) {
                case FLOAT:
                    return value.getFloat();
                case DOUBLE:
                    return value.getDouble();
                default:
                    throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, format("conversion %s to double not supported", fieldType));
            }
        }

        @Override
        public Slice getSlice()
        {
            if (isNull()) {
                return Slices.EMPTY_SLICE;
            }

            if (fieldType == FieldType.BYTE) {
                return Slices.wrappedBuffer(value.slice());
            }

            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, format("conversion %s to Slice not supported", fieldType));
        }
    }
}
