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

import static com.qubole.presto.kinesis.decoder.json.RFC2822JsonKinesisFieldDecoder.FORMATTER;
import static com.qubole.presto.kinesis.decoder.util.DecoderTestUtil.checkIsNull;
import static com.qubole.presto.kinesis.decoder.util.DecoderTestUtil.checkValue;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.qubole.presto.kinesis.decoder.KinesisFieldDecoder;
import io.airlift.json.ObjectMapperProvider;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.qubole.presto.kinesis.KinesisColumnHandle;
import com.qubole.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestRFC2822JsonKinesisFieldDecoder
{
    private static final Map<String, JsonKinesisFieldDecoder> DECODERS = ImmutableMap.of(KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME, new JsonKinesisFieldDecoder(),
            RFC2822JsonKinesisFieldDecoder.NAME, new RFC2822JsonKinesisFieldDecoder());

    private static final ObjectMapperProvider PROVIDER = new ObjectMapperProvider();

    private static Map<KinesisColumnHandle, KinesisFieldDecoder<?>> map(List<KinesisColumnHandle> columns)
    {
        ImmutableMap.Builder<KinesisColumnHandle, KinesisFieldDecoder<?>> map = ImmutableMap.builder();
        for (KinesisColumnHandle column : columns) {
            map.put(column, DECODERS.get(column.getDataFormat()));
        }
        return map.build();
    }

    @Test
    public void testBasicFormatting()
            throws Exception
    {
        long now = (System.currentTimeMillis() / 1000) * 1000; // rfc2822 is second granularity
        String nowString = FORMATTER.print(now);

        byte[] json = format("{\"a_number\":%d,\"a_string\":\"%s\"}", now, nowString).getBytes(StandardCharsets.UTF_8);

        JsonKinesisRowDecoder rowDecoder = new JsonKinesisRowDecoder(PROVIDER.get());
        KinesisColumnHandle row1 = new KinesisColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME, null, false, false);
        KinesisColumnHandle row2 = new KinesisColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME, null, false, false);

        KinesisColumnHandle row3 = new KinesisColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);
        KinesisColumnHandle row4 = new KinesisColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);

        KinesisColumnHandle row5 = new KinesisColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);
        KinesisColumnHandle row6 = new KinesisColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);

        List<KinesisColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<KinesisFieldValueProvider> providers = new HashSet<>();

        boolean valid = rowDecoder.decodeRow(json, providers, columns, map(columns));
        assertTrue(valid);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        checkValue(providers, row1, now);
        checkValue(providers, row2, nowString);

        // number parsed as number --> as is
        checkValue(providers, row3, now);
        // string parsed as number --> parse text, convert to timestamp
        checkValue(providers, row4, now);

        // number parsed as string --> parse text, convert to timestamp, turn into string
        checkValue(providers, row5, Long.toString(now));

        // string parsed as string --> as is
        checkValue(providers, row6, nowString);
    }

    @Test
    public void testNullValues()
            throws Exception
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        JsonKinesisRowDecoder rowDecoder = new JsonKinesisRowDecoder(PROVIDER.get());
        KinesisColumnHandle row1 = new KinesisColumnHandle("", 0, "row1", BigintType.BIGINT, "a_number", KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME, null, false, false);
        KinesisColumnHandle row2 = new KinesisColumnHandle("", 1, "row2", VarcharType.VARCHAR, "a_string", KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME, null, false, false);

        KinesisColumnHandle row3 = new KinesisColumnHandle("", 2, "row3", BigintType.BIGINT, "a_number", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);
        KinesisColumnHandle row4 = new KinesisColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);

        KinesisColumnHandle row5 = new KinesisColumnHandle("", 4, "row5", VarcharType.VARCHAR, "a_number", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);
        KinesisColumnHandle row6 = new KinesisColumnHandle("", 5, "row6", VarcharType.VARCHAR, "a_string", RFC2822JsonKinesisFieldDecoder.NAME, null, false, false);

        List<KinesisColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6);
        Set<KinesisFieldValueProvider> providers = new HashSet<>();

        boolean valid = rowDecoder.decodeRow(json, providers, columns, map(columns));
        assertTrue(valid);

        assertEquals(providers.size(), columns.size());

        // sanity checks
        checkIsNull(providers, row1);
        checkIsNull(providers, row2);
        checkIsNull(providers, row3);
        checkIsNull(providers, row4);
        checkIsNull(providers, row5);
        checkIsNull(providers, row6);
    }
}
