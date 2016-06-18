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
package com.facebook.presto.kinesis;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;

/**
 *  Kinesis connector specific error codes.
 */
public enum KinesisErrorCode
        implements ErrorCodeSupplier
{
    /**
     * A requested data conversion is not supported.
     */
    KINESIS_CONVERSION_NOT_SUPPORTED(0),
    KINESIS_SPLIT_ERROR(1);

    // Connectors can use error codes starting at EXTERNAL
    public static final int StartingErrorCode = 0x0200_0000;

    private final ErrorCode errorCode;

    KinesisErrorCode(int code)
    {
        errorCode = new ErrorCode(code + StartingErrorCode, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
