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

import com.facebook.presto.spi.ConnectorSession;

/**
 * Define session variables supported in the connector and an accessor.
 *
 * Note that we default these properties to what is in the configuration,
 * so there should always be a value.
 *
 * Created by derekbennett on 6/23/16.
 */
public class SessionVariables
{
    private SessionVariables() {}

    public static final String ITERATION_NUMBER = "iteration_number"; // int
    public static final String CHECKPOINT_LOGICAL_NAME = "checkpoint_logical_name"; // string
    public static final String MAX_BATCHES = "max_batches"; // int
    public static final String BATCH_SIZE = "batch_size"; // int

    public static int getBatchSize(ConnectorSession session)
    {
        int value = session.getProperty(BATCH_SIZE, Integer.class);
        return value;
    }

    public static int getMaxBatches(ConnectorSession session)
    {
        int value = session.getProperty(MAX_BATCHES, Integer.class);
        return value;
    }

    public static String getSessionProperty(ConnectorSession session, String key)
    {
        String value = session.getProperty(key, String.class);
        if (value == null) {
            return "";
        }
        else {
            return value;
        }
    }
}
