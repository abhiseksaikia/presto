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
package com.facebook.presto.spi;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ThriftUtils
{
    private ThriftUtils()
    {
    }

    // Serialize Optional<Duration> to Long, using null to represent absence
    public static Optional<Long> serializeOptionalDuration(Optional<Duration> optionalDuration)
    {
        return optionalDuration.map(duration -> duration.toMillis());
    }

    // Deserialize Long to Optional<Duration>, where null represents no value
    public static Optional<Duration> deserializeOptionalDuration(Optional<Long> durationMillis)
    {
        return durationMillis.map(duration -> Optional.of(new Duration(duration, TimeUnit.MILLISECONDS))).orElse(Optional.empty());
    }

    public static Optional<Double> serializeOptionalDataSize(Optional<DataSize> optionalDuration)
    {
        return optionalDuration.map(duration -> duration.getValue(DataSize.Unit.BYTE));
    }

    public static Optional<DataSize> deserializeDataSize(Optional<Double> dataSize)
    {
        return dataSize.map(size -> new DataSize(size, DataSize.Unit.BYTE));
    }
}
