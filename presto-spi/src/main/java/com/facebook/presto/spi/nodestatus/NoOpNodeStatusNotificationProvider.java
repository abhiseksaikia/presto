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
package com.facebook.presto.spi.nodestatus;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NoOpNodeStatusNotificationProvider
        implements NodeStatusNotificationProvider
{
    private final ConcurrentMap<InetAddress, Set<GracefulShutdownEventListener>> remoteHostShutdownEventListeners = new ConcurrentHashMap<>();

    @Override
    public void registerGracefulShutdownEventListener(GracefulShutdownEventListener listener)
    {
    }

    @Override
    public void removeGracefulShutdownEventListener(GracefulShutdownEventListener listener)
    {
    }

    @Override
    public void registerRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener)
    {
        remoteHostShutdownEventListeners.computeIfAbsent(inetAddress, id -> new HashSet<>()).add(listener);
    }

    @Override
    public void removeRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener)
    {
        if (remoteHostShutdownEventListeners.containsKey(inetAddress)) {
            remoteHostShutdownEventListeners.get(inetAddress).remove(listener);
        }
    }
}
