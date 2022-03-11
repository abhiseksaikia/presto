package com.facebook.presto.hive.metastore;

public class BinaryCustomStorageInfo
        implements CustomStorageInfo<byte[]>
{
    @Override
    public byte[] getCustomStorageInfo()
    {
        return new byte[0];
    }
}
