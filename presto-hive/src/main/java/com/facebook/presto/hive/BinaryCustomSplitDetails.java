package com.facebook.presto.hive;

import static java.util.Objects.requireNonNull;

public class BinaryCustomSplitDetails
        implements CustomSplitDetails<byte[]>
{
    private final byte[] customSplitDetails;

    public BinaryCustomSplitDetails(byte[] customSplitDetails)
    {
        this.customSplitDetails = requireNonNull(customSplitDetails, "customSplitDetails is null");
    }

    @Override
    public byte[] getCustomSplitDetails()
    {
        return customSplitDetails;
    }
}
