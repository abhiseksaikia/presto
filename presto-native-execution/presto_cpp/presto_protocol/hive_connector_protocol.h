#pragma once
#include "presto_cpp/presto_protocol/ConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/hive/presto_protocol_hive.h"

namespace facebook::presto::protocol::hive {
using HiveConnectorProtocol = ConnectorProtocolTemplate<
    HiveTableHandle,
    HiveTableLayoutHandle,
    HiveColumnHandle,
    HiveInsertTableHandle,
    HiveOutputTableHandle,
    HiveSplit,
    HivePartitioningHandle,
    HiveTransactionHandle,
    HiveMetadataUpdateHandle>;
} // namespace facebook::presto::protocol