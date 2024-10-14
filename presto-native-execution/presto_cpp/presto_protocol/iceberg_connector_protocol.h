#pragma once

#include "presto_cpp/presto_protocol/ConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/iceberg/presto_protocol_iceberg.h"

namespace facebook::presto::protocol::iceberg {

using IcebergConnectorProtocol = ConnectorProtocolTemplate<
    IcebergTableHandle,
    IcebergTableLayoutHandle,
    IcebergColumnHandle,
    NotImplemented,
    NotImplemented,
    IcebergSplit,
    NotImplemented,
    hive::HiveTransactionHandle,
    NotImplemented>;

} // namespace facebook::presto::protocol