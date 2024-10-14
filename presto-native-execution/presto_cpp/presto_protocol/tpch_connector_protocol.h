#pragma once

#include "presto_cpp/presto_protocol/ConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/tpch/presto_protocol_tpch.h"

namespace facebook::presto::protocol::tpch {

using TpchConnectorProtocol = ConnectorProtocolTemplate<
    TpchTableHandle,
    TpchTableLayoutHandle,
    TpchColumnHandle,
    NotImplemented,
    NotImplemented,
    TpchSplit,
    TpchPartitioningHandle,
    TpchTransactionHandle,
    NotImplemented>;

} // namespace facebook::presto::protocol