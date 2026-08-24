#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace rpc::protocol {

// Transport metadata is deliberately separate from application metadata. The
// timeout is a relative budget: monotonic clocks cannot be compared across
// two hosts.
struct RequestMetadata {
    std::string service;
    std::string method;
    std::string idempotency_key;
    std::string application_metadata;
    std::uint64_t timeout_ms = 0;
};

struct RequestMetadataView {
    std::string_view service;
    std::string_view method;
    std::string_view idempotency_key;
    std::string_view application_metadata;
    std::uint64_t timeout_ms = 0;
};

bool EncodeRequestMetadata(const RequestMetadata &metadata, std::string &output, std::string *error = nullptr);
bool EncodeRequestMetadata(const RequestMetadataView &metadata, std::string &output,
                           std::string *error = nullptr);
bool RequestMetadataEncodedSize(const RequestMetadataView &metadata, std::size_t &size,
                                std::string *error = nullptr);
bool AppendRequestMetadata(const RequestMetadataView &metadata, std::string &output,
                           std::string *error = nullptr);
bool DecodeRequestMetadata(const std::string &input, RequestMetadata &metadata, std::string *error = nullptr);
bool DecodeRequestMetadataView(std::string_view input, RequestMetadataView &metadata,
                               std::string *error = nullptr);

} // namespace rpc::protocol
