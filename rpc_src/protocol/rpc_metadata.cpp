#include "protocol/rpc_metadata.h"

#include <cstring>
#include <limits>
#include <string_view>

namespace rpc::protocol {
namespace {

constexpr char kMetadataMagic[] = {'R', 'M', '1', '\0'};
constexpr std::size_t kHeaderSize = 4 + 8 + 4 * 4;
constexpr std::size_t kMaxFieldSize = 64 * 1024;

void PutU32(char *output, std::uint32_t value)
{
    const std::uint32_t encoded = __builtin_bswap32(value);
    std::memcpy(output, &encoded, sizeof(encoded));
}

void PutU64(char *output, std::uint64_t value)
{
    const std::uint64_t encoded = __builtin_bswap64(value);
    std::memcpy(output, &encoded, sizeof(encoded));
}

std::uint32_t GetU32(const char *input)
{
    std::uint32_t encoded = 0;
    std::memcpy(&encoded, input, sizeof(encoded));
    return __builtin_bswap32(encoded);
}

std::uint64_t GetU64(const char *input)
{
    std::uint64_t encoded = 0;
    std::memcpy(&encoded, input, sizeof(encoded));
    return __builtin_bswap64(encoded);
}

bool CheckField(std::string_view value, std::string *error)
{
    if (value.size() <= kMaxFieldSize)
    {
        return true;
    }
    if (error != nullptr)
    {
        *error = "metadata field exceeds limit";
    }
    return false;
}

} // namespace

bool EncodeRequestMetadata(const RequestMetadata &metadata, std::string &output, std::string *error)
{
    return EncodeRequestMetadata(
        RequestMetadataView{metadata.service, metadata.method, metadata.idempotency_key,
                            metadata.application_metadata, metadata.timeout_ms},
        output, error);
}

bool EncodeRequestMetadata(const RequestMetadataView &metadata, std::string &output, std::string *error)
{
    std::size_t total = 0;
    if (!RequestMetadataEncodedSize(metadata, total, error))
    {
        return false;
    }
    output.clear();
    output.reserve(total);
    return AppendRequestMetadata(metadata, output, error);
}

bool RequestMetadataEncodedSize(const RequestMetadataView &metadata, std::size_t &size, std::string *error)
{
    if (!CheckField(metadata.service, error) || !CheckField(metadata.method, error) ||
        !CheckField(metadata.idempotency_key, error) || !CheckField(metadata.application_metadata, error))
    {
        return false;
    }
    const std::size_t total = kHeaderSize + metadata.service.size() + metadata.method.size() +
                              metadata.idempotency_key.size() + metadata.application_metadata.size();
    if (total > std::numeric_limits<std::uint32_t>::max())
    {
        if (error != nullptr)
        {
            *error = "metadata exceeds protocol limit";
        }
        return false;
    }
    size = total;
    return true;
}

bool AppendRequestMetadata(const RequestMetadataView &metadata, std::string &output, std::string *error)
{
    std::size_t ignored_size = 0;
    if (!RequestMetadataEncodedSize(metadata, ignored_size, error))
    {
        return false;
    }
    char header[kHeaderSize]{};
    std::memcpy(header, kMetadataMagic, sizeof(kMetadataMagic));
    PutU64(header + 4, metadata.timeout_ms);
    PutU32(header + 12, static_cast<std::uint32_t>(metadata.service.size()));
    PutU32(header + 16, static_cast<std::uint32_t>(metadata.method.size()));
    PutU32(header + 20, static_cast<std::uint32_t>(metadata.idempotency_key.size()));
    PutU32(header + 24, static_cast<std::uint32_t>(metadata.application_metadata.size()));
    output.append(header, sizeof(header));
    output.append(metadata.service.data(), metadata.service.size());
    output.append(metadata.method.data(), metadata.method.size());
    output.append(metadata.idempotency_key.data(), metadata.idempotency_key.size());
    output.append(metadata.application_metadata.data(), metadata.application_metadata.size());
    return true;
}

bool DecodeRequestMetadata(const std::string &input, RequestMetadata &metadata, std::string *error)
{
    RequestMetadataView view;
    if (!DecodeRequestMetadataView(input, view, error))
    {
        return false;
    }
    metadata.service.assign(view.service.data(), view.service.size());
    metadata.method.assign(view.method.data(), view.method.size());
    metadata.idempotency_key.assign(view.idempotency_key.data(), view.idempotency_key.size());
    metadata.application_metadata.assign(view.application_metadata.data(), view.application_metadata.size());
    metadata.timeout_ms = view.timeout_ms;
    return true;
}

bool DecodeRequestMetadataView(std::string_view input, RequestMetadataView &metadata, std::string *error)
{
    if (input.size() < kHeaderSize ||
        std::char_traits<char>::compare(input.data(), kMetadataMagic, sizeof(kMetadataMagic)) != 0)
    {
        if (error != nullptr)
        {
            *error = "missing RPC metadata envelope";
        }
        return false;
    }
    const char *data = input.data();
    const std::uint64_t timeout_ms = GetU64(data + 4);
    const std::uint32_t service_size = GetU32(data + 12);
    const std::uint32_t method_size = GetU32(data + 16);
    const std::uint32_t key_size = GetU32(data + 20);
    const std::uint32_t application_size = GetU32(data + 24);
    const std::size_t fields_size = static_cast<std::size_t>(service_size) + method_size + key_size + application_size;
    if (service_size > kMaxFieldSize || method_size > kMaxFieldSize || key_size > kMaxFieldSize ||
        application_size > kMaxFieldSize || fields_size > input.size() - kHeaderSize)
    {
        if (error != nullptr)
        {
            *error = "invalid RPC metadata envelope";
        }
        return false;
    }
    std::size_t offset = kHeaderSize;
    metadata.service = std::string_view(data + offset, service_size);
    offset += service_size;
    metadata.method = std::string_view(data + offset, method_size);
    offset += method_size;
    metadata.idempotency_key = std::string_view(data + offset, key_size);
    offset += key_size;
    metadata.application_metadata = std::string_view(data + offset, application_size);
    metadata.timeout_ms = timeout_ms;
    return true;
}

} // namespace rpc::protocol
