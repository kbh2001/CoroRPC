#include "protocol/frame.h"
#include "protocol/rpc_metadata.h"

#include <iostream>
#include <cstring>
#include <string>

namespace {

bool CheckCase(const rpc::protocol::RequestMetadataView &metadata, std::string_view body,
               std::uint64_t request_id)
{
    std::string encoded_metadata;
    if (!rpc::protocol::EncodeRequestMetadata(metadata, encoded_metadata))
    {
        return false;
    }

    rpc::protocol::Frame frame;
    frame.header.message_type = rpc::protocol::MessageType::REQUEST;
    frame.header.request_id = request_id;
    frame.metadata = encoded_metadata;
    frame.body.assign(body.data(), body.size());

    std::string generic;
    std::string direct;
    if (!rpc::protocol::FrameEncoder::Encode(frame, generic) ||
        !rpc::protocol::FrameEncoder::EncodeRequest(metadata, body, request_id, direct) ||
        generic != direct)
    {
        return false;
    }

    rpc::protocol::FrameDecoder decoder;
    rpc::protocol::FrameView decoded;
    const std::size_t split = direct.size() > rpc::protocol::kFixedHeaderSize + 1
                                  ? rpc::protocol::kFixedHeaderSize + 1
                                  : direct.size();
    if (decoder.Feed(direct.data(), split) == rpc::protocol::DecodeStatus::PROTOCOL_ERROR ||
        decoder.NextView(decoded) != rpc::protocol::DecodeStatus::NEED_MORE ||
        decoder.Feed(direct.data() + split, direct.size() - split) ==
            rpc::protocol::DecodeStatus::PROTOCOL_ERROR ||
        decoder.NextView(decoded) != rpc::protocol::DecodeStatus::FRAME_READY)
    {
        return false;
    }

    rpc::protocol::RequestMetadataView decoded_metadata;
    std::string reencoded;
    if (!rpc::protocol::DecodeRequestMetadataView(decoded.metadata, decoded_metadata) ||
        decoded.header.request_id != request_id || decoded.body != body ||
        decoded_metadata.service != metadata.service || decoded_metadata.method != metadata.method ||
        decoded_metadata.idempotency_key != metadata.idempotency_key ||
        decoded_metadata.application_metadata != metadata.application_metadata ||
        decoded_metadata.timeout_ms != metadata.timeout_ms ||
        !rpc::protocol::FrameEncoder::Encode(decoded, reencoded) || reencoded != direct)
    {
        return false;
    }

    rpc::protocol::FrameDecoder owning_decoder;
    rpc::protocol::Frame owning_frame;
    if (owning_decoder.Feed(direct.data(), direct.size()) == rpc::protocol::DecodeStatus::PROTOCOL_ERROR ||
        owning_decoder.Next(owning_frame) != rpc::protocol::DecodeStatus::FRAME_READY ||
        owning_frame.header.request_id != request_id || owning_frame.body != body)
    {
        return false;
    }

    rpc::protocol::FrameDecoder direct_decoder;
    char *writable = direct_decoder.PrepareWritable(direct.size());
    if (writable == nullptr)
    {
        return false;
    }
    std::memcpy(writable, direct.data(), direct.size());
    rpc::protocol::FrameView direct_view;
    return direct_decoder.CommitWritable(direct.size()) != rpc::protocol::DecodeStatus::PROTOCOL_ERROR &&
           direct_decoder.NextView(direct_view) == rpc::protocol::DecodeStatus::FRAME_READY &&
           direct_view.header.request_id == request_id && direct_view.body == body;
}

bool CheckViewStableAcrossBatch()
{
    rpc::protocol::Frame first;
    first.header.message_type = rpc::protocol::MessageType::RESPONSE;
    first.header.request_id = 1;
    first.body = "first-response";

    rpc::protocol::Frame second;
    second.header.message_type = rpc::protocol::MessageType::RESPONSE;
    second.header.request_id = 2;
    second.body = "second-respons";

    std::string first_bytes;
    std::string second_bytes;
    if (!rpc::protocol::FrameEncoder::Encode(first, first_bytes) ||
        !rpc::protocol::FrameEncoder::Encode(second, second_bytes) ||
        first_bytes.size() != second_bytes.size())
    {
        return false;
    }

    const std::string batch = first_bytes + second_bytes;
    rpc::protocol::FrameDecoder decoder;
    char *writable = decoder.PrepareWritable(batch.size());
    if (writable == nullptr)
    {
        return false;
    }
    std::memcpy(writable, batch.data(), batch.size());
    if (decoder.CommitWritable(batch.size()) == rpc::protocol::DecodeStatus::PROTOCOL_ERROR)
    {
        return false;
    }

    rpc::protocol::FrameView first_view;
    rpc::protocol::FrameView second_view;
    if (decoder.NextView(first_view) != rpc::protocol::DecodeStatus::FRAME_READY ||
        first_view.body != first.body ||
        decoder.NextView(second_view) != rpc::protocol::DecodeStatus::FRAME_READY)
    {
        return false;
    }
    return first_view.body == first.body && second_view.body == second.body;
}

} // namespace

int main()
{
    const std::string binary_body("ab\0cd", 5);
    const bool ok =
        CheckCase({"EchoService", "Echo", {}, {}, 5000}, "test", 1) &&
        CheckCase({"LongServiceNameForHeapStorage", "Method", "key", "application", 17},
                  binary_body, 0x1020304050607080ULL) &&
        CheckViewStableAcrossBatch();
    std::cout << (ok ? "protocol fast path: PASS\n" : "protocol fast path: FAIL\n");
    return ok ? 0 : 1;
}
