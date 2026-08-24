#include "protocol/frame.h"
#include "protocol/rpc_metadata.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

namespace rpc::protocol {
namespace {

void PutU16(char *out, std::uint16_t value)
{
    const std::uint16_t encoded = __builtin_bswap16(value);
    std::memcpy(out, &encoded, sizeof(encoded));
}

void PutU32(char *out, std::uint32_t value)
{
    const std::uint32_t encoded = __builtin_bswap32(value);
    std::memcpy(out, &encoded, sizeof(encoded));
}

void PutU64(char *out, std::uint64_t value)
{
    const std::uint64_t encoded = __builtin_bswap64(value);
    std::memcpy(out, &encoded, sizeof(encoded));
}

void BuildHeader(char *output, std::uint32_t flags, MessageType type, std::uint64_t request_id,
                 std::uint32_t metadata_size, std::uint32_t body_size)
{
    PutU32(output, kMagic);
    PutU16(output + 4, kVersion);
    PutU16(output + 6, kFixedHeaderSize);
    PutU32(output + 8, flags);
    output[12] = static_cast<char>(type);
    output[13] = 0;
    output[14] = 0;
    output[15] = 0;
    PutU64(output + 16, request_id);
    PutU32(output + 24, metadata_size);
    PutU32(output + 28, body_size);
}

std::uint16_t GetU16(const char *data)
{
    std::uint16_t encoded = 0;
    std::memcpy(&encoded, data, sizeof(encoded));
    return __builtin_bswap16(encoded);
}

std::uint32_t GetU32(const char *data)
{
    std::uint32_t encoded = 0;
    std::memcpy(&encoded, data, sizeof(encoded));
    return __builtin_bswap32(encoded);
}

std::uint64_t GetU64(const char *data)
{
    std::uint64_t encoded = 0;
    std::memcpy(&encoded, data, sizeof(encoded));
    return __builtin_bswap64(encoded);
}

bool IsValidMessageType(std::uint8_t value)
{
    return value >= static_cast<std::uint8_t>(MessageType::REQUEST) &&
           value <= static_cast<std::uint8_t>(MessageType::PONG);
}

void SetError(std::string *error, const char *message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

} // namespace

bool FrameEncoder::Encode(const Frame &frame, std::string &output, std::string *error)
{
    return Encode(FrameView{frame.header, frame.metadata, frame.body}, output, error);
}

bool FrameEncoder::Encode(const FrameView &frame, std::string &output, std::string *error)
{
    if (frame.metadata.size() > std::numeric_limits<std::uint32_t>::max() ||
        frame.body.size() > std::numeric_limits<std::uint32_t>::max())
    {
        SetError(error, "frame field exceeds uint32 length");
        return false;
    }
    const std::uint8_t type = static_cast<std::uint8_t>(frame.header.message_type);
    if (!IsValidMessageType(type))
    {
        SetError(error, "invalid frame enum");
        return false;
    }
    if ((frame.header.message_type == MessageType::REQUEST || frame.header.message_type == MessageType::RESPONSE) &&
        frame.header.request_id == 0)
    {
        SetError(error, "request and response frames require a request id");
        return false;
    }

    std::array<char, kFixedHeaderSize> header{};
    BuildHeader(header.data(), frame.header.flags, frame.header.message_type, frame.header.request_id,
                static_cast<std::uint32_t>(frame.metadata.size()),
                static_cast<std::uint32_t>(frame.body.size()));
    output.assign(header.data(), header.size());
    output.reserve(kFixedHeaderSize + frame.metadata.size() + frame.body.size());
    output.append(frame.metadata);
    output.append(frame.body);
    return true;
}

bool FrameEncoder::EncodeRequest(const RequestMetadataView &metadata, std::string_view body,
                                 std::uint64_t request_id, std::string &output, std::string *error)
{
    std::size_t metadata_size = 0;
    if (request_id == 0)
    {
        SetError(error, "request frames require a request id");
        return false;
    }
    if (!RequestMetadataEncodedSize(metadata, metadata_size, error) ||
        body.size() > std::numeric_limits<std::uint32_t>::max())
    {
        if (body.size() > std::numeric_limits<std::uint32_t>::max())
        {
            SetError(error, "frame field exceeds uint32 length");
        }
        return false;
    }

    std::array<char, kFixedHeaderSize> header{};
    BuildHeader(header.data(), 0, MessageType::REQUEST, request_id,
                static_cast<std::uint32_t>(metadata_size), static_cast<std::uint32_t>(body.size()));
    output.assign(header.data(), header.size());
    output.reserve(kFixedHeaderSize + metadata_size + body.size());
    if (!AppendRequestMetadata(metadata, output, error))
    {
        output.clear();
        return false;
    }
    output.append(body.data(), body.size());
    return true;
}

FrameDecoder::FrameDecoder(FrameLimits limits) : limits_(limits)
{
    const std::size_t min_buffer = static_cast<std::size_t>(kFixedHeaderSize) + limits_.max_metadata_size +
                                   static_cast<std::size_t>(limits_.max_body_size);
    if (limits_.max_buffer_size < min_buffer)
    {
        limits_.max_buffer_size = min_buffer;
    }
}

DecodeStatus FrameDecoder::Feed(const void *data, std::size_t size)
{
    if (state_ == State::FAILED)
    {
        return DecodeStatus::PROTOCOL_ERROR;
    }
    if (size == 0)
    {
        return DecodeStatus::NEED_MORE;
    }
    if (data == nullptr)
    {
        Fail("null frame input");
        return DecodeStatus::PROTOCOL_ERROR;
    }
    if (size > buffer_capacity_ - buffer_size_)
    {
        CompactBuffer();
    }
    if (!EnsureWritable(size))
    {
        return DecodeStatus::PROTOCOL_ERROR;
    }
    const auto *input = static_cast<const char *>(data);
    std::memcpy(buffer_.get() + buffer_size_, input, size);
    buffer_size_ += size;
    return DecodeStatus::NEED_MORE;
}

char *FrameDecoder::PrepareWritable(std::size_t size)
{
    if (state_ == State::FAILED || size == 0 || prepared_size_ != 0)
    {
        return nullptr;
    }
    if (size > buffer_capacity_ - buffer_size_)
    {
        CompactBuffer();
    }
    if (!EnsureWritable(size))
    {
        return nullptr;
    }
    prepared_size_ = size;
    return buffer_.get() + buffer_size_;
}

DecodeStatus FrameDecoder::CommitWritable(std::size_t size)
{
    if (prepared_size_ == 0 || size > prepared_size_)
    {
        Fail("invalid decoder write commit");
        prepared_size_ = 0;
        return DecodeStatus::PROTOCOL_ERROR;
    }
    buffer_size_ += size;
    prepared_size_ = 0;
    return DecodeStatus::NEED_MORE;
}

DecodeStatus FrameDecoder::Next(Frame &frame)
{
    FrameView view;
    const DecodeStatus status = NextView(view);
    if (status == DecodeStatus::FRAME_READY)
    {
        frame.header = view.header;
        frame.metadata.assign(view.metadata.data(), view.metadata.size());
        frame.body.assign(view.body.data(), view.body.size());
    }
    return status;
}

DecodeStatus FrameDecoder::NextView(FrameView &frame)
{
    if (state_ == State::FAILED)
    {
        return DecodeStatus::PROTOCOL_ERROR;
    }
    for (;;)
    {
        switch (state_)
        {
        case State::READ_FIXED_HEADER:
            if (ReadableBytes() < kFixedHeaderSize)
            {
                return DecodeStatus::NEED_MORE;
            }
            if (!DecodeHeader())
            {
                return DecodeStatus::PROTOCOL_ERROR;
            }
            break;
        case State::READ_METADATA:
            if (ReadableBytes() < static_cast<std::size_t>(pending_metadata_size_) + pending_body_size_)
            {
                return DecodeStatus::NEED_MORE;
            }
            frame.header = pending_header_;
            frame.metadata = std::string_view(buffer_.get() + read_offset_, pending_metadata_size_);
            read_offset_ += pending_metadata_size_;
            frame.body = std::string_view(buffer_.get() + read_offset_, pending_body_size_);
            read_offset_ += pending_body_size_;
            state_ = State::READ_FIXED_HEADER;
            return DecodeStatus::FRAME_READY;
        case State::FAILED:
            return DecodeStatus::PROTOCOL_ERROR;
        }
    }
}

void FrameDecoder::Reset()
{
    state_ = State::READ_FIXED_HEADER;
    buffer_size_ = 0;
    read_offset_ = 0;
    prepared_size_ = 0;
    pending_header_ = {};
    pending_metadata_size_ = 0;
    pending_body_size_ = 0;
    error_.clear();
}

bool FrameDecoder::DecodeHeader()
{
    const char *data = buffer_.get() + read_offset_;
    if (GetU32(data) != kMagic)
    {
        Fail("invalid frame magic");
        return false;
    }
    if (GetU16(data + 4) != kVersion)
    {
        Fail("unsupported frame version");
        return false;
    }
    if (GetU16(data + 6) != kFixedHeaderSize)
    {
        Fail("unsupported frame header size");
        return false;
    }
    const std::uint8_t type = static_cast<unsigned char>(data[12]);
    if (!IsValidMessageType(type) || data[13] != 0 || data[14] != 0 || data[15] != 0)
    {
        Fail("invalid frame header enum or reserved field");
        return false;
    }

    pending_header_.flags = GetU32(data + 8);
    pending_header_.message_type = static_cast<MessageType>(type);
    pending_header_.request_id = GetU64(data + 16);
    pending_metadata_size_ = GetU32(data + 24);
    pending_body_size_ = GetU32(data + 28);
    if (!ValidateHeader(pending_metadata_size_, pending_body_size_))
    {
        return false;
    }
    read_offset_ += kFixedHeaderSize;
    state_ = State::READ_METADATA;
    return true;
}

bool FrameDecoder::ValidateHeader(std::uint32_t metadata_size, std::uint32_t body_size)
{
    if ((pending_header_.message_type == MessageType::REQUEST || pending_header_.message_type == MessageType::RESPONSE) &&
        pending_header_.request_id == 0)
    {
        Fail("request and response frames require a request id");
        return false;
    }
    if (metadata_size > limits_.max_metadata_size || body_size > limits_.max_body_size)
    {
        Fail("frame field exceeds configured limit");
        return false;
    }
    return true;
}

void FrameDecoder::CompactBuffer()
{
    if (read_offset_ == 0)
    {
        return;
    }
    const std::size_t readable = buffer_size_ - read_offset_;
    if (readable != 0)
    {
        std::memmove(buffer_.get(), buffer_.get() + read_offset_, readable);
    }
    buffer_size_ = readable;
    read_offset_ = 0;
}

bool FrameDecoder::EnsureWritable(std::size_t size)
{
    if (size > limits_.max_buffer_size - buffer_size_)
    {
        Fail("decoder buffer limit exceeded");
        return false;
    }
    const std::size_t required = buffer_size_ + size;
    if (required <= buffer_capacity_)
    {
        return true;
    }
    std::size_t next_capacity = std::max<std::size_t>(16 * 1024, buffer_capacity_ * 2);
    next_capacity = std::max(next_capacity, required);
    next_capacity = std::min(next_capacity, limits_.max_buffer_size);
    auto next = std::make_unique<char[]>(next_capacity);
    if (buffer_size_ != 0)
    {
        std::memcpy(next.get(), buffer_.get(), buffer_size_);
    }
    buffer_ = std::move(next);
    buffer_capacity_ = next_capacity;
    return true;
}

std::size_t FrameDecoder::ReadableBytes() const noexcept
{
    return buffer_size_ - read_offset_;
}

void FrameDecoder::Fail(const char *message)
{
    state_ = State::FAILED;
    error_ = message;
}

} // namespace rpc::protocol
