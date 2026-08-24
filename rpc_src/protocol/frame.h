#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace rpc::protocol {

struct RequestMetadataView;

constexpr std::uint32_t kMagic = 0x52504352U; // "RPCR"
constexpr std::uint16_t kVersion = 1;
constexpr std::uint16_t kFixedHeaderSize = 32;

enum class MessageType : std::uint8_t {
    REQUEST = 1,
    RESPONSE = 2,
    CANCEL = 3,
    PING = 4,
    PONG = 5,
};

enum class DecodeStatus : std::uint8_t { NEED_MORE, FRAME_READY, PROTOCOL_ERROR };

struct FrameHeader {
    std::uint32_t flags = 0;
    MessageType message_type = MessageType::REQUEST;
    std::uint64_t request_id = 0;
};

struct Frame {
    FrameHeader header;
    std::string metadata;
    std::string body;
};

// A non-owning frame decoded directly from FrameDecoder's receive buffer.
// Advancing through bytes already received does not move the buffer. The views
// remain valid until the decoder is fed, prepared for another write, or reset.
struct FrameView {
    FrameHeader header;
    std::string_view metadata;
    std::string_view body;
};

struct FrameLimits {
    std::uint32_t max_metadata_size = 64 * 1024;
    std::uint32_t max_body_size = 16 * 1024 * 1024;
    std::size_t max_buffer_size = 17 * 1024 * 1024;
};

class FrameEncoder {
public:
    static bool Encode(const Frame &frame, std::string &output, std::string *error = nullptr);
    static bool Encode(const FrameView &frame, std::string &output, std::string *error = nullptr);
    static bool EncodeRequest(const RequestMetadataView &metadata, std::string_view body,
                              std::uint64_t request_id, std::string &output,
                              std::string *error = nullptr);
};

class FrameDecoder {
public:
    explicit FrameDecoder(FrameLimits limits = {});

    DecodeStatus Feed(const void *data, std::size_t size);
    char *PrepareWritable(std::size_t size);
    DecodeStatus CommitWritable(std::size_t size);
    DecodeStatus Next(Frame &frame);
    DecodeStatus NextView(FrameView &frame);
    void Reset();

    const std::string &error() const noexcept { return error_; }

private:
    enum class State : std::uint8_t { READ_FIXED_HEADER, READ_METADATA, FAILED };

    bool DecodeHeader();
    bool ValidateHeader(std::uint32_t metadata_size, std::uint32_t body_size);
    void CompactBuffer();
    bool EnsureWritable(std::size_t size);
    std::size_t ReadableBytes() const noexcept;
    void Fail(const char *message);

    FrameLimits limits_;
    State state_ = State::READ_FIXED_HEADER;
    std::unique_ptr<char[]> buffer_;
    std::size_t buffer_size_ = 0;
    std::size_t buffer_capacity_ = 0;
    std::size_t prepared_size_ = 0;
    std::size_t read_offset_ = 0;
    FrameHeader pending_header_;
    std::uint32_t pending_metadata_size_ = 0;
    std::uint32_t pending_body_size_ = 0;
    std::string error_;
};

} // namespace rpc::protocol
