#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace rpc::runtime {

struct TimerList;

enum class TimerNodeType : std::uint8_t {
    NONE,
    IO_WAIT,
    ROUTINE_WAIT,
};

struct TimerNode {
    static constexpr std::size_t kNotInDeadlineHeap = std::numeric_limits<std::size_t>::max();

    explicit TimerNode(TimerNodeType node_type = TimerNodeType::NONE) noexcept : type(node_type) {}

    TimerNode *previous = nullptr;
    TimerNode *next = nullptr;
    TimerList *list = nullptr;
    std::uint64_t deadline_ms = 0;
    bool scheduled = false;
    TimerNodeType type = TimerNodeType::NONE;
    std::size_t deadline_heap_index = kNotInDeadlineHeap;
};

struct TimerList {
    TimerNode *head = nullptr;
    TimerNode *tail = nullptr;

    bool empty() const noexcept { return head == nullptr; }
    void PushBack(TimerNode &node);
    void Remove(TimerNode &node) noexcept;
    TimerNode *PopFront() noexcept;
    void SpliceBack(TimerList &other) noexcept;
};

class TimerWheel {
public:
    static constexpr std::size_t kDefaultSlotCount = 60 * 1000;

    explicit TimerWheel(std::size_t slot_count = kDefaultSlotCount, std::uint64_t start_ms = 0);

    void Add(TimerNode &node);
    void Remove(TimerNode &node) noexcept;
    void Advance(std::uint64_t now_ms, TimerList &expired);
    std::uint64_t NextDeadlineMs() const noexcept;

    std::size_t size() const noexcept { return size_; }
    std::size_t slot_count() const noexcept { return slots_.size(); }
    std::uint64_t current_tick_ms() const noexcept { return current_tick_ms_; }

    static std::uint64_t NowMs() noexcept;

private:
    static bool DeadlineBefore(const TimerNode *left, const TimerNode *right) noexcept;
    void DeadlineHeapInsert(TimerNode &node);
    void DeadlineHeapRemove(TimerNode &node) noexcept;
    void DeadlineHeapSwap(std::size_t left, std::size_t right) noexcept;
    void DeadlineHeapSiftUp(std::size_t index) noexcept;
    void DeadlineHeapSiftDown(std::size_t index) noexcept;

    std::vector<TimerList> slots_;
    std::uint64_t current_tick_ms_;
    std::size_t current_slot_ = 0;
    std::size_t size_ = 0;
    std::vector<TimerNode *> deadline_heap_;
};

} // namespace rpc::runtime
