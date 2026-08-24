#include "runtime/timer_wheel.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <stdexcept>

namespace rpc::runtime {

void TimerList::PushBack(TimerNode &node)
{
    if (node.list != nullptr)
    {
        throw std::logic_error("timer node is already linked");
    }
    node.previous = tail;
    node.next = nullptr;
    node.list = this;
    if (tail != nullptr)
    {
        tail->next = &node;
    }
    else
    {
        head = &node;
    }
    tail = &node;
}

void TimerList::Remove(TimerNode &node) noexcept
{
    if (node.list != this)
    {
        return;
    }
    if (node.previous != nullptr)
    {
        node.previous->next = node.next;
    }
    else
    {
        head = node.next;
    }
    if (node.next != nullptr)
    {
        node.next->previous = node.previous;
    }
    else
    {
        tail = node.previous;
    }
    node.previous = nullptr;
    node.next = nullptr;
    node.list = nullptr;
}

TimerNode *TimerList::PopFront() noexcept
{
    TimerNode *node = head;
    if (node != nullptr)
    {
        Remove(*node);
    }
    return node;
}

void TimerList::SpliceBack(TimerList &other) noexcept
{
    if (other.empty())
    {
        return;
    }
    for (TimerNode *node = other.head; node != nullptr; node = node->next)
    {
        node->list = this;
    }
    if (tail != nullptr)
    {
        tail->next = other.head;
        other.head->previous = tail;
    }
    else
    {
        head = other.head;
    }
    tail = other.tail;
    other.head = nullptr;
    other.tail = nullptr;
}

TimerWheel::TimerWheel(std::size_t slot_count, std::uint64_t start_ms)
    : slots_(slot_count), current_tick_ms_(start_ms == 0 ? NowMs() : start_ms)
{
    if (slot_count == 0)
    {
        throw std::invalid_argument("timer wheel requires at least one slot");
    }
    deadline_heap_.reserve(std::min<std::size_t>(slot_count, 256));
}

void TimerWheel::Add(TimerNode &node)
{
    if (node.deadline_ms <= current_tick_ms_)
    {
        throw std::logic_error("timer deadline must be after the last processed tick");
    }
    Remove(node);

    const std::uint64_t remaining = node.deadline_ms > current_tick_ms_ ? node.deadline_ms - current_tick_ms_ : 0;
    const std::size_t offset = static_cast<std::size_t>(
        std::max<std::uint64_t>(1, std::min<std::uint64_t>(remaining, slots_.size() - 1)));
    slots_[(current_slot_ + offset) % slots_.size()].PushBack(node);
    node.scheduled = true;
    DeadlineHeapInsert(node);
    ++size_;
}

void TimerWheel::Remove(TimerNode &node) noexcept
{
    if (node.list == nullptr)
    {
        return;
    }
    DeadlineHeapRemove(node);
    const bool was_scheduled = node.scheduled;
    node.list->Remove(node);
    node.scheduled = false;
    if (was_scheduled && size_ != 0)
    {
        --size_;
    }
}

void TimerWheel::Advance(std::uint64_t now_ms, TimerList &expired)
{
    if (now_ms <= current_tick_ms_)
    {
        return;
    }

    const std::uint64_t elapsed = now_ms - current_tick_ms_;
    const std::size_t steps = static_cast<std::size_t>(
        std::min<std::uint64_t>(elapsed, slots_.size()));
    for (std::size_t step = 1; step <= steps; ++step)
    {
        TimerList &slot = slots_[(current_slot_ + step) % slots_.size()];
        for (TimerNode *node = slot.head; node != nullptr; node = node->next)
        {
            DeadlineHeapRemove(*node);
            node->scheduled = false;
            if (size_ != 0)
            {
                --size_;
            }
        }
        expired.SpliceBack(slot);
    }
    current_slot_ = (current_slot_ + static_cast<std::size_t>(elapsed % slots_.size())) % slots_.size();
    current_tick_ms_ = now_ms;
}

std::uint64_t TimerWheel::NextDeadlineMs() const noexcept
{
    return deadline_heap_.empty() ? std::numeric_limits<std::uint64_t>::max()
                                  : deadline_heap_.front()->deadline_ms;
}

bool TimerWheel::DeadlineBefore(const TimerNode *left, const TimerNode *right) noexcept
{
    return left->deadline_ms < right->deadline_ms ||
           (left->deadline_ms == right->deadline_ms && std::less<const TimerNode *>{}(left, right));
}

void TimerWheel::DeadlineHeapInsert(TimerNode &node)
{
    if (node.deadline_heap_index != TimerNode::kNotInDeadlineHeap)
    {
        throw std::logic_error("timer node is already in deadline heap");
    }
    node.deadline_heap_index = deadline_heap_.size();
    deadline_heap_.push_back(&node);
    DeadlineHeapSiftUp(node.deadline_heap_index);
}

void TimerWheel::DeadlineHeapRemove(TimerNode &node) noexcept
{
    const std::size_t index = node.deadline_heap_index;
    if (index == TimerNode::kNotInDeadlineHeap)
    {
        return;
    }
    TimerNode *last = deadline_heap_.back();
    deadline_heap_.pop_back();
    node.deadline_heap_index = TimerNode::kNotInDeadlineHeap;
    if (index == deadline_heap_.size())
    {
        return;
    }
    deadline_heap_[index] = last;
    last->deadline_heap_index = index;
    if (index > 0 && DeadlineBefore(last, deadline_heap_[(index - 1) / 2]))
    {
        DeadlineHeapSiftUp(index);
    }
    else
    {
        DeadlineHeapSiftDown(index);
    }
}

void TimerWheel::DeadlineHeapSwap(std::size_t left, std::size_t right) noexcept
{
    std::swap(deadline_heap_[left], deadline_heap_[right]);
    deadline_heap_[left]->deadline_heap_index = left;
    deadline_heap_[right]->deadline_heap_index = right;
}

void TimerWheel::DeadlineHeapSiftUp(std::size_t index) noexcept
{
    while (index > 0)
    {
        const std::size_t parent = (index - 1) / 2;
        if (!DeadlineBefore(deadline_heap_[index], deadline_heap_[parent]))
        {
            break;
        }
        DeadlineHeapSwap(index, parent);
        index = parent;
    }
}

void TimerWheel::DeadlineHeapSiftDown(std::size_t index) noexcept
{
    for (;;)
    {
        const std::size_t left = index * 2 + 1;
        if (left >= deadline_heap_.size())
        {
            return;
        }
        const std::size_t right = left + 1;
        const std::size_t child = right < deadline_heap_.size() &&
                                          DeadlineBefore(deadline_heap_[right], deadline_heap_[left])
                                      ? right
                                      : left;
        if (!DeadlineBefore(deadline_heap_[child], deadline_heap_[index]))
        {
            return;
        }
        DeadlineHeapSwap(index, child);
        index = child;
    }
}

std::uint64_t TimerWheel::NowMs() noexcept
{
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now().time_since_epoch())
                                         .count());
}

} // namespace rpc::runtime
