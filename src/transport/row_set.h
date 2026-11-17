// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <bit>
#include <cstdint>

namespace TangoUcx::transport
{
// A set of rows of the publisher's session table, which has 64 rows: one bit per row, so a set
// is one integer and the set operations are single instructions. Iterating visits the rows in
// ascending order:
//
//     for(auto row : every.members)
//         if(!pub.has_room(pub.rows[row]))
//             ...
class RowSet
{
  public:
    static constexpr std::uint32_t k_capacity = 64;

    constexpr RowSet() = default;
    static constexpr RowSet of(std::uint32_t row)
    {
        return RowSet{std::uint64_t{1} << row};
    }

    constexpr bool has(std::uint32_t row) const
    {
        return bits_ >> row & 1;
    }
    constexpr void add(std::uint32_t row)
    {
        bits_ |= std::uint64_t{1} << row;
    }
    constexpr void remove(std::uint32_t row)
    {
        bits_ &= ~(std::uint64_t{1} << row);
    }
    constexpr bool empty() const
    {
        return bits_ == 0;
    }
    constexpr std::uint32_t size() const
    {
        return std::uint32_t(std::popcount(bits_));
    }

    // The rows in either set, in both, and in this one but not the other.
    friend constexpr RowSet operator|(RowSet a, RowSet b)
    {
        return RowSet{a.bits_ | b.bits_};
    }
    friend constexpr RowSet operator&(RowSet a, RowSet b)
    {
        return RowSet{a.bits_ & b.bits_};
    }
    constexpr RowSet without(RowSet other) const
    {
        return RowSet{bits_ & ~other.bits_};
    }
    constexpr RowSet &operator|=(RowSet other)
    {
        bits_ |= other.bits_;
        return *this;
    }
    friend constexpr bool operator==(RowSet, RowSet) = default;

    // Visits the rows lowest first: the lowest set bit is the current row, and clearing it
    // moves to the next.
    class iterator
    {
      public:
        constexpr std::uint32_t operator*() const
        {
            return std::uint32_t(std::countr_zero(left_));
        }
        constexpr iterator &operator++()
        {
            left_ &= left_ - 1;
            return *this;
        }
        friend constexpr bool operator==(iterator, iterator) = default;

      private:
        friend class RowSet;
        constexpr explicit iterator(std::uint64_t left) : left_(left) {}
        std::uint64_t left_;
    };
    constexpr iterator begin() const
    {
        return iterator{bits_};
    }
    constexpr iterator end() const
    {
        return iterator{0};
    }

  private:
    constexpr explicit RowSet(std::uint64_t bits) : bits_(bits) {}
    std::uint64_t bits_ = 0;
};
} // namespace TangoUcx::transport
