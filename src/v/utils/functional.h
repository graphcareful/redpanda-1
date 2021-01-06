/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "utils/concepts-enabled.h"

#include <functional>
#include <optional>
namespace reduce {
struct push_back {
    /// TODO(rob) introduce a c++ concept here
    template<typename vec_like>
    vec_like operator()(vec_like acc, typename vec_like::value_type t) {
        acc.push_back(std::move(t));
        return std::move(acc);
    }
};

struct push_back_opt {
    template<typename vec_like>
    vec_like
    operator()(vec_like acc, std::optional<typename vec_like::value_type> ot) {
        if (ot) {
            acc.push_back(std::move(*ot));
        }
        return std::move(acc);
    }
};

} // namespace reduce
namespace xform {
struct logical_true {
    bool operator()(bool x) { return x; }
};

template<typename T>
struct equal_to {
    explicit equal_to(T value)
      : _value(std::forward<T>(value)) {}
    bool operator()(const T& other) { return _value == other; }
    T _value;
};

template<typename T>
struct not_equal_to {
    explicit not_equal_to(T value)
      : _value(std::forward<T>(value)) {}
    bool operator()(const T& other) { return _value != other; }
    T _value;
};
} // namespace xform
