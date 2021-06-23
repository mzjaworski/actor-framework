/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright 2011-2021 Dominik Charousset                                     *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#pragma once

#include <type_traits>

#include "caf/async/blocking_observer.hpp"
#include "caf/async/observer_buffer.hpp"
#include "caf/error.hpp"
#include "caf/flow/observer.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"
#include "caf/sec.hpp"

namespace caf::async {

/// Provides a potentially infinite number of items, publishing them to
/// observers according to signaled demand. The publisher runs asynchronously to
/// its observers and only guarantees happens-before ordering and the invocation
/// sequences defined by the Reactive Streams protocol
/// (https://www.reactive-streams.org).
template <class T>
class publisher {
public:
  using output_type = T;

  /// Internal interface of a `publisher`.
  class impl : public virtual ref_counted {
  public:
    /// Adds a new observer.
    virtual void subscribe(flow::observer<T> who) = 0;

    publisher as_publisher() noexcept {
      return publisher{intrusive_ptr<impl>(this)};
    }
  };

  explicit publisher(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  publisher& operator=(std::nullptr_t) noexcept {
    pimpl_.reset();
    return *this;
  }

  publisher() noexcept = default;
  publisher(publisher&&) noexcept = default;
  publisher(const publisher&) noexcept = default;
  publisher& operator=(publisher&&) noexcept = default;
  publisher& operator=(const publisher&) noexcept = default;

  /// @copydoc impl::subscribe
  void subscribe(flow::observer<T> sink) {
    if (pimpl_)
      pimpl_->subscribe(std::move(sink));
    else
      sink.on_error(make_error(sec::invalid_upstream));
  }

  /// Creates a new coordinator, then observes the output of this publisher
  /// locally and optionally returns a new publisher created by the coordinator.
  template <class Impl, class Context, class Fn, class... Ts>
  auto subscribe_with(Context& ctx, Fn init, Ts&&... ctor_args) {
    auto [ptr, launch] = ctx.template make_flow_coordinator<Impl>(
      std::forward<Ts>(ctor_args)...);
    using init_res_t = decltype(init(ptr, ptr->observe(*this)));
    if constexpr (std::is_same_v<init_res_t, void>) {
      init(ptr, ptr->observe(*this));
      launch();
    } else {
      auto res = ptr->to_publisher(init(ptr, ptr->observe(*this)));
      launch();
      return res;
    }
  }

  blocking_publisher<T> to_blocking() const;

  template <class OnNext>
  void blocking_for_each(OnNext fun) const;

  template <class OnNext>
  void blocking_for_each_while(OnNext fun) const;

  bool valid() const noexcept {
    return pimpl_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  bool operator!() const noexcept {
    return !valid();
  }

  impl* ptr() {
    return pimpl_.get();
  }

  const impl* ptr() const {
    return pimpl_.get();
  }

  const intrusive_ptr<impl>& as_intrusive_ptr() const& noexcept {
    return pimpl_;
  }

  intrusive_ptr<impl>&& as_intrusive_ptr() && noexcept {
    return std::move(pimpl_);
  }

  void swap(publisher& other) {
    pimpl_.swap(other.pimpl_);
  }

private:
  intrusive_ptr<impl> pimpl_;
};

/// A variety of `publisher` with blocking access to the items. In general,
/// blocking publishers are rarely needed outside of testing or bridging to
/// legacy code.
template <class T>
class blocking_publisher {
public:
  using output_type = T;

  explicit blocking_publisher(publisher<T> decorated) noexcept
    : decorated_(std::move(decorated)) {
    // nop
  }

  blocking_publisher() noexcept = default;
  blocking_publisher(blocking_publisher&&) noexcept = default;
  blocking_publisher(const blocking_publisher&) noexcept = default;
  blocking_publisher& operator=(blocking_publisher&&) noexcept = default;
  blocking_publisher& operator=(const blocking_publisher&) noexcept = default;

  template <class OnNext, class OnError, class OnComplete>
  void subscribe(OnNext fun, OnError err, OnComplete fin) {
    static_assert(std::is_invocable_v<OnNext, T>,
                  "OnNext handlers must have signature 'void(T)'");
    using res_t = decltype(fun(std::declval<T>()));
    static_assert(std::is_same_v<res_t, void> || std::is_same_v<res_t, bool>,
                  "OnNext handlers must have signature 'void(T)' or 'bool(T)'");
    auto buf = make_counted<blocking_observer<T>>();
    decorated_.subscribe(flow::observer<T>{buf});
    buf->run(std::move(fun), std::move(err), std::move(fin));
  }

  template <class OnNext, class OnError>
  void subscribe(OnNext fun, OnError err) {
    subscribe(std::move(fun), std::move(err), [] {});
  }

  template <class OnNext>
  void subscribe(OnNext fun) {
    subscribe(
      std::move(fun), [](const error&) {}, [] {});
  }

private:
  publisher<T> decorated_;
};

template <class T>
blocking_publisher<T> publisher<T>::to_blocking() const {
  return blocking_publisher<T>{*this};
}

template <class T>
template <class OnNext>
void publisher<T>::blocking_for_each(OnNext fun) const {
  to_blocking().subscribe(std::move(fun));
}

template <class T>
template <class OnNext>
void publisher<T>::blocking_for_each_while(OnNext fun) const {
  to_blocking().subscribe(std::move(fun));
}

/// Creates a new @ref async::publisher that dispatches to an
/// @ref flow::observable managed by a new @ref coordinator.
/// @relates publisher
template <class Impl, class Context, class Fn, class... Ts>
auto publisher_from(Context& ctx, Fn init, Ts&&... ctor_args) {
  auto [ptr, launch]
    = ctx.template make_flow_coordinator<Impl>(std::forward<Ts>(ctor_args)...);
  using init_res_t = decltype(init(ptr));
  static_assert(!std::is_same_v<init_res_t, void>);
  auto res = ptr->to_async_publisher(init(ptr));
  launch();
  return res;
}

} // namespace caf::async
