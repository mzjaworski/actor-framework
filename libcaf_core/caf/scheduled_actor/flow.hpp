// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/actor.hpp"
#include "caf/async/batch.hpp"
#include "caf/detail/subscription_decorator.hpp"
#include "caf/detail/unsafe_flow_msg.hpp"
#include "caf/disposable.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/observable.hpp"
#include "caf/flow/observable_builder.hpp"
#include "caf/flow/observer.hpp"
#include "caf/scheduled_actor.hpp"

namespace caf::detail {

template <class T>
class publisher_proxy : public async::publisher<T>::impl {
public:
  using coordinated_subscription = flow::coordinator::subscription_impl;

  using coordinated_subscription_ptr = flow::coordinator::subscription_impl_ptr;

  class forwarder_impl : public flow::observer<T>::impl {
  public:
    forwarder_impl(actor hdl, flow::observer<T> sink)
      : hdl_(std::move(hdl)), sink_(std::move(sink)) {
      // nop
    }

    ~forwarder_impl() {
      if (sink_) {
        if (!sub_) {
          // Actor dropped the subscription message or terminated.
          sink_.on_error(make_error(sec::cannot_add_upstream));
        } else {
          // Actor failed to call on_attach() on the subscription.
          sink_.on_error(make_error(sec::invalid_stream_state));
        }
      }
    }

    void on_complete() override {
      if (sink_) {
        sink_.on_complete();
        reset();
      }
    }

    void on_error(const error& what) override {
      if (sink_) {
        sink_.on_error(what);
        reset();
      }
    }

    void on_attach(flow::subscription sub) override {
      CAF_ASSERT(dynamic_cast<coordinated_subscription*>(sub.ptr()) != nullptr);
      if (sink_ && !sub_) {
        sub_ = sub;
        using wrapper = detail::subscription_decorator;
        auto wrapped = make_counted<wrapper>(hdl_, std::move(sub));
        sink_.on_attach(flow::subscription{std::move(wrapped)});
        return;
      }
      sub.cancel();
    }

    void on_next(span<const T> items) override {
      if (sink_)
        sink_.on_next(items);
    }

    void dispose() override {
      if (sink_) {
        if (sub_)
          sub_.cancel();
        sink_.on_complete();
        reset();
      }
    }

    bool disposed() const noexcept override {
      return !sink_;
    }

  private:
    void reset() {
      hdl_ = nullptr;
      sink_ = nullptr;
      sub_ = nullptr;
    }

    actor hdl_;
    flow::observer<T> sink_;
    flow::subscription sub_;
  };

  publisher_proxy(actor hdl, flow::observable<T> src)
    : hdl_(std::move(hdl)), src_(std::move(src)) {
    // nop
  }

  void subscribe(flow::observer<T> sink) override {
    auto fwd = make_counted<forwarder_impl>(hdl_, std::move(sink));
    anon_send(hdl_, unsafe_flow_msg{src_, flow::observer<T>{fwd}});
  }

private:
  actor hdl_;
  flow::observable<T> src_;
};

/// Fetches items from a foreign publisher and forwards then to a coordinated
/// publisher of an actor.
template <class T>
class item_forwarder : public flow::observer<T>::impl {
public:
  explicit item_forwarder(actor hdl, flow::observer<T> target)
    : hdl_(std::move(hdl)), target_(std::move(target)) {
    // nop
  }

  // -- called from outside the actor ------------------------------------------

  void on_complete() override {
    auto ptr = flow::observer_base_ptr{target_.as_intrusive_ptr()};
    anon_send(hdl_, unsafe_flow_msg{std::move(ptr)});
  }

  void on_error(const error& what) override {
    auto ptr = flow::observer_base_ptr{target_.as_intrusive_ptr()};
    anon_send(hdl_, unsafe_flow_msg{std::move(ptr), what});
  }

  void on_next(span<const T> items) override {
    auto ptr = flow::observer_base_ptr{target_.as_intrusive_ptr()};
    anon_send(hdl_, unsafe_flow_msg{std::move(ptr), async::make_batch(items)});
  }

  void on_batch(const async::batch& items) override {
    auto ptr = flow::observer_base_ptr{target_.as_intrusive_ptr()};
    anon_send(hdl_, unsafe_flow_msg{std::move(ptr), items});
  }

  void on_attach(flow::subscription sub) override {
    anon_send(hdl_,
              unsafe_flow_msg{target_.as_intrusive_ptr(), std::move(sub)});
  }

  void dispose() override {
    on_complete();
    hdl_ = nullptr;
    target_ = nullptr;
  }

  bool disposed() const noexcept override {
    return hdl_ != nullptr;
  }

private:
  actor hdl_;
  flow::observer<T> target_;
};

} // namespace caf::detail

namespace caf::flow {

template <>
struct has_impl_include<scheduled_actor> {
  static constexpr bool value = true;
};

} // namespace caf::flow

namespace caf {

template <class T>
flow::observable<T> scheduled_actor::observe_impl(async::publisher<T> source) {
  // Internally, we have a broadcaster called `local` that makes its inputs
  // available to coordinated observers. The forwarder simply converts all
  // function calls from the source to messages. After receiving the messages,
  // the actor then calls the appropriate member functions on `local`, including
  // on_attach().
  auto local = make_counted<flow::broadcaster_impl<T>>(this);
  watch(local->as_disposable());
  auto hdl = actor_cast<actor>(this);
  auto fwd = make_counted<detail::item_forwarder<T>>(std::move(hdl),
                                                     flow::observer<T>{local});
  source.subscribe(flow::observer<T>{std::move(fwd)});
  return flow::observable<T>{local};
}

template <class Observable>
async::publisher<typename Observable::output_type>
scheduled_actor::to_async_publisher_impl(Observable source) {
  static_assert(flow::is_observable_v<Observable>);
  using output_type = typename Observable::output_type;
  auto obs = std::move(source).as_observable();
  watch(obs.as_disposable());
  using proxy_type = detail::publisher_proxy<output_type>;
  auto ptr = make_counted<proxy_type>(actor_cast<actor>(this), std::move(obs));
  return async::publisher<output_type>{std::move(ptr)};
}

} // namespace caf
