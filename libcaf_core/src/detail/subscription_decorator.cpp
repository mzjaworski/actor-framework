// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/detail/subscription_decorator.hpp"

#include "caf/detail/unsafe_flow_msg.hpp"
#include "caf/send.hpp"

namespace caf::detail {

subscription_decorator::subscription_decorator(actor hdl,
                                               flow::subscription decorated)
  : hdl_(std::move(hdl)), decorated_(std::move(decorated)) {
  // nop
}

subscription_decorator::~subscription_decorator() {
  reset();
}

void subscription_decorator::cancel() {
  reset();
}

void subscription_decorator::request(size_t n) {
  anon_send(hdl_, unsafe_flow_msg{decorated_, n});
}

void subscription_decorator::reset() {
  if (hdl_) {
    anon_send(hdl_, unsafe_flow_msg{decorated_});
    hdl_ = nullptr;
    decorated_ = nullptr;
  }
}

} // namespace caf::detail
