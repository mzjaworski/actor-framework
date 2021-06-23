// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/actor.hpp"
#include "caf/flow/subscription.hpp"

namespace caf::detail {

class subscription_decorator : public flow::subscription::impl {
public:
  subscription_decorator(actor hdl, flow::subscription decorated);

  ~subscription_decorator();

  void cancel() override;

  void request(size_t n) override;

private:
  void reset();

  actor hdl_;
  flow::subscription decorated_;
};

} // namespace caf::detail
