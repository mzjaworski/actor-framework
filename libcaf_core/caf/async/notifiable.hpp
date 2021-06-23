// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <cstddef>

#include "caf/detail/core_export.hpp"
#include "caf/fwd.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::async {

/// Synchronization primitive for signaling events to a listener.
class CAF_CORE_EXPORT notifiable {
public:
  /// Internal impl of a `notifiable`.
  class impl : public virtual ref_counted {
  public:
    ~impl() override;

    /// Signals to the listener that an event has occurred.
    virtual void notify_event() = 0;

    /// Signals to the listener that the event source has stopped.
    virtual void notify_close() = 0;

    /// Signals to the listener that the event source has failed.
    virtual void notify_abort(const error& reason) = 0;
  };

  /// Receives notifications from a connected `notifiable`.
  class listener : public virtual ref_counted {
  public:
    ~listener() override;

    /// Signals to the listener that an event has occurred.
    virtual void on_event() = 0;

    /// Signals to the listener that the event source has stopped.
    virtual void on_close() = 0;

    /// Signals to the listener that the event source has failed.
    virtual void on_abort(const error& reason) = 0;
  };

  using listener_ptr = intrusive_ptr<listener>;

  explicit notifiable(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  notifiable& operator=(std::nullptr_t) noexcept {
    pimpl_ = nullptr;
    return *this;
  }

  notifiable() noexcept = default;
  notifiable(notifiable&&) noexcept = default;
  notifiable(const notifiable&) noexcept = default;
  notifiable& operator=(notifiable&&) noexcept = default;
  notifiable& operator=(const notifiable&) noexcept = default;

  bool valid() const noexcept {
    return pimpl_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  bool operator!() const noexcept {
    return !valid();
  }

  /// @copydoc impl::notify
  void notify_event() {
    if (pimpl_)
      pimpl_->notify_event();
  }

  /// @copydoc impl::close
  void notify_close() {
    if (pimpl_) {
      pimpl_->notify_close();
      pimpl_ = nullptr;
    }
  }

  /// @copydoc impl::abort
  void notify_abort(const error& reason) {
    if (pimpl_) {
      pimpl_->notify_abort(reason);
      pimpl_ = nullptr;
    }
  }

private:
  intrusive_ptr<impl> pimpl_;
};

} // namespace caf::async
