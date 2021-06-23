// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/detail/core_export.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf {

/// Represents a disposable resource.
class CAF_CORE_EXPORT disposable {
public:
  /// Internal impl of a `disposable`.
  class impl : public virtual ref_counted {
  public:
    ~impl() override;

    /// Disposes the resource.
    /// @note Calling `dispose()` on a disposed resource is a no-op.
    virtual void dispose() = 0;

    /// Checks whether the resource has been disposed.
    virtual bool disposed() const noexcept = 0;
  };

  explicit disposable(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  disposable() noexcept = default;
  disposable(disposable&&) noexcept = default;
  disposable(const disposable&) noexcept = default;
  disposable& operator=(disposable&&) noexcept = default;
  disposable& operator=(const disposable&) noexcept = default;

  /// @copydoc impl::dispose
  void dispose() {
    if (pimpl_) {
      pimpl_->dispose();
      pimpl_ = nullptr;
    }
  }

  /// @copydoc impl::disposed
  [[nodiscard]] bool disposed() const noexcept {
    return pimpl_ ? pimpl_->disposed() : true;
  }

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

  intrusive_ptr<impl>&& as_intrusive_ptr() && noexcept {
    return std::move(pimpl_);
  }

  void swap(disposable& other) {
    pimpl_.swap(other.pimpl_);
  }

private:
  intrusive_ptr<impl> pimpl_;
};

} // namespace caf
