// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/fwd.hpp"

namespace caf::flow {

class coordinator;

class subscription;

class observer_base;

using observer_base_ptr = intrusive_ptr<observer_base>;

class observable_base;

using observable_base_ptr = intrusive_ptr<observable_base>;

template <class T>
class observer;

template <class T>
class observable;

template <class In, class Out>
class processor;

template <class Generator, class... Steps>
class generation;

template <class Step, class... Steps>
class transformation;

template <class T>
struct is_observable {
  static constexpr bool value = false;
};

template <class T>
struct is_observable<observable<T>> {
  static constexpr bool value = true;
};

template <class Step, class... Steps>
struct is_observable<transformation<Step, Steps...>> {
  static constexpr bool value = true;
};

template <class Generator, class... Steps>
struct is_observable<generation<Generator, Steps...>> {
  static constexpr bool value = true;
};

template <class In, class Out>
struct is_observable<processor<In, Out>> {
  static constexpr bool value = true;
};

template <class T>
constexpr bool is_observable_v = is_observable<T>::value;

template <class T>
struct is_observer {
  static constexpr bool value = false;
};

template <class T>
struct is_observer<observer<T>> {
  static constexpr bool value = true;
};

template <class Step, class... Steps>
struct is_observer<transformation<Step, Steps...>> {
  static constexpr bool value = true;
};

template <class In, class Out>
struct is_observer<processor<In, Out>> {
  static constexpr bool value = true;
};

template <class T>
constexpr bool is_observer_v = is_observer<T>::value;

class observable_builder;

template <class T>
struct input_type_oracle {
  using type = typename T::input_type;
};

template <class T>
using input_type_t = typename input_type_oracle<T>::type;

template <class T>
struct output_type_oracle {
  using type = typename T::output_type;
};

template <class T>
using output_type_t = typename output_type_oracle<T>::type;

template <class>
struct has_impl_include {
  static constexpr bool value = false;
};

template <class T>
constexpr bool has_impl_include_v = has_impl_include<T>::value;

template <class T>
class merger_impl;

} // namespace caf::flow
