#ifndef TINY_BASE_ENDIAN_UTIL_H_
#define TINY_BASE_ENDIAN_UTIL_H_

#include <cstdint>
#include <vector>

namespace utils {

template <typename T>
T SwapEndian(T value) {
  union {
    T value;
    uint8_t u8[sizeof(T)];
  } src, dest;

  src.value = value;

  for (auto i = 0; i < sizeof(T); i++) {
    dest.u8[i] = src.u8[sizeof(T) - i - 1];
  }

  return dest.value;
}

template <typename T>
std::vector<T> SwapEndian(const std::vector<T>& value_array) {
  auto array(value_array);

  for (auto& v : array) {
    v = SwapEndian<T>(v);
  }

  return array;
}

template <typename T>
void SwapEndianInPlace(std::vector<T>& value_array) {
  for (auto& v : value_array) {
    v = SwapEndian<T>(v);
  }
}

}  // namespace utils

#endif  // TINY_BASE_ENDIAN_UTIL_H_
