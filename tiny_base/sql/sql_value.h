#ifndef TINY_BASE_SQL_VALUE_H_
#define TINY_BASE_SQL_VALUE_H_

#include <cstring>
#include <ctime>
#include <experimental/any>
#include <iomanip>
#include <string>
#include <vector>

namespace sql {

namespace expr = std::experimental;

enum DataType {
  OneByteNull = 0x00,
  TwoByteNull = 0x01,
  FourByteNull = 0x02,
  EightByteNull = 0x03,
  TinyInt = 0x04,
  SmallInt = 0x05,
  Int = 0x06,
  BigInt = 0x07,
  Real = 0x08,
  Double = 0x09,
  DateTime = 0x0A,
  Date = 0x0B,
  Text = 0x0C,
  Invalid = 0xFF
};

using Value = std::experimental::any;

enum OperatorType { Equal, Unequal, Larger, Smaller, NotLarger, NotSmaller };

static const Value BytesToValue(const DataType& type,
                                const std::vector<char>& bytes) {
  Value to_value;

  switch (type) {
    case OneByteNull:
    case TinyInt: {
      int8_t value_8;
      std::memcpy(&value_8, bytes.data(), sizeof(value_8));
      to_value = value_8;
    } break;
    case TwoByteNull:
    case SmallInt: {
      int16_t value_16;
      std::memcpy(&value_16, bytes.data(), sizeof(value_16));
      to_value = value_16;
    } break;
    case FourByteNull:
    case Int: {
      int32_t value_32;
      std::memcpy(&value_32, bytes.data(), sizeof(value_32));
      to_value = value_32;
    } break;
    case EightByteNull:
    case BigInt: {
      int64_t value_64;
      std::memcpy(&value_64, bytes.data(), sizeof(value_64));
      to_value = value_64;
    } break;
    case Real: {
      float value_float;
      std::memcpy(&value_float, bytes.data(), sizeof(value_float));
      to_value = value_float;
    } break;
    case Double: {
      double value_double;
      std::memcpy(&value_double, bytes.data(), sizeof(value_double));
      to_value = value_double;
    } break;
    case DateTime: {
      uint64_t value_date_time;
      std::memcpy(&value_date_time, bytes.data(), sizeof(value_date_time));
      to_value = value_date_time;
    } break;
    case Date: {
      uint64_t value_date;
      std::memcpy(&value_date, bytes.data(), sizeof(value_date));
      to_value = value_date;
    } break;
    case Text: {
      std::string value_str;
      std::copy(bytes.begin(), bytes.end(), std::back_inserter(value_str));
      to_value = value_str;
    } break;
    default:
      break;
  }

  return to_value;
}

static const std::string BytesToString(const DataType& type,
                                       const std::vector<char>& bytes) {
  std::string res_str;

  switch (type) {
    case OneByteNull:
    case TinyInt: {
      int8_t value_8;
      std::memcpy(&value_8, bytes.data(), sizeof(value_8));
      res_str = std::to_string(value_8);
    } break;
    case TwoByteNull:
    case SmallInt: {
      int16_t value_16;
      std::memcpy(&value_16, bytes.data(), sizeof(value_16));
      res_str = std::to_string(value_16);
    } break;
    case FourByteNull:
    case Int: {
      int32_t value_32;
      std::memcpy(&value_32, bytes.data(), sizeof(value_32));
      res_str = std::to_string(value_32);
    } break;
    case EightByteNull:
    case BigInt: {
      int64_t value_64;
      std::memcpy(&value_64, bytes.data(), sizeof(value_64));
      res_str = std::to_string(value_64);
    } break;
    case Real: {
      float value_float;
      std::memcpy(&value_float, bytes.data(), sizeof(value_float));
      res_str = std::to_string(value_float);
    } break;
    case Double: {
      double value_double;
      std::memcpy(&value_double, bytes.data(), sizeof(value_double));
      res_str = std::to_string(value_double);
    } break;
    case DateTime: {
      uint64_t value_date_time;
      std::memcpy(&value_date_time, bytes.data(), sizeof(value_date_time));
      std::time_t date_time(value_date_time);
      std::stringstream str_stream;
      // TODO: localtime or gmtime
      str_stream << std::put_time(std::localtime(&date_time), "%F_%T");
      res_str = str_stream.str();
    } break;
    case Date: {
      uint64_t value_date;
      std::memcpy(&value_date, bytes.data(), sizeof(value_date));
      std::time_t date(value_date);
      std::stringstream str_stream;
      // TODO: localtime or gmtime
      str_stream << std::put_time(std::localtime(&date), "%F");
      res_str = str_stream.str();
    } break;
    case Text: {
      std::string value_str;
      std::copy(bytes.begin(), bytes.end(), std::back_inserter(value_str));
      res_str = value_str;
    } break;
    default:
      break;
  }

  return res_str;
}

template <typename T>
bool Compare(const T& lhs, const T& rhs, const OperatorType& operator_type) {
  bool result(false);
  switch (operator_type) {
    case Equal:
      result = (lhs == rhs);
      break;
    case Unequal:
      result = (lhs != rhs);
      break;
    case Larger:
      result = (lhs > rhs);
      break;
    case Smaller:
      result = (lhs < rhs);
      break;
    case NotLarger:
      result = (lhs <= rhs);
      break;
    case NotSmaller:
      result = (lhs >= rhs);
      break;
    default:
      break;
  }
  return result;
}

template <>
inline bool Compare(const std::time_t& lhs, const std::time_t& rhs,
                    const OperatorType& operator_type) {
  bool result(false);
  switch (operator_type) {
    case Equal:
      result = (lhs == rhs);
      break;
    case Unequal:
      result = (lhs != rhs);
      break;
    case Larger:
      result = (std::difftime(lhs, rhs) > 0.0f);
      break;
    case Smaller:
      result = (std::difftime(lhs, rhs) < 0.0f);
      break;
    case NotLarger:
      result = (std::difftime(lhs, rhs) <= 0.0f);
      break;
    case NotSmaller:
      result = (std::difftime(lhs, rhs) >= 0.0f);
      break;
    default:
      break;
  }
  return result;
}

static const bool CompareValue(const Value& lhs, const Value& rhs,
                               const DataType& type,
                               const OperatorType& operator_type) {
  bool result(false);
  switch (type) {
    case OneByteNull:
    case TinyInt: {
      int8_t lhs_value_8 = std::experimental::any_cast<int8_t>(lhs);
      int8_t rhs_value_8 = std::experimental::any_cast<int8_t>(rhs);
      result = Compare(lhs_value_8, rhs_value_8, operator_type);
    } break;
    case TwoByteNull:
    case SmallInt: {
      int16_t lhs_value_16 = std::experimental::any_cast<int16_t>(lhs);
      int16_t rhs_value_16 = std::experimental::any_cast<int16_t>(rhs);
      result = Compare(lhs_value_16, rhs_value_16, operator_type);
    } break;
    case FourByteNull:
    case Int: {
      int32_t lhs_value_32 = std::experimental::any_cast<int32_t>(lhs);
      int32_t rhs_value_32 = std::experimental::any_cast<int32_t>(rhs);
      result = Compare(lhs_value_32, rhs_value_32, operator_type);
    } break;
    case EightByteNull:
    case BigInt: {
      int64_t lhs_value_64 = std::experimental::any_cast<int64_t>(lhs);
      int64_t rhs_value_64 = std::experimental::any_cast<int64_t>(rhs);
      result = Compare(lhs_value_64, rhs_value_64, operator_type);
    } break;
    case Real: {
      float lhs_value_float = expr::any_cast<float>(lhs);
      float rhs_value_float = expr::any_cast<float>(rhs);
      result = Compare(lhs_value_float, rhs_value_float, operator_type);
    } break;
    case Double: {
      double lhs_value_double = expr::any_cast<double>(lhs);
      double rhs_value_double = expr::any_cast<double>(rhs);
      result = Compare(lhs_value_double, rhs_value_double, operator_type);
    } break;
    case DateTime: {
      uint64_t lhs_value_date_time = expr::any_cast<uint64_t>(lhs);
      uint64_t rhs_value_date_time = expr::any_cast<uint64_t>(rhs);
      std::time_t lhs_date_time(lhs_value_date_time);
      std::time_t rhs_date_time(rhs_value_date_time);
      result = Compare(lhs_date_time, rhs_date_time, operator_type);
    } break;
    case Date: {
      uint64_t lhs_value_date = expr::any_cast<uint64_t>(lhs);
      uint64_t rhs_value_date = expr::any_cast<uint64_t>(rhs);
      std::time_t lhs_date(lhs_value_date);
      std::time_t rhs_date(rhs_value_date);
      result = Compare(lhs_date, rhs_date, operator_type);
    } break;
    case Text: {
      std::string lhs_value_str = std::experimental::any_cast<std::string>(lhs);
      std::string rhs_value_str = std::experimental::any_cast<std::string>(rhs);
      result = Compare(lhs_value_str, rhs_value_str, operator_type);
    } break;
    default:
      break;
  }

  return result;
}

}  // namespace sql

#endif  // TINY_BASE_SQL_VALUE_H_
