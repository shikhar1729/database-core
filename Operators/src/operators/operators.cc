
#include "operators/operators.h"

#include <bits/stdc++.h>

#include <cassert>
#include <functional>
#include <string>

#include "common/macros.h"

#define UNUSED(p) ((void)(p))
namespace buzzdb {
namespace operators {

Register Register::from_int(int64_t value) {
  Register r{};
  r.val_int = value;
  return r;
}

Register Register::from_string(const std::string& value) {
  Register r{};
  r.val_str = value;
  return r;
}

Register::Type Register::get_type() const {
  return this->val_int ? Type::INT64 : Type::CHAR16;
}

int64_t Register::as_int() const { return this->val_int ? this->val_int : 0; }

std::string Register::as_string() const { return this->val_str; }

uint64_t Register::get_hash() const {
  return this->get_type() == Type::INT64
             ? std::hash<int64_t>{}(this->val_int)
             : std::hash<std::string>{}(this->val_str);
}

bool operator==(const Register& r1, const Register& r2) {
  return r1.get_hash() == r2.get_hash();
}

bool operator!=(const Register& r1, const Register& r2) {
  return r1.get_hash() != r2.get_hash();
}

bool operator<(const Register& r1, const Register& r2) {
  return r1.get_type() == Register::Type::INT64
             ? r1.as_int() < r2.as_int()
             : r1.as_string().compare(r2.as_string()) < 0;
}

bool operator<=(const Register& r1, const Register& r2) {
  return r1.get_type() == Register::Type::INT64
             ? r1.as_int() <= r2.as_int()
             : r1.as_string().compare(r2.as_string()) <= 0;
}

bool operator>(const Register& r1, const Register& r2) {
  return r1.get_type() == Register::Type::INT64
             ? r1.as_int() > r2.as_int()
             : r1.as_string().compare(r2.as_string()) > 0;
}

bool operator>=(const Register& r1, const Register& r2) {
  return r1.get_type() == Register::Type::INT64
             ? r1.as_int() >= r2.as_int()
             : r1.as_string().compare(r2.as_string()) >= 0;
}

Print::Print(Operator& input, std::ostream& stream) : UnaryOperator(input) {
  this->stream = &stream;
}

Print::~Print() = default;

void Print::open() { this->input->open(); }

bool Print::next() {
  if (this->input->next()) {
    std::vector<Register*> input_tuple = this->input->get_output();
    std::string str = "";

    for (auto& reg : input_tuple) {
      if (reg->get_type() == Register::Type::INT64) {
        str += std::to_string(reg->as_int()) + ",";
      } else {
        str += reg->as_string() + ",";
      }
    }

    if (input_tuple.size() > 0) {
      str = str.substr(0, str.size() - 1);
      str += '\n';
      *this->stream << str;
    }
    return true;
  }
  return false;
}

void Print::close() { this->input->close(); }

std::vector<Register*> Print::get_output() { return {}; }

Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
    : UnaryOperator(input) {
  this->attr_indexes = std::move(attr_indexes);
}

Projection::~Projection() = default;

void Projection::open() { this->input->open(); }

bool Projection::next() {
  if (this->input->next()) {
    std::vector<Register*> output_registers = this->input->get_output();
    for (auto attr_index : this->attr_indexes) {
      this->output_registers.push_back(*output_registers[attr_index]);
    }
    return true;
  }
  return false;
}

void Projection::close() { this->input->close(); }

std::vector<Register*> Projection::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

Select::Select(Operator& input, PredicateAttributeInt64 predicate)
    : UnaryOperator(input) {
  this->predicateAttribute = Select::PredicateAttribute::INT;
  this->intPredicate = predicate;
}

Select::Select(Operator& input, PredicateAttributeChar16 predicate)
    : UnaryOperator(input) {
  this->predicateAttribute = Select::PredicateAttribute::CHAR;
  this->charPredicate = predicate;
}

Select::Select(Operator& input, PredicateAttributeAttribute predicate)
    : UnaryOperator(input) {
  this->predicateAttribute = Select::PredicateAttribute::ATTRIBUTE;
  this->attributePredicate = predicate;
}

Select::~Select() = default;

void Select::open() { this->input->open(); }

bool Select::next() {
  if (this->input->next()) {
    this->output_registers.clear();
    std::vector<Register*> regs = this->input->get_output();
    switch (this->predicateAttribute) {
      case PredicateAttribute::INT:
        switch (this->intPredicate.predicate_type) {
          case Select::PredicateType::EQ: {
            if (*regs[this->intPredicate.attr_index] ==
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::NE: {
            if (*regs[this->intPredicate.attr_index] !=
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LT: {
            if (*regs[this->intPredicate.attr_index] <
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LE: {
            if (*regs[this->intPredicate.attr_index] <=
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GT: {
            if (*regs[this->intPredicate.attr_index] >
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GE: {
            if (*regs[this->intPredicate.attr_index] >=
                Register::from_int(this->intPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
        }
        break;
      case PredicateAttribute::CHAR:
        switch (this->charPredicate.predicate_type) {
          case Select::PredicateType::EQ: {
            Register str_reg =
                Register::from_string(this->charPredicate.constant);
            if (*regs[this->charPredicate.attr_index] == str_reg) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::NE: {
            if (*regs[this->charPredicate.attr_index] !=
                Register::from_string(this->charPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LT: {
            if (*regs[this->charPredicate.attr_index] <
                Register::from_string(this->charPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LE: {
            if (*regs[this->charPredicate.attr_index] <=
                Register::from_string(this->charPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GT: {
            if (*regs[this->charPredicate.attr_index] >
                Register::from_string(this->charPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GE: {
            if (*regs[this->charPredicate.attr_index] >=
                Register::from_string(this->charPredicate.constant)) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
        }
        break;
      case PredicateAttribute::ATTRIBUTE:
        switch (this->attributePredicate.predicate_type) {
          case Select::PredicateType::EQ: {
            if (*regs[this->attributePredicate.attr_left_index] ==
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::NE: {
            if (*regs[this->attributePredicate.attr_left_index] !=
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LT: {
            if (*regs[this->attributePredicate.attr_left_index] <
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::LE: {
            if (*regs[this->attributePredicate.attr_left_index] <=
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GT: {
            if (*regs[this->attributePredicate.attr_left_index] >
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
          case Select::PredicateType::GE: {
            if (*regs[this->attributePredicate.attr_left_index] >=
                *regs[this->attributePredicate.attr_right_index]) {
              for (auto& r : regs) {
                this->output_registers.push_back(*r);
              }
            }
          }
            return true;
            break;
        }
        break;
    }
  }
  return false;
}

void Select::close() { this->input->close(); }

std::vector<Register*> Select::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  return output;
}

Sort::Sort(Operator& input, std::vector<Criterion> criteria)
    : UnaryOperator(input) {
  this->criteria = criteria;
}

Sort::~Sort() = default;

void Sort::open() { this->input->open(); }

bool Sort::next() {
  this->output_registers.clear();
  if (!this->isMaterialized) {
    while (this->input->next()) {
      std::vector<Register*> regs = this->input->get_output();
      std::vector<Register> output_registers;
      output_registers.reserve(regs.size());
      for (auto& reg : regs) {
        output_registers.push_back(*reg);
      }
      this->registers.push_back(output_registers);
    }
    for (Criterion c : this->criteria) {
      if (c.desc) {
        std::sort(this->registers.begin(), this->registers.end(),
                  [&c](const std::vector<Register>& regs1,
                       const std::vector<Register>& regs2) {
                    return regs1[c.attr_index] > regs2[c.attr_index];
                  });
      }
    }
    this->isMaterialized = true;
  }
  if (this->curr_idx < this->registers.size()) {
    auto regs = this->registers[this->curr_idx];
    for (auto& reg : regs) {
      this->output_registers.push_back(reg);
    }
    ++this->curr_idx;
    return true;
  }
  return false;
}

std::vector<Register*> Sort::get_output() {
  std::vector<Register*> output;
  output.reserve(this->output_registers.size());
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  return output;
}

void Sort::close() { this->input->close(); }

HashJoin::HashJoin(Operator& input_left, Operator& input_right,
                   size_t attr_index_left, size_t attr_index_right)
    : BinaryOperator(input_left, input_right) {
  this->attr_index_left = attr_index_left;
  this->attr_index_right = attr_index_right;
}

HashJoin::~HashJoin() = default;

void HashJoin::open() {
  this->input_left->open();
  this->input_right->open();
}

bool HashJoin::next() {
  this->output_registers.clear();
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> registers_map;
    std::unordered_map<Register, std::vector<Register>, RegisterVectorHasher>
        left_set;
    std::unordered_set<Register, std::vector<Register>, RegisterVectorHasher>
        right_set;
    while (this->input_left->next()) {
      std::vector<Register> regs;
      for (auto& reg : this->input_left->get_output()) {
        regs.push_back(*reg);
      }
      left_set.insert(make_pair(regs[this->attr_index_left], regs));
    }

    // while (this->input_right->next()) {
    //   std::vector<Register> regs;
    //   for (auto& reg : this->input_right->get_output()) {
    //     regs.push_back(*reg);
    //   }
    //   left_set.insert({regs[this->attr_index_right], regs});
    // }

    // for (auto left_it : left_set) {
    //   auto got = right_set.find(left_it.first);
    //   if (got == right_set.end()) {
    //     registers_map.insert({left_it.first, 1});
    //   } else {
    //     registers_map[left_it.first] += 1;
    //   }

    //   if (got == right_set.end()) {
    //     std::vector<Register> temp_vec;
    //     temp_vec.insert(temp_vec.end(), left_it.second.begin(),
    //                     left_it.second.end());
    //     //temp_vec.insert(temp_vec.end(), right_it.second.begin(),
    //     //right_it.second.end());
    //     this->registers.push_back(temp_vec);
    //   }
    // }

    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const std::vector<Register>& regs1,
           const std::vector<Register>& regs2) { return regs1[0] < regs2[0];
           });

    this->isMaterialized = true;
  }

  if (this->curr_idx < static_cast<int>(this->registers.size())) {
    this->output_registers = this->registers[this->curr_idx];
    ++this->curr_idx;
    return true;
  }
  return false;
}

void HashJoin::close() {
  this->input_left->close();
  this->input_right->close();
}

std::vector<Register*> HashJoin::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  return output;
}

HashAggregation::HashAggregation(Operator& input,
                                 std::vector<size_t> group_by_attrs,
                                 std::vector<AggrFunc> aggr_funcs)
    : UnaryOperator(input) {
  this->group_by_attrs = std::move(group_by_attrs);
  this->aggr_funcs = std::move(aggr_funcs);
}

HashAggregation::~HashAggregation() = default;

void HashAggregation::open() { this->input->open(); }

bool HashAggregation::next() {
  this->output_registers.clear();
  std::optional<Register> minRegister;
  std::optional<Register> maxRegister;
  std::unordered_map<Register, int, RegisterHasher> countMap;
  std::unordered_map<Register, int, RegisterHasher> sumMap;
  if (!this->isMaterialized) {
    while (this->input->next()) {
      std::vector<Register*> regs = this->input->get_output();
      for (AggrFunc func : this->aggr_funcs) {
        switch (func.func) {
          case AggrFunc::MIN:
            if (!minRegister) {
              minRegister = *regs[func.attr_index];
            } else {
              if (*regs[func.attr_index] < *minRegister) {
                minRegister = *regs[func.attr_index];
              }
            }
            break;
          case AggrFunc::MAX:
            if (!maxRegister) {
              maxRegister = *regs[func.attr_index];
            } else {
              if (*regs[func.attr_index] > *maxRegister) {
                maxRegister = *regs[func.attr_index];
              }
            }
            break;
          case AggrFunc::COUNT:
            for (size_t attribute : this->group_by_attrs) {
              auto it = countMap.find(*regs[attribute]);
              if (it == countMap.end()) {
                countMap.insert({*regs[attribute], 1});
              } else {
                countMap[*regs[attribute]] += 1;
              }
            }
            break;
          case AggrFunc::SUM:
            for (size_t attr : this->group_by_attrs) {
              Register myreg = *regs[attr];
              auto it = sumMap.find(myreg);
              Register r = *regs[func.attr_index];
              if (it == sumMap.end()) {
                sumMap.insert({myreg, static_cast<int>(r.as_int())});
              } else {
                sumMap[myreg] += static_cast<int>(r.as_int());
              }
            }
            break;
        }
      }
    }
    if (sumMap.size() > 0) {
      std::vector<Register> keys;
      keys.reserve(sumMap.size());
      for (auto it : sumMap) {
        keys.push_back(it.first);
      }
      this->num_keys = static_cast<int>(keys.size());
      std::sort(keys.begin(), keys.end());
      for (auto& it : keys) {
        std::vector<Register> reg_vector;
        reg_vector.push_back(it);
        auto sumRegister = Register::from_int(sumMap[it]);
        reg_vector.push_back(sumRegister);
        auto countRegister = Register::from_int(countMap[it]);
        reg_vector.push_back(countRegister);
        this->registers_temp.push_back(reg_vector);
      }
    }
    this->isMaterialized = true;
  }
  if (minRegister) {
    this->output_registers.push_back(*minRegister);
    if (maxRegister) {
      this->output_registers.push_back(*maxRegister);
    }
    return true;
  }
  if (this->counter_idx < this->num_keys) {
    this->output_registers = this->registers_temp[this->counter_idx];
    ++this->counter_idx;
    return true;
  }
  return false;
};

void HashAggregation::close() { this->input->close(); }

std::vector<Register*> HashAggregation::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  return output;
}

Union::Union(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Union::~Union() = default;

void Union::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Union::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> registers_map;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (registers_map.find(*it) == registers_map.end()) {
          registers_map.insert({*it, 1});
        } else {
          registers_map[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (registers_map.find(*it) == registers_map.end()) {
          registers_map.insert({*it, 1});
        } else {
          registers_map[*it] += 1;
        }
      }
    }

    for (auto it : registers_map) {
      this->registers.push_back(it.first);
    }
    std::sort(this->registers.begin(), this->registers.end(),
              [](const Register& l, const Register& r) { return l < r; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> Union::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void Union::close() {
  this->input_left->close();
  this->input_right->close();
}

UnionAll::UnionAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

UnionAll::~UnionAll() = default;

void UnionAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool UnionAll::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> registers_map;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (registers_map.find(*it) == registers_map.end()) {
          registers_map.insert({*it, 1});
        } else {
          registers_map[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (registers_map.find(*it) == registers_map.end()) {
          registers_map.insert({*it, 1});
        } else {
          registers_map[*it] += 1;
        }
      }
    }

    for (auto it : registers_map) {
      int leftCount = it.second;
      for (int i = 0; i < leftCount; i++) {
        this->registers.push_back(it.first);
      }
    }
    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const Register& lhs, const Register& rhs) { return lhs < rhs; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> UnionAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void UnionAll::close() {
  this->input_left->close();
  this->input_right->close();
}

Intersect::Intersect(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Intersect::~Intersect() = default;

void Intersect::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Intersect::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> left_registers;
    std::unordered_map<Register, int, RegisterHasher> right_registers;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (left_registers.find(*it) == left_registers.end()) {
          left_registers.insert({*it, 1});
        } else {
          left_registers[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (right_registers.find(*it) == right_registers.end()) {
          right_registers.insert({*it, 1});
        } else {
          right_registers[*it] += 1;
        }
      }
    }

    for (auto it : left_registers) {
      if (right_registers.find(it.first) != right_registers.end()) {
        this->registers.push_back(it.first);
      }
    }
    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const Register& lhs, const Register& rhs) { return lhs < rhs; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> Intersect::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void Intersect::close() {
  this->input_left->close();
  this->input_right->close();
}

IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

IntersectAll::~IntersectAll() = default;

void IntersectAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool IntersectAll::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> left_registers;
    std::unordered_map<Register, int, RegisterHasher> right_registers;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (left_registers.find(*it) == left_registers.end()) {
          left_registers.insert({*it, 1});
        } else {
          left_registers[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (right_registers.find(*it) == right_registers.end()) {
          right_registers.insert({*it, 1});
        } else {
          right_registers[*it] += 1;
        }
      }
    }

    for (auto it : left_registers) {
      auto itr = right_registers.find(it.first);
      if (itr != right_registers.end()) {
        int leftCount = it.second;
        int rightCount = itr->second;
        if (leftCount <= rightCount) {
          for (int i = 0; i < leftCount; i++) {
            this->registers.push_back(it.first);
          }
        } else {
          for (int i = 0; i < rightCount; i++) {
            this->registers.push_back(itr->first);
          }
        }
      }
    }
    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const Register& lhs, const Register& rhs) { return lhs < rhs; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> IntersectAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void IntersectAll::close() {
  this->input_left->close();
  this->input_right->close();
}

Except::Except(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Except::~Except() = default;

void Except::open() {
  this->input_left->open();
  this->input_right->open();
}

bool Except::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> left_registers;
    std::unordered_map<Register, int, RegisterHasher> right_registers;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (left_registers.find(*it) == left_registers.end()) {
          left_registers.insert({*it, 1});
        } else {
          left_registers[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (right_registers.find(*it) == right_registers.end()) {
          right_registers.insert({*it, 1});
        } else {
          right_registers[*it] += 1;
        }
      }
    }

    for (auto it : left_registers) {
      if (right_registers.find(it.first) == right_registers.end()) {
        this->registers.push_back(it.first);
      }
    }
    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const Register& lhs, const Register& rhs) { return lhs < rhs; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> Except::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void Except::close() {
  this->input_left->close();
  this->input_right->close();
}

ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

ExceptAll::~ExceptAll() = default;

void ExceptAll::open() {
  this->input_left->open();
  this->input_right->open();
}

bool ExceptAll::next() {
  if (!this->isMaterialized) {
    std::unordered_map<Register, int, RegisterHasher> left_registers;
    std::unordered_map<Register, int, RegisterHasher> right_registers;
    while (this->input_left->next()) {
      std::vector<Register*> regs = this->input_left->get_output();
      for (auto it : regs) {
        if (left_registers.find(*it) == left_registers.end()) {
          left_registers.insert({*it, 1});
        } else {
          left_registers[*it] += 1;
        }
      }
    }

    while (this->input_right->next()) {
      std::vector<Register*> regs = this->input_right->get_output();
      for (auto it : regs) {
        if (right_registers.find(*it) == right_registers.end()) {
          right_registers.insert({*it, 1});
        } else {
          right_registers[*it] += 1;
        }
      }
    }

    for (auto it : left_registers) {
      if (right_registers.find(it.first) == right_registers.end()) {
        int leftCount = it.second;
        for (int i = 0; i < leftCount; i++) {
          this->registers.push_back(it.first);
        }
      } else {
        int leftCount = it.second;
        int rightCount = right_registers.find(it.first)->second;
        if ((leftCount - rightCount) > 0) {
          for (int i = 0; i < (leftCount - rightCount); i++) {
            this->registers.push_back(it.first);
          }
        }
      }
    }
    std::sort(
        this->registers.begin(), this->registers.end(),
        [](const Register& lhs, const Register& rhs) { return lhs < rhs; });
    this->isMaterialized = true;
  }
  if (this->counter_idx < static_cast<int>(this->registers.size())) {
    this->output_registers.push_back(this->registers[this->counter_idx]);
    this->counter_idx++;
    return true;
  } else {
    return false;
  }
}

std::vector<Register*> ExceptAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : this->output_registers) {
    output.push_back(&reg);
  }
  this->output_registers.clear();
  return output;
}

void ExceptAll::close() {
  this->input_left->close();
  this->input_right->close();
}

}  // namespace operators
}  // namespace buzzdb
