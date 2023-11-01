#pragma once

#include <string>

namespace buzzdb {
namespace tutorial {

class CommandExecutor {
 public:
  CommandExecutor();

  /// Execute a command
  /// Return the appropriate output as a string
  std::string execute(std::string);
  std::string isValid(std::string);

};

}  // namespace tutorial
}  // namespace buzzdb
