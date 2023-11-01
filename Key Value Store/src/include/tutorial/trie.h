#include <string>
#include <bits/stdc++.h>

namespace buzzdb {
namespace tutorial {

#define CHAR_SIZE 128

class Trie {
 public:
  bool isLeaf;
  Trie* character[CHAR_SIZE];
  std::vector<int> wordOccurenceIndex;
  Trie();
  ~Trie();

  void insert(std::string, int);
  std::string search(std::string, int);
};
}
}
