#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

template <typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
  struct Node {
    /// The level in the tree.
    uint16_t level;

    /// The number of children.
    uint16_t count;

    // Constructor
    Node(uint16_t level, uint16_t count) : level(level), count(count) {}

    /// Is the node a leaf node?
    bool is_leaf() const { return level == 0; }
    std::optional<uint64_t> parentPageId;
  };

  struct InnerNode : public Node {
    /// The capacity of a node.
    static constexpr uint32_t kCapacity =
        (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));

    /// The keys.
    KeyT keys[kCapacity];

    /// The children.
    uint64_t children[kCapacity];

    /// Constructor.
    InnerNode() : Node(0, 0) {}

    /// Get the index of the first key that is not less than than a provided
    /// key.
    /// @param[in] key          The key that should be searched.
    std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
      if (this->count == 2 && !ComparatorT()(this->keys[0], key)) {
        return std::make_pair(0, true);
      }

      int low = 0;
      int high = this->count - 1;
      std::optional<uint32_t> index;

      while (low <= high) {
        int mid = low + (high - low) / 2;

        if (key == this->keys[mid]) {
          return std::make_pair(static_cast<uint32_t>(mid), true);
        } else if (key > this->keys[mid]) {
          low = mid + 1;
        } else {
          index = mid;
          high = mid - 1;
        }
      }

      return std::make_pair(index.has_value() ? *index : 0, index.has_value());
    }

    /// Insert a key.
    /// @param[in] key          The separator that should be inserted.
    /// @param[in] split_page   The id of the split page that should be
    /// inserted.
    void insert(const KeyT &key, uint64_t split_page,
                const std::optional<uint64_t> &leftNode) {
      std::vector<KeyT> keyVector;
      std::vector<uint64_t> childrenVector;
      std::optional<int> index;
      for (int i = 0; i < this->count - 1; i++) {
        keyVector.push_back(this->keys[i]);
        childrenVector.push_back(this->children[i]);
        if (!ComparatorT()(this->keys[i], key) && !index) {
          index = i;
        }
        this->keys[i] = 0;
        this->children[i] = 0;
      }
      /// push the last element in the children array
      childrenVector.push_back(this->children[this->count - 1]);
      this->children[this->count - 1] = 0;
      if (!index) {
        keyVector.insert(keyVector.begin() + (this->count - 1), key);
        childrenVector.insert(childrenVector.begin() + this->count, split_page);
      } else {
        keyVector.insert(keyVector.begin() + *index, key);
        if (leftNode) {
          index = *index + 1;
        }
        childrenVector.insert(childrenVector.begin() + *index, split_page);
      }
      for (int i = 0; i < static_cast<int>(keyVector.size()); i++) {
        this->keys[i] = keyVector[i];
      }
      for (int i = 0; i < static_cast<int>(childrenVector.size()); i++) {
        this->children[i] = childrenVector[i];
      }
      this->count++;
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(std::byte *buffer) {
      auto mid_idx = 1 + ((this->kCapacity - 1) / 2);
      auto newInnerNode = reinterpret_cast<InnerNode *>(buffer);
      int startIndex = 1;
      /// calculate a separator key
      auto separator_key =
          static_cast<int>((this->keys[mid_idx] + this->keys[mid_idx - 1]) / 2);
      for (int i = mid_idx; i < static_cast<int>(this->kCapacity) + 1; i++) {
        newInnerNode->keys[startIndex - 1] = this->keys[i - 1];
        newInnerNode->children[startIndex] = this->children[i];
        this->keys[i - 1] = 0;
        this->children[i] = 0;
        this->count--;
        startIndex++;
      }

      newInnerNode->count = startIndex;
      return separator_key;
    }
  };
  struct LeafNode : public Node {
    /// The capacity of a node.
    static constexpr uint32_t kCapacity =
        (PageSize / (sizeof(KeyT) + sizeof(ValueT))) - 2;

    /// The keys.
    KeyT keys[kCapacity];

    /// The values.
    ValueT values[kCapacity];

    /// Constructor.
    LeafNode() : Node(0, 0) {}

    /// Insert a key.
    /// @param[in] key          The key that should be inserted.
    /// @param[in] value        The value that should be inserted.
    void insert(const KeyT &key, const ValueT &value) {
      if (this->count == 0) {
        this->keys[this->count] = key;
        this->values[this->count] = value;
        this->count++;
      } else {
        std::vector<KeyT> keyVector;
        std::vector<KeyT> valVector;
        std::optional<int> index;
        for (int i = 0; i < this->count; i++) {
          keyVector.push_back(this->keys[i]);
          valVector.push_back(this->values[i]);
          if (!ComparatorT()(this->keys[i], key) && !index) {
            index = i;
          }
          this->keys[i] = 0;
          this->values[i] = 0;
        }

        if (!index) {
          index = this->count;
          if (keyVector[*index - 1] == key) {
            valVector[*index - 1] = value;
          } else {
            keyVector.insert(keyVector.begin() + *index, key);
            valVector.insert(valVector.begin() + *index, value);
            this->count++;
          }
        } else {
          if (keyVector[*index] == key) {
            valVector[*index] = value;
          } else {
            keyVector.insert(keyVector.begin() + *index, key);
            valVector.insert(valVector.begin() + *index, value);
            this->count++;
          }
        }

        for (int i = 0; i < static_cast<int>(keyVector.size()); i++) {
          this->keys[i] = keyVector[i];
          this->values[i] = valVector[i];
        }
      }
    }

    /// Erase a key.
    void erase(int &index) {
      // delete add shift other elements to left
      for (int i = index; i < this->count; ++i) {
        this->keys[i] = this->keys[i + 1];
        this->values[i] = this->values[i + 1];
      }
      this->keys[this->count] = 0;
      this->values[this->count] = 0;
      this->count--;
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(std::byte *buffer) {
      auto mid_idx = (this->kCapacity + 1) / 2;
      auto newLeafNode = reinterpret_cast<LeafNode *>(buffer);
      int index = 0;
      for (int i = mid_idx; i < static_cast<int>(this->kCapacity); i++) {
        newLeafNode->keys[index] = this->keys[i];
        newLeafNode->values[index] = this->values[i];
        this->keys[i] = 0;
        this->values[i] = 0;
        this->count--;
        index++;
      }
      newLeafNode->count = index + 1;
      return this->keys[index - 1];
    }
  };

  /// The root.
  std::optional<uint64_t> root;

  bool isTreeEmpty;

  uint16_t rootLevel = 0;

  std::map<KeyT, bool> erasedKeys;

  /// Next page id.
  /// You don't need to worry about about the page allocation.
  /// Just increment the next_page_id whenever you need a new page.
  uint64_t next_page_id;

  /// Constructor.
  BTree(uint16_t segment_id, BufferManager &buffer_manager)
      : Segment(segment_id, buffer_manager) {
    this->isTreeEmpty = true;
  }

  /// Lookup an entry in the tree.
  /// @param[in] key      The key that should be searched.
  std::optional<ValueT> lookup(const KeyT &key) {
    bool keyFound = false;
    std::optional<ValueT> foundKey;
    if (this->erasedKeys.find(key) != this->erasedKeys.end() || !this->root) {
      return foundKey;
    }

    auto currPageId = *this->root;
    int next = 0;
    uint64_t previousParentPageId = currPageId;
    while (!keyFound) {
      auto &current_page = this->buffer_manager.fix_page(currPageId, false);
      auto current_node = reinterpret_cast<Node *>(current_page.get_data());

      /// If the current node is a leaf node
      if (current_node->is_leaf()) {
        auto leaf_node = reinterpret_cast<LeafNode *>(current_node);
        for (int i = 0; i < leaf_node->count; i++) {
          if (leaf_node->keys[i] == key) {
            foundKey = leaf_node->values[i];
            return foundKey;
          }
        }

        /// Check the children of parent node
        currPageId = previousParentPageId;
        next++;
      } else
      /// If the current node is an internal node
      {
        auto inner_node = reinterpret_cast<InnerNode *>(current_node);
        if (current_node->parentPageId) {
          previousParentPageId = *current_node->parentPageId;
        }
        /// move to next node
        std::pair<uint32_t, bool> lower_bound = inner_node->lower_bound(key);
        if (lower_bound.second) {
          if (inner_node->count < next) {
            return foundKey;
          } else {
            currPageId = inner_node->children[lower_bound.first + next];
            next = 0;
          }
        } else {
          currPageId = inner_node->children[inner_node->count - 1];
        }
      }
      this->buffer_manager.unfix_page(current_page, false);
    }
    return foundKey;
  }

  /// Erase an entry in the tree.
  /// @param[in] key      The key that should be searched.
  void erase(const KeyT &key) {
    if (this->root) {
      bool keyFound = false;
      auto currPageId = *this->root;
      int next = 0;
      uint64_t previousParentPageId = currPageId;
      while (!keyFound) {
        auto &current_page = this->buffer_manager.fix_page(currPageId, true);
        auto current_node = reinterpret_cast<Node *>(current_page.get_data());
        if (current_node->is_leaf()) {
          auto leaf_node = reinterpret_cast<LeafNode *>(current_node);
          for (int i = 0; i < leaf_node->count; i++) {
            if (leaf_node->keys[i] == key) {
              leaf_node->erase(i);
              this->erasedKeys.insert(std::pair<KeyT, bool>(key, true));
              keyFound = true;
              break;
            }
          }
          currPageId = previousParentPageId;
          next++;
        } else {
          auto inner_node = reinterpret_cast<InnerNode *>(current_node);
          if (current_node->parentPageId) {
            previousParentPageId = *current_node->parentPageId;
          }
          /// move to next node
          std::pair<uint32_t, bool> lower_bound = inner_node->lower_bound(key);
          if (lower_bound.second) {
            if (inner_node->count < next) {
              keyFound = true;
            } else {
              currPageId = inner_node->children[lower_bound.first + next];
              next = 0;
            }
          } else {
            currPageId = inner_node->children[inner_node->count - 1];
          }
        }
        this->buffer_manager.unfix_page(current_page, true);
      }
    }
  }

  /// Inserts a new entry into the tree.
  /// @param[in] key      The key that should be inserted.
  /// @param[in] value    The value that should be inserted.
  void insert(const KeyT &key, const ValueT &value) {
    if (this->isTreeEmpty) {
      this->root = 0;
      this->next_page_id = 1;
      this->isTreeEmpty = false;
    }
    auto current_node_page_id = *this->root;
    bool isKeyInserted = false;

    while (!isKeyInserted) {
      auto &current_page =
          this->buffer_manager.fix_page(current_node_page_id, true);
      auto current_node = reinterpret_cast<Node *>(current_page.get_data());
      /// if root node is a leaf node
      if (current_node->is_leaf()) {
        auto leaf_node = static_cast<LeafNode *>(current_node);
        /// if there is space to insert the key
        if (leaf_node->count < leaf_node->kCapacity) {
          leaf_node->insert(key, value);
          this->buffer_manager.unfix_page(current_page, true);
          isKeyInserted = true;
        } else
        /// no space to insert the key
        {
          /// create new leaf node
          auto new_leaf_page_id = this->next_page_id;
          this->next_page_id++;
          auto &new_leaf_page =
              this->buffer_manager.fix_page(new_leaf_page_id, true);

          /// split the current leaf node
          KeyT separator_key = leaf_node->split(
              reinterpret_cast<std::byte *>(new_leaf_page.get_data()));
          auto new_node = reinterpret_cast<Node *>(new_leaf_page.get_data());
          auto new_leaf_node = static_cast<LeafNode *>(new_node);

          /// if separator_key >= key
          /// add new key to left subtree, otherwise add new key to right
          /// subtree
          if (!ComparatorT()(separator_key, key)) {
            leaf_node->insert(key, value);
          } else {
            new_leaf_node->insert(key, value);
          }

          /// check if current node has a parent
          if (!leaf_node->parentPageId) {
            /// root has new page id
            this->root = this->next_page_id;
            this->next_page_id++;
            auto &new_root_node_page =
                this->buffer_manager.fix_page(*this->root, true);
            auto new_inner_root_node =
                reinterpret_cast<Node *>(new_root_node_page.get_data());
            auto new_root_node = static_cast<InnerNode *>(new_inner_root_node);
            /// increase level by 1
            rootLevel += 1;
            new_root_node->level = rootLevel;
            new_root_node->keys[0] = separator_key;
            new_root_node->children[0] = current_node_page_id;
            new_root_node->children[1] = new_leaf_page_id;
            new_root_node->count += 2;
            leaf_node->parentPageId = *this->root;
            new_leaf_node->parentPageId = *this->root;

            this->buffer_manager.unfix_page(new_leaf_page, true);
            this->buffer_manager.unfix_page(new_root_node_page, true);
          } else {
            /// add separator_key into existing parent node
            auto &parent_node_page =
                this->buffer_manager.fix_page(*leaf_node->parentPageId, true);
            auto parent_node =
                reinterpret_cast<Node *>(parent_node_page.get_data());
            auto parent_inner_node = static_cast<InnerNode *>(parent_node);

            std::optional<uint64_t> leftNode;
            if (leaf_node->values[leaf_node->count - 1] <
                new_leaf_node->values[0]) {
              leftNode = current_node_page_id;
            }
            parent_inner_node->insert(separator_key, new_leaf_page_id,
                                      leftNode);

            new_leaf_node->parentPageId = *leaf_node->parentPageId;
            this->buffer_manager.unfix_page(parent_node_page, true);
          }
          this->buffer_manager.unfix_page(current_page, true);
          isKeyInserted = true;
        }
      } else {
        auto inner_node = static_cast<InnerNode *>(current_node);
        /// check capacity of inner node
        if (inner_node->count == (inner_node->kCapacity + 1)) {
          auto new_inner_node_page_id = this->next_page_id;
          this->next_page_id++;
          auto &new_inner_node_page =
              this->buffer_manager.fix_page(new_inner_node_page_id, true);
          KeyT separator_key = inner_node->split(
              reinterpret_cast<std::byte *>(new_inner_node_page.get_data()));
          auto new_node =
              reinterpret_cast<Node *>(new_inner_node_page.get_data());
          auto new_inner_node = static_cast<InnerNode *>(new_node);
          new_inner_node->level = inner_node->level;
          
          for (int i = 0; i < new_inner_node->count; i++) {
            auto &child = this->buffer_manager.fix_page(
                new_inner_node->children[i], true);
            auto child_node = reinterpret_cast<Node *>(child.get_data());
            auto child_inner_node = static_cast<InnerNode *>(child_node);
            child_inner_node->parentPageId = new_inner_node_page_id;
          }
          /// check if current inner node has a parent
          if (!inner_node->parentPageId) {
            /// root has new page id
            this->root = this->next_page_id;
            this->next_page_id++;
            auto &new_root_node_page =
                this->buffer_manager.fix_page(*this->root, true);
            auto new_root_node =
                reinterpret_cast<Node *>(new_root_node_page.get_data());
            auto new_root_inner_node = static_cast<InnerNode *>(new_root_node);
            /// increase level by 1
            rootLevel += 1;
            new_root_inner_node->level = rootLevel;
            /// add first key
            new_root_inner_node->keys[0] = separator_key;
            /// add children to first key
            new_root_inner_node->children[0] = current_node_page_id;
            new_root_inner_node->children[1] = new_inner_node_page_id;
            /// increase children number
            new_root_inner_node->count += 2;
            /// set parent page id for both inner nodes
            inner_node->parentPageId = *this->root;
            new_inner_node->parentPageId = *this->root;

            this->buffer_manager.unfix_page(new_root_node_page, true);
            /// move to next node
            std::pair<uint32_t, bool> lower_bound =
                new_root_inner_node->lower_bound(key);
            if (lower_bound.second) {
              /// go to lhs child
              current_node_page_id =
                  new_root_inner_node->children[lower_bound.first];
            } else {
              /// go to rhs child
              current_node_page_id =
                  new_root_inner_node->children[new_root_inner_node->count - 1];
            }
          } else {
            /// get the parent node of the current node
            auto &parent_node_page =
                this->buffer_manager.fix_page(*inner_node->parentPageId, true);
            auto parent_node =
                reinterpret_cast<Node *>(parent_node_page.get_data());
            auto parent_inner_node = static_cast<InnerNode *>(parent_node);
            std::optional<uint64_t> leftNode;
            parent_inner_node->insert(separator_key, new_inner_node_page_id,
                                      leftNode);
            new_inner_node->parentPageId = *inner_node->parentPageId;

            /// move to next node
            std::pair<uint32_t, bool> lower_bound =
                parent_inner_node->lower_bound(key);
            if (lower_bound.second) {
              /// go to lhs child
              current_node_page_id =
                  parent_inner_node->children[lower_bound.first];
            } else {
              /// go to rhs child
              current_node_page_id =
                  parent_inner_node->children[parent_inner_node->count - 1];
            }
            this->buffer_manager.unfix_page(parent_node_page, true);
          }
          this->buffer_manager.unfix_page(new_inner_node_page, true);
        } else {
          std::pair<uint32_t, bool> lower_bound = inner_node->lower_bound(key);
          if (lower_bound.second) {
            current_node_page_id = inner_node->children[lower_bound.first];
          } else {
            current_node_page_id = inner_node->children[inner_node->count - 1];
          }
        }
        this->buffer_manager.unfix_page(current_page, true);
      }
    }
  }
};

}  // namespace buzzdb
