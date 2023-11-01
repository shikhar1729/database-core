
#include "buffer/buffer_manager.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "common/macros.h"
#include "storage/file.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

BufferFrame::BufferFrame(uint64_t page_id, bool is_dirty, uint64_t frame_size) {
  this->page_id = page_id;
  this->dirty = is_dirty;
  this->cnt = 1;
  if (this->data.size() == 0) {
    this->data.resize((frame_size / sizeof(uint64_t)), 0);
  }
}

char* BufferFrame::get_data() {
  return reinterpret_cast<char*>(this->data.data());
}

BufferManager::BufferManager(size_t page_size, size_t page_count)
    : page_size(page_size), page_count(page_count) {}

BufferManager::~BufferManager() {
  /// Write dirty pages in fifo buffer to file

  for (auto page : fifoBuffer) {
    if (page.getDirty()) {
      std::string file_name =
          std::to_string(BufferManager::get_segment_id(page.getPageID()));
      auto file = File::open_file(file_name.c_str(), File::WRITE);
      uint64_t offSet = get_segment_page_id(page.getPageID()) * page_size;
      file->write_block(page.get_data(), offSet, page_size);
    }
  }

  /// Write dirty pages in lru buffer to file
  for (auto page : lruBuffer) {
    if (page.getDirty()) {
      std::string file_name =
          std::to_string(BufferManager::get_segment_id(page.getPageID()));
      auto file = File::open_file(file_name.c_str(), File::WRITE);
      uint64_t offSet = get_segment_page_id(page.getPageID()) * page_size;
      file->write_block(page.get_data(), offSet, page_size);
    }
  }
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
  bool pageFoundInLRUorFIFO = false;

  // // Check if page exists in either LRU or FIFO
  qLock.lock();

  if (lruBuffer.size() > 0) {
    for (auto page : lruBuffer) {
      if (page.get_page_id() == page_id) {
        pageFoundInLRUorFIFO = true;
        break;
      }
    }
  }
  qLock.unlock();

  qLock.lock();

  if (fifoBuffer.size() > 0) {
    for (auto page : fifoBuffer) {
      if (page.get_page_id() == page_id) {
        pageFoundInLRUorFIFO = true;
        break;
      }
    }
  }
  qLock.unlock();

  std::vector<uint64_t> data;

  // If page is not in LRU or FIFO Buffer
  if (!pageFoundInLRUorFIFO) {
    data.resize(page_size / sizeof(uint64_t), 0);
    std::string file_name =
        std::to_string(BufferManager::get_segment_id(page_id));
    auto file = File::open_file(file_name.c_str(), File::WRITE);
    uint64_t offSet = BufferManager::get_segment_page_id(page_id) * page_size;
    file->read_block(offSet, page_size, reinterpret_cast<char*>(data.data()));
  }

  if (exclusive) {
    page_lock.lock();
  } else {
    page_lock.lock_shared();
  }

  BufferFrame newPage(page_id, false, page_size);
  newPage.setExclusive(exclusive);

  if (data.size() > 0) {
    newPage.set_data(data);
  }

  if (fifoBuffer.size() == 0 && lruBuffer.size() == 0) {
    qLock.lock();
    fifoBuffer.push_back(newPage);
    qLock.unlock();
    if (!exclusive) {
      page_lock.unlock_shared();
    }
    return fifoBuffer.back();
  }

  // Find page in LRU Buffer
  for (size_t i = 0; i < lruBuffer.size(); i++) {
    if (lruBuffer[i].get_page_id() == page_id) {
      newPage.setDirty(lruBuffer[i].getDirty());
      newPage.set_data(lruBuffer[i].get_vector_data());
      qLock.lock();
      lruBuffer.erase(lruBuffer.begin() + i);
      lruBuffer.push_back(newPage);
      qLock.unlock();

      if (!exclusive) {
        page_lock.unlock_shared();
      }
      return lruBuffer.back();
    }
  }

  // Find page in FIFO Buffer
  for (size_t i = 0; i < fifoBuffer.size(); i++) {
    if (fifoBuffer[i].get_page_id() == page_id) {
      // Move the page from FIFO to LRU if there is space
      if (lruBuffer.size() + fifoBuffer.size() < page_count) {
        newPage.setDirty(fifoBuffer[i].getDirty());
        newPage.set_data(fifoBuffer[i].get_vector_data());
        qLock.lock();
        fifoBuffer.erase(fifoBuffer.begin() + i);
        lruBuffer.push_back(newPage);
        qLock.unlock();

        if (!exclusive) {
          page_lock.unlock_shared();
        }
        return lruBuffer.back();
      }

      for (size_t i = 0; i < lruBuffer.size(); i++) {
        // If the page is unfixed
        if (lruBuffer[i].getCount() == 0) {
          // If the page is not dirty
          if (!lruBuffer[i].getDirty()) {
            qLock.lock();
            lruBuffer.erase(lruBuffer.begin() + i);
            lruBuffer.push_back(newPage);
            qLock.unlock();
            if (!exclusive) {
              page_lock.unlock_shared();
            }
          } else {
            qLock.lock();
            uint64_t foundPageId = lruBuffer[i].get_page_id();
            std::vector<uint64_t> foundPageData =
                lruBuffer[i].get_vector_data();

            lruBuffer.erase(lruBuffer.begin() + i);
            lruBuffer.push_back(newPage);
            qLock.unlock();

            if (exclusive) {
              page_lock.unlock();
            } else {
              page_lock.unlock_shared();
            }

            // Write back the dirty page to disk

            std::string file_name =
                std::to_string(BufferManager::get_segment_id(foundPageId));
            auto file = File::open_file(file_name.c_str(), File::WRITE);
            uint64_t offSet = get_segment_page_id(foundPageId) * page_size;
            file->write_block(reinterpret_cast<char*>(foundPageData.data()),
                              offSet, page_size);
            if (exclusive) {
              page_lock.lock();
            }
          }
          return lruBuffer.back();
        }
      }
    }
  }

  // If buffer is not full
  if (fifoBuffer.size() + lruBuffer.size() < page_count) {
    qLock.lock();

    fifoBuffer.push_back(newPage);
    qLock.unlock();

    if (!exclusive) {
      page_lock.unlock_shared();
    }

    return fifoBuffer.back();
  } else {
    // Find free slot if buffer is full
    for (size_t i = 0; i < fifoBuffer.size(); i++) {
      //  If the page is unfixed

      if (fifoBuffer[i].getCount() == 0) {
        // If the page is not dirty
        if (!fifoBuffer[i].getDirty()) {
          qLock.lock();

          fifoBuffer.erase(fifoBuffer.begin() + i);
          fifoBuffer.push_back(newPage);
          qLock.unlock();

          if (!exclusive) {
            page_lock.unlock_shared();
          }

        } else {
          qLock.lock();

          uint64_t foundPageId = fifoBuffer[i].get_page_id();

          std::vector<uint64_t> foundPageData = fifoBuffer[i].get_vector_data();

          fifoBuffer.erase(fifoBuffer.begin() + i);
          fifoBuffer.push_back(newPage);
          qLock.unlock();

          if (exclusive) {
            page_lock.unlock();
          } else {
            page_lock.unlock_shared();
          }

          std::string file_name =
              std::to_string(BufferManager::get_segment_id(foundPageId));
          auto file = File::open_file(file_name.c_str(), File::WRITE);
          uint64_t offSet = get_segment_page_id(foundPageId) * page_size;
          file->write_block(reinterpret_cast<char*>(foundPageData.data()),
                            offSet, page_size);
          if (exclusive) {
            page_lock.lock();
          }
        }

        return fifoBuffer.back();
      }
    }
  }

  if (exclusive) {
    page_lock.unlock();
  } else {
    page_lock.unlock_shared();
  }

  throw buffer_full_error{};
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
  qLock.lock();
  for (size_t i = 0; i < fifoBuffer.size(); i++) {
    if (fifoBuffer[i].get_page_id() == page.get_page_id()) {
      fifoBuffer[i].setCount(0);
      fifoBuffer[i].setDirty(is_dirty);
      break;
    }
  }
  qLock.unlock();

  qLock.lock();

  for (size_t i = 0; i < lruBuffer.size(); i++) {
    if (lruBuffer[i].get_page_id() == page.get_page_id()) {
      lruBuffer[i].setCount(0);
      lruBuffer[i].setDirty(is_dirty);
      break;
    }
  }

  if (page.getExclusive()) {
    page_lock.unlock();
  }
  qLock.unlock();
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
  std::vector<uint64_t> fifo_list;
  qLock.lock();
  for (auto page : this->fifoBuffer) {
    fifo_list.push_back(page.get_page_id());
  }
  qLock.unlock();
  return fifo_list;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
  std::vector<uint64_t> lru_list;
  for (auto page : this->lruBuffer) {
    lru_list.push_back(page.get_page_id());
  }
  return lru_list;
}

}  // namespace buzzdb
