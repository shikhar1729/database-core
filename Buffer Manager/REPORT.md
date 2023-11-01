# Buffer Manager Implementation

## Overview

This design aims to implement a Buffer Manager for managing memory buffers effectively. The Buffer Manager is responsible for loading, storing, and managing pages in memory, providing a layer of abstraction between the application and physical storage. The project consists of a C++ class library with header and source files (`buffer_manager.h` and `buffer_manager.cpp`) that define the BufferManager and BufferFrame classes.

## Design Decisions

1. **BufferFrame Class**: The `BufferFrame` class represents a single page in memory. It holds essential information about the page, including the page ID, whether it is dirty (modified), whether it is exclusively locked, and the page's data. A key design decision was to use a vector to store page data, allowing flexibility in handling different page sizes.

2. **BufferManager Class**: The `BufferManager` class manages a collection of `BufferFrame` objects. It uses two deque containers, `lruBuffer` for Least Recently Used pages and `fifoBuffer` for First-In-First-Out pages, to implement 2Q page replacement policy. The Buffer Manager supports thread-safe operations with shared and exclusive locks using `std::shared_mutex`.

3. **Page Loading**: When a page is requested, the Buffer Manager checks if it's already in memory (in either the LRU or FIFO buffer). If not, it loads the page from disk into memory.

4. **Page Unfixing**: When a page is no longer needed, it can be "unfixed" using the `unfix_page` function. If the page is marked as dirty, it is written back to disk before being released (This happens when evicting the page, as a part of fix_page function).

5. **FIFO and LRU Lists**: The `get_fifo_list` and `get_lru_list` functions provide lists of page IDs currently held in the FIFO and LRU buffers, respectively.

## Missing Components

1. **List of locks**: The code implementation is missing a comprehensive set of locks, one for each BufferFrame. Currently it uses a single lock at the BufferManager level.
2. **Individual locks for FIFO and LRU**: Having individual locks for FIFO and LRU would allow us to have efficient locking mechanisms in place while accessing the queues.


## Conclusion

The Buffer Manager project provides a foundation for managing pages in memory efficiently. However, it requires further development, including comprehensive testing, documentation, exception handling, and integration with other components, to be a complete and robust solution. Further refinement and enhancements will ensure the Buffer Manager's reliability and usability in real-world applications.