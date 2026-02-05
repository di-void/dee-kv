## Fixes
- log `truncate()` function is too naive and inefficient
- log helper functions (i.e `get_entry_term()`, `get_entries_from()`, `find_first_index_of_term()`) 
  - They all go straight to disk, but writes are buffered. Disk may report stale state where current state is needed
