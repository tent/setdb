# SetDB

SetDB is an implementation of the Redis protocol that persists to LevelDB on
disk instead of memory. This allows the dataset size to grow beyond the bounds
of memory.
