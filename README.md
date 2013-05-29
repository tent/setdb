# SetDB

SetDB is an implementation of the Redis protocol that persists to LevelDB on
disk instead of memory. This allows the dataset size to grow beyond the bounds
of memory.

If you have leveldb in a non-standard path (git clone), you need to explicitly set CGO_{C,LD}FLAGS.
`CGO_CFLAGS="-I/path/to/leveldb/include" CGO_LDFLAGS="-L/path/to/leveldb" go get github.com/cupcake/setdb`