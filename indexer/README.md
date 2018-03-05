

# Indexing

The curl package might need you to preinstall
`libssl-dev`. On Ubuntu/Debian this can be installed
by running the following command.

	sudo apt-get install pkg-config libssl-dev

Common-crawl is split in 80 shards of 165 GB each.
You do not need to download all of the shards to get something
working. 1/10 of a shard is already searchable.

Create a directory that will host all of these.
From the `indexer` directory, run 

	cargo run --release -- --index <yourindexdirectory> --shard <shardid>


`shard_id` must be a number between 1 and 80.

Indexing is committed every `100 wet files`. If you interrupt indexing, and resume
it (by running the same command), indexing will resume from the last commit.
