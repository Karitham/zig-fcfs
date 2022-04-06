//! FCFS is a First Come First Served hash map implementation.
//!
//! This is mostly a study project, as this algorithm is not optimal
//! especially over something like robinhood or a hashbrown/swiss hash table.
//!
//! This implementation is based off closed hashing and a single array
//! of cells, which makes it optimized for lookups, but insertions might be slow.
//! 
//! The hashing function used only produces a 32 bit value, thus
//! the map can only hold up to 2^32 - 1 items.
//! To give a relative idea, it means you can store up to 4294967295 items
//! Since zig slices are a pointer (usize) and a length (usize), on x64 this means 2*64 bits.
//! For a `FCFS([]T, []V)` this means you can store up to 384GB of slice headers.
//! This should be fine for the ~5 coming years at the time of writing,
//! but is not future-proof, especially with smaller KV pairs.
const std = @import("std");

pub fn FCFS(comptime K: type, comptime V: type) type {
    return struct {
        seed: u32 = 0xc70f6907,
        alloc: std.mem.Allocator,
        max_load_factor: f16 = 0.70,
        map_buf: []?K = undefined,
        value_buf: []?V = undefined,
        load: usize = 0,
        iter_off: usize = 0,

        const Self = @This();

        /// Cell represents a key value pair.
        pub const Cell = struct {
            key: K,
            val: V,
        };

        pub fn init(alloc: std.mem.Allocator) !Self {
            return Self{
                .seed = std.crypto.random.int(u32),
                .alloc = alloc,
                .map_buf = try alloc.alloc(?K, 64),
                .value_buf = try alloc.alloc(?V, 64),
            };
        }

        /// deinit the map's memory
        pub fn deinit(self: *Self) void {
            self.alloc.free(self.map_buf);
            self.alloc.free(self.value_buf);
        }

        /// puts a new value in the map, overwriting any existing value
        /// returns the old value if any existed
        pub fn put(self: *Self, key: K, value: V) std.mem.Allocator.Error!?V {
            if (@intToFloat(f16, self.load) / @intToFloat(f16, self.map_buf.len) > self.max_load_factor) try self.rehash(self.map_buf.len * 2);

            var kh = std.hash.Murmur3_32.hashWithSeed(std.mem.asBytes(&key), self.seed) % self.map_buf.len;
            var old: ?V = null;

            // If there's already a key here, we need to move aside until there's no more key there
            while (self.map_buf[kh]) |val| : (kh = (kh + 1) % self.map_buf.len) {
                if (std.mem.eql(u8, std.mem.asBytes(&val), std.mem.asBytes(&key))) {
                    old = self.value_buf[kh].?;
                    break;
                }
            }

            self.map_buf[kh] = key;
            self.value_buf[kh] = value;
            if (old == null) self.load += 1;
            return old;
        }

        /// creates a new internal buffer, and copies the cells over
        /// resets all other properties such that iterators are invalidated
        fn rehash(self: *Self, new_size: usize) std.mem.Allocator.Error!void {
            var nm = try self.alloc.alloc(?K, new_size);
            errdefer self.alloc.free(nm);
            var vm = try self.alloc.alloc(?V, new_size);
            errdefer self.alloc.free(vm);

            var new = Self{
                .seed = self.seed,
                .alloc = self.alloc,
                .map_buf = nm,
                .value_buf = vm,
            };

            for (self.map_buf) |v, i| {
                if (v) |val| _ = try new.put(val, self.value_buf[i].?);
            }

            self.alloc.free(self.map_buf);
            self.alloc.free(self.value_buf);

            self.* = new;
        }

        /// delete a key from the map
        /// returns the old value if any existed
        pub fn delete(self: *Self, key: K) ?V {
            const old_index = null;

            const keyBytes = std.mem.asBytes(&key);
            var kh = std.hash.Murmur2_32ur3_32.hashWithSeed(keyBytes, self.seed) % self.map_buf.len;

            while (self.map_buf[kh]) |val| : (kh = (kh + 1) % self.map_buf.len) {
                if (std.mem.eql(u8, std.mem.asBytes(&val), keyBytes)) old_index = kh;
            }

            const val = if (old_index != null) self.value_buf[old_index] else return;

            std.mem.copy(K, self.map_buf[old_index..kh], self.map_buf[old_index + 1 .. kh]);
            std.mem.copy(K, self.value_buf[old_index..kh], self.value_buf[old_index + 1 .. kh]);

            self.map_buf[kh] = undefined;
            self.value_buf[kh] = undefined;

            self.load -= 1;
            return val;
        }

        /// lookup the index of a key in the map
        fn lookup(self: *Self, key: K) ?usize {
            const keyBytes = std.mem.asBytes(&key);
            var kh = std.hash.Murmur3_32.hashWithSeed(keyBytes, self.seed) % self.map_buf.len;

            while (self.map_buf[kh]) |val| : (kh = (kh + 1) % self.map_buf.len) {
                if (std.mem.eql(u8, std.mem.asBytes(&val), keyBytes)) return kh;
            }

            return null;
        }

        /// get returns the value associated with the key, or null if it doesn't exist
        pub fn get(self: *Self, key: K) ?V {
            return if (self.lookup(key)) |i| self.value_buf[i].? else null;
        }

        /// next returns the next value in the map
        /// iteration is unordered.
        /// if the map grows, the iterator is invalidated
        pub fn next(self: *Self) ?Cell {
            if (self.load == 0) return null;
            if (self.iter_off >= self.map_buf.len) return null;

            while (self.iter_off < self.map_buf.len) : (self.iter_off += 1) {
                if (self.map_buf[self.iter_off]) |k| {
                    defer self.iter_off += 1;
                    return Cell{ .key = k, .val = self.value_buf[self.iter_off].? };
                }
            }
            return null;
        }

        /// reset the iterator
        pub fn reset(self: *Self) void {
            self.iter_off = 0;
        }
    };
}

test "fcfs" {
    if (std.os.getenv("BENCH") == null) return;

    const op_count = 10000;

    var map = try FCFS(f32, u32).init(std.testing.allocator);
    defer map.deinit();

    // this should grow the map a few times.
    var key: f32 = 24.5;
    var i: u32 = 0;
    const insert_start_t = std.time.nanoTimestamp();
    while (i < op_count) : (i += 1) {
        _ = try map.put(key, i);
        key += 0.5;
    }
    const insert_end_t = std.time.nanoTimestamp();

    var j: usize = 0;
    const iter_start_t = std.time.nanoTimestamp();
    while (map.next()) |_| j += 1;
    const iter_end_t = std.time.nanoTimestamp();

    if (j != i) std.log.err("got {} values expected {}", .{ j, i });

    std.debug.print("bench:\n", .{});
    std.debug.print(
        "fcfs insert: {} ns/op\tfcfs iter: {} ns/op\n",
        .{
            @divTrunc(insert_end_t - insert_start_t, @intCast(i128, i)),
            @divTrunc(iter_end_t - iter_start_t, @intCast(i128, i)),
        },
    );
}
