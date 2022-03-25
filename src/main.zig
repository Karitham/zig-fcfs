const std = @import("std");
const testing = std.testing;

pub fn FCFS(comptime K: type, comptime V: type) type {
    return struct {
        seed: u32 = 0xc70f6907,
        alloc: std.mem.Allocator,
        max_load_factor: f16 = 0.75,
        map_buf: []?Cell = undefined,
        load: usize = 0,

        const Self = @This();

        const Cell = struct {
            key: K,
            val: V,
        };

        pub fn init(alloc: std.mem.Allocator) !Self {
            return Self{
                .seed = std.crypto.random.int(u32),
                .alloc = alloc,
                .map_buf = try alloc.alloc(?Cell, 64),
            };
        }

        pub fn deinit(self: *Self) void {
            self.alloc.free(self.map_buf);
        }

        /// puts a new value in the map, overwriting any existing value
        /// returns the old value if it existed
        pub fn put(self: *Self, key: K, value: V) std.mem.Allocator.Error!?V {
            if (@intToFloat(f16, self.load) / @intToFloat(f16, self.map_buf.len) > self.max_load_factor) {
                try self.rehash(self.load * 2);
            }

            var kh = std.hash.Murmur3_32.hashWithSeed(std.mem.asBytes(&key), self.seed) % self.map_buf.len;

            // If there's already a key here, we need to move aside until there's no more key there
            while (self.map_buf[kh] != null) {
                if (self.map_buf[kh]) |cell| {
                    if (std.mem.eql(u8, std.mem.asBytes(&cell.key), std.mem.asBytes(&key))) {
                        const old = cell.val;

                        self.map_buf[kh] = Cell{
                            .key = key,
                            .val = value,
                        };

                        return old;
                    }
                }

                kh = (kh + 1) % self.map_buf.len;
            }

            self.load += 1;
            self.map_buf[kh] = Cell{
                .key = key,
                .val = value,
            };

            return null;
        }

        fn rehash(self: *Self, new_size: usize) std.mem.Allocator.Error!void {
            var nm = try self.alloc.alloc(?Cell, new_size);
            errdefer self.alloc.free(nm);

            var new = Self{
                .seed = self.seed,
                .alloc = self.alloc,
                .map_buf = nm,
            };

            for (self.map_buf) |c| {
                if (c) |cell| _ = try new.put(cell.key, cell.val);
            }

            self.alloc.free(self.map_buf);
            self.map_buf = new.map_buf;
        }

        pub fn get(self: *Self, key: K) ?V {
            var kh = std.hash.Murmur3_32.hashWithSeed(std.mem.asBytes(&key), self.seed) % self.map_buf.len;

            while (self.map_buf[kh] != null) {
                if (self.map_buf[kh]) |cell| {
                    if (std.mem.eql(u8, std.mem.asBytes(&cell.key), std.mem.asBytes(&key))) return cell.val;
                }

                kh = (kh + 1) % self.map_buf.len;
            }
            return null;
        }
    };
}

test "fcfs" {
    var map = try FCFS(f32, u32).init(std.testing.allocator);
    defer map.deinit();

    var key: f32 = 24.5;
    var i: u32 = 0;

    // this should grow the map a few times.
    while (i < 10000) : (i += 1) {
        _ = try map.put(key, i);
        key += 0.5;
    }

    try std.testing.expectEqual(map.get(24.5 + 50 * 0.5), 50);
}
