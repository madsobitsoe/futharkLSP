let main [n][m] (world: [n][m]i32): [n][m]i32 =
  map2 (\c i -> if i == n-1 then world[n-1] else c) world (iota n)
