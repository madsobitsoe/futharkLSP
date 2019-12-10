let foo [n] (A: [n][n]i32) =
  let on_row row i = let padding = replicate n 0
                     let padding[i] = 10
                     in concat row padding
  in map2 on_row A (iota n)

let main [n] (As: [][n][n]i32) = map foo As
