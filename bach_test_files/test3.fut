let main (x: []i32) (y: []i32):i32 =
  let (a: i32) = 2 in
  reduce (+) 0 (map2 (*) x y)
