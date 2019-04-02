-- ==
-- input { 0 } output { [[[0]]] }

let insert [n] 't (x: t) (a: [n]t) (i: i32): []t =
  let (b,c) = split i a   in b ++ [x] ++ c

let list_insertions [n] 't (x: t) (a: [n]t): [][]t =
  map (insert x a) (0...(length a))

let main (x: i32) = map (list_insertions x) [[]]
