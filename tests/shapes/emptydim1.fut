-- ==
-- input { empty([0][0]i32) [[1]] } error: .
-- input { [[1]] empty([0][1]i32) } output { [[1]] empty([0][1]i32) }
-- input { [[1]] [[1,2]] } error: .
-- input { [[1]] [[2]] } output { [[1]] [[2]] }

let f [n] (xs: [][n]i32) = \(ys: [][n]i32) -> (xs, ys)

let main (xs: [][]i32) (ys: [][]i32) = f xs ys
