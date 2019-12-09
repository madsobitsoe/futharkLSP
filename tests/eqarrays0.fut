-- Equality checking of arrays.
--
-- More subtle than it looks, as you also have to compare the dimensions.
-- ==
-- input { empty([0]i32) empty([0]i32) }
-- output { true }
-- input { empty([0]i32) [1] }
-- output { false }
-- input { [1] empty([0]i32) }
-- output { false }
-- input { [1,2] [1,2] }
-- output { true }
-- input { [1,2] [3,4] }
-- output { false }

let main (xs: []i32) (ys: []i32) = xs == ys
