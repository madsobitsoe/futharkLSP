-- Basic size-parameterised type.
-- ==
-- input { 0 } output { empty(i32) }
-- input { 3 } output { [0,1,2] }

type ints [n] = [n]i32

let main(n: i32): ints [n] = iota n
