-- Function on sum type.
-- ==
-- input { } 
-- output { 1 }

type foobar = #foo  | #bar i32

let f (x : foobar) : i32 = 
  match x
  case #foo   -> 0
  case #bar y -> y

let main = f (#bar 1)

