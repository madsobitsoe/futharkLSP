-- Nested sum types.
-- ==
-- input { } 
-- output { 4 }

type foobar   = #foo  | #bar i32
type doopfoop = #doop | #foop foobar

let main : i32 =
  match (#foop (#bar 5) : doopfoop) 
  case #doop          -> 1
  case #foop #foo     -> 2
  case #foop (#bar 6) -> 3
  case #foop (#bar 5) -> 4
  case _              -> 5
