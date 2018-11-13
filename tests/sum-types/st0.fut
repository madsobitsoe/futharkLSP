-- Basic 1-ary sum types and matches.
-- ==
-- input { } 
-- output { 5 }

type foobar = #foo | #bar i32

let main : i32 =
  match #bar 5 : foobar
    case #foo   -> 1
    case #bar 1 -> 2
    case #bar x -> x
