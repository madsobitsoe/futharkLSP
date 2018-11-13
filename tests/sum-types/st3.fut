-- Array of sum types.
-- ==
-- input { } 
-- output { [0,1,2,0,0,3,0,4] }

type foobar = #foo  | #bar i32

let foobar_func (xs : []foobar) : []i32 =
  let g x = match x
            case #foo   -> 0
            case #bar y -> y
  in map g xs

let main : []i32 = foobar_func [#foo, #bar 1, #bar 2, #foo, #foo, #bar 3, #foo, #bar 4]
