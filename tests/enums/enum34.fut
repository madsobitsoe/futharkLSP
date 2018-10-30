-- Missing pattern warning 4; intended behaviour is to not print the warning with
-- superfluous parentheses.
-- ==
-- warning: unmatched

type foobar = #foo | #bar

let f : i32 =
  match #foo : foobar
    case (((((#foo))))) -> 1
