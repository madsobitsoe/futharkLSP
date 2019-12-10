.. _language-reference:

Language Reference
==================

This reference seeks to describe every construct in the Futhark
language.  It is not presented in a tutorial fashion, but rather
intended for quick lookup and documentation of subtleties.  For this
reason, it is not written in a bottom-up manner, and some concepts may
be used before they are fully defined.  It is a good idea to have a
basic grasp of Futhark (or some other functional programming language)
before reading this reference.  An ambiguous grammar is given for the
full language.  The text describes how ambiguities are resolved in
practice (for example by applying rules of operator precedence).

This reference describes only the language itself.  Documentation for
the basis library is `available elsewhere
<https://futhark-lang.org/docs/>`_.

Identifiers and Keywords
------------------------

.. productionlist::
   id: `letter` (`letter` | "_" | "'")* | "_" `id`
   quals: (`id` ".")+
   qualid: `id` | `quals` `id`
   binop: `opstartchar` `opchar`*
   qualbinop: `binop` | `quals` `binop` | "`" `qualid` "`"
   fieldid: `decimal` | `id`
   opstartchar: "+" | "-" | "*" | "/" | "%" | "=" | "!" | ">" | "<" | "|" | "&" | "^"
   opchar: `opstartchar` | "."
   constructor: "#" `id`

Many things in Futhark are named. When we are defining something, we
give it an unqualified name (`id`).  When referencing something inside
a module, we use a qualified name (`qualid`).  The constructor names
of a sum type (:ref:`compounds`) are identifiers prefixed with ``#``,
with no space afterwards.  The fields of a record are named with
`fieldid`.  Note that a `fieldid` can be a decimal number.  Futhark
has three distinct name spaces: terms, module types, and types.
Modules (including parametric modules) and values both share the term
namespace.

.. _primitives:

Primitive Types and Values
--------------------------

.. productionlist::
   literal: `intnumber` | `floatnumber` | "true" | "false"

Boolean literals are written ``true`` and ``false``.  The primitive
types in Futhark are the signed integer types ``i8``, ``i16``,
``i32``, ``i64``, the unsigned integer types ``u8``, ``u16``, ``u32``,
``u64``, the floating-point types ``f32``, ``f64``, as well as
``bool``.  An ``f32`` is always a single-precision float and a ``f64``
is a double-precision float.

.. productionlist::
   int_type: "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64"
   float_type: "f8" | "f16" | "f32" | "f64"

Numeric literals can be suffixed with their intended type.  For
example ``42i8`` is of type ``i8``, and ``1337e2f64`` is of type
``f64``.  If no suffix is given, the type of the literal will be
inferred based on its use.  If the use is not constrained, integral
literals will be assigned type ``i32``, and decimal literals type
``f64``.  Hexadecimal literals are supported by prefixing with ``0x``,
and binary literals by prefixing with ``0b``.

Floats can also be written in hexadecimal format such as ``0x1.fp3``,
instead of the usual decimal notation. Here, ``0x1.f`` evaluates to
``1 15/16`` and the ``p3`` multiplies it by ``2^3 = 8``.

.. productionlist::
   intnumber: (`decimal` | `hexadecimal` | `binary`) [`int_type`]
   decimal: `decdigit` (`decdigit` |"_")*
   hexadecimal: 0 ("x" | "X") `hexdigit` (`hexdigit` |"_")*
   binary: 0 ("b" | "B") `bindigit` (`bindigit` | "_")*

.. productionlist::
   floatnumber: (`pointfloat` | `exponentfloat`) [`float_type`]
   pointfloat: [`intpart`] `fraction`
   exponentfloat: (`intpart` | `pointfloat`) `exponent`
   hexadecimalfloat: 0 ("x" | "X") `hexintpart` `hexfraction` ("p"|"P") ["+" | "-"] `decdigit`+
   intpart: `decdigit` (`decdigit` |"_")*
   fraction: "." `decdigit` (`decdigit` |"_")*
   hexintpart: `hexdigit` (`hexdigit` | "_")*
   hexfraction: "." `hexdigit` (`hexdigit` |"_")*
   exponent: ("e" | "E") ["+" | "-"] `decdigit`+

.. productionlist::
   decdigit: "0"..."9"
   hexdigit: `decdigit` | "a"..."f" | "A"..."F"
   bindigit: "0" | "1"

.. _compounds:

Compound Types and Values
~~~~~~~~~~~~~~~~~~~~~~~~~

.. productionlist::
   type:   `qualid`
       : | `array_type`
       : | `tuple_type`
       : | `record_type`
       : | `sum_type`
       : | `function_type`
       : | `type_application`

Compound types can be constructed based on the primitive types.  The
Futhark type system is entirely structural, and type abbreviations are
merely shorthands.  The only exception is abstract types whose
definition has been hidden via the module system (see `Module
System`_).

.. productionlist::
   tuple_type: "(" ")" | "(" `type` ("[" "," `type` "]")* ")"

A tuple value or type is written as a sequence of comma-separated
values or types enclosed in parentheses.  For example, ``(0, 1)`` is a
tuple value of type ``(i32,i32)``.  The elements of a tuple need not
have the same type -- the value ``(false, 1, 2.0)`` is of type
``(bool, i32, f64)``.  A tuple element can also be another tuple, as
in ``((1,2),(3,4))``, which is of type ``((i32,i32),(i32,i32))``.  A
tuple cannot have just one element, but empty tuples are permitted,
although they are not very useful.  Empty tuples are written ``()``
and are of type ``()``.

.. productionlist::
   array_type: "[" [`dim`] "]" `type`
   dim: `qualid` | `decimal`

An array value is written as a sequence of zero or more
comma-separated values enclosed in square brackets: ``[1,2,3]``.  An
array type is written as ``[d]t``, where ``t`` is the element type of
the array, and ``d`` is an integer indicating the size.  We typically
elide ``d``, in which case the size will be inferred.  As an example,
an array of three integers could be written as ``[1,2,3]``, and has
type ``[3]i32``.  An empty array is written as ``[]``, and its type is
inferred from its use.  When writing Futhark values for such uses as
``futhark test`` (but not when writing programs), empty arrays are
written ``empty([0]t)`` for an empty array of type ``[0]t``.  All
dimensions must be given a size, and at least one must be zero,
e.g. ``empty([2][0]i32)``.

Multi-dimensional arrays are supported in Futhark, but they must be
*regular*, meaning that all inner arrays must have the same shape.
For example, ``[[1,2], [3,4], [5,6]]`` is a valid array of type
``[3][2]i32``, but ``[[1,2], [3,4,5], [6,7]]`` is not, because there
we cannot come up with integers ``m`` and ``n`` such that
``[m][n]i32`` describes the array.  The restriction to regular arrays
is rooted in low-level concerns about efficient compilation.  However,
we can understand it in language terms by the inability to write a
type with consistent dimension sizes for an irregular array value.  In
a Futhark program, all array values, including intermediate (unnamed)
arrays, must be typeable.

.. productionlist::
   sum_type: `constructor` `type`* ("|" `constructor` `type`*)*

Sum types are anonymous in Futhark, and are written as the
constructors separated by vertical bars.  Each constructor consists of
a ``#``-prefixed *name*, followed by zero or more types, called its
*payload*.  **Note:** The current implementation of sum types is
fairly inefficient, in that all possible constructors of a sum-typed
value will be resident in memory.  Avoid using sum types where
multiple constructors have large payloads.  Further, there is an
implementation weakness where arrays of sum types with an array
payload may result in incorrect size inference and run-time errors.
Try to avoid these for now.

.. productionlist::
   record_type: "{" "}" | "{" `fieldid` ":" `type` ("," `fieldid` ":" `type`)* "}"

Records are mappings from field names to values, with the field names
known statically.  A tuple behaves in all respects like a record with
numeric field names, and vice versa.  It is an error for a record type
to name the same field twice.

.. productionlist::
   type_application: `type` `type_arg` | "*" `type`
   type_arg: "[" [`dim`] "]" | `type`

A parametric type abbreviation can be applied by juxtaposing its name
and its arguments.  The application must provide as many arguments as
the type abbreviation has parameters - partial application is
presently not allowed.  See `Type Abbreviations`_ for further details.

.. productionlist::
   function_type: `param_type` "->" `type`
   param_type: `type` | "(" `id` ":" `type` ")"

Functions are classified via function types, but they are not fully
first class.  See `Higher-order functions`_ for the details.

.. productionlist::
   stringlit: '"' `stringchar` '"'
   stringchar: <any source character except "\" or newline or quotes>

String literals are supported, but only as syntactic sugar for UTF-8
encoded arrays of ``u8`` values.  There is no character type in
Futhark.

Declarations
------------

A Futhark file or module consists of a sequence of declarations.  Each
declaration is processed in order, and a declaration can only refer to
names bound by preceding declarations.

.. productionlist::
   dec:   `fun_bind` | `val_bind` | `type_bind` | `mod_bind` | `mod_type_bind`
      : | "open" `mod_exp`
      : | "import" `stringlit`
      : | "local" `dec`

The ``open`` declaration brings names defined in another module into
scope (see also `Module System`_).  For the meaning of ``import``, see
`Referring to Other Files`_.  If a declaration is prefixed with
``local``, whatever names it defines will *not* be visible outside the
current module.  In particular ``local open`` is used to bring names
from another module into scope, without making those names available
to users of the module being defined.  In most cases, using module
type ascription is a better idea.

Declaring Functions and Values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. productionlist::
   fun_bind:   ("let" | "entry") (`id` | "(" `binop` ")") `type_param`* `pat`+ [":" `type`] "=" `exp`
           : | ("let" | "entry") `pat` `binop` `pat` [":" `type`] "=" `exp`

.. productionlist::
   val_bind: "let" `id` [":" `type`] "=" `exp`

Functions and values must be defined before they are used.  A function
declaration must specify the name, parameters, and body
of the function::

  let name params...: rettype = body

Hindley-Milner-style type inference is supported.  A parameter may be
given a type with the notation ``(name: type)``.  Functions may not be
recursive.  Optionally, the programmer may put *shape declarations* in
the return type and parameter types; see `Shape Declarations`_.  A
function can be *polymorphic* by using type parameters, in the same
way as for `Type Abbreviations`_::

  let reverse [n] 't (xs: [n]t): [n]t = xs[::-1]

Type parameters for a function do not need to cover the types of all
parameters.  The type checker will add more if necessary.  For
example, the following is well typed::

  let pair 'a (x: a) y = (x, y)

A new type variable will be invented for the parameter ``y``.

Shape and type parameters are not passed explicitly when calling
function, but are automatically derived.  If an array value *v* is
passed for a type parameter *t*, all other arguments passed of type
*t* must have the same shape as *v*.  For example, consider the following
definition::

  let pair 't (x: t) (y: t) = (x, y)

The application ``pair [1] [2,3]`` will fail at run-time.

To simplify the handling of in-inplace updates (see
:ref:`in-place-updates`), the value returned by a function may not
alias any global variables.

User-Defined Operators
~~~~~~~~~~~~~~~~~~~~~~

Infix operators are defined much like functions::

  let (p1: t1) op (p2: t2): rt = ...

For example::

  let (a:i32,b:i32) +^ (c:i32,d:i32) = (a+c, b+d)

We can also define operators by enclosing the operator name in
parentheses and suffixing the parameters, as an ordinary function::

  let (+^) (a:i32,b:i32) (c:i32,d:i32) = (a+c, b+d)

This is necessary when defining a polymorphic operator.

A valid operator name is a non-empty sequence of characters chosen
from the string ``"+-*/%=!><&^"``.  The fixity of an operator is
determined by its first characters, which must correspond to a
built-in operator.  Thus, ``+^`` binds like ``+``, whilst ``*^`` binds
like ``*``.  The longest such prefix is used to determine fixity, so
``>>=`` binds like ``>>``, not like ``>``.

It is not permitted to define operators with the names ``&&`` or
``||`` (although these as prefixes are accepted).  This is because a
user-defined version of these operators would not be short-circuiting.
User-defined operators behave exactly like ordinary functions, except
for bbeing infix.

A built-in operator can be shadowed (i.e. a new ``+`` can be defined).
This will result in the built-in polymorphic operator becoming
inaccessible, except through the ``intrinsics`` module.

An infix operator can also be defined with prefix notation, like an
ordinary function, by enclosing it in parentheses::

  let (+) (x: i32) (y: i32) = x - y

This is necessary when defining operators that take type or shape
parameters.

.. _entry-points:

Entry Points
~~~~~~~~~~~~

Apart from declaring a function with the keyword ``let``, it can also
be declared with ``entry``.  When the Futhark program is compiled any
top-level function declared with ``entry`` will be exposed as an entry
point.  If the Futhark program has been compiled as a library, these
are the functions that will be exposed.  If compiled as an executable,
you can use the ``--entry-point`` command line option of the generated
executable to select the entry point you wish to run.

Any top-level function named ``main`` will always be considered an
entry point, whether it is declared with ``entry`` or not.

Value Declarations
~~~~~~~~~~~~~~~~~~

A named value/constant can be declared as follows::

  let name: type = definition

The definition can be an arbitrary expression, including function
calls and other values, although they must be in scope before the
value is defined.

Shape Declarations
~~~~~~~~~~~~~~~~~~

Whenever a pattern occurs (in ``let``, ``loop``, and function
parameters), as well as in return types, *shape declarations* may be
used to express invariants about the shapes of arrays
that are accepted or produced by the function.  For example::

  let f [n] (a: [n]i32) (b: [n]i32): [n]i32 =
    map (+) a b

We use a *shape parameter*, ``[n]``, to explicitly quantify the names
of shapes.  The ``[n]`` parameter need not be explicitly passed when
calling ``f``.  Rather, its value is implicitly deduced from the
arguments passed for the value parameters.  Any size parameter must be
used in a value parameter.  This is an error::

  let f [n] (x: i32) = n

A shape declaration can also be an integer constant (with no suffix).
The dimension names bound can be used as ordinary variables within the
scope of the parameters.  If a function is called with arguments, or
returns a value, that does not fulfill the shape constraints, the
program will fail with a runtime error.  Likewise, if a pattern with
shape declarations is attempted bound to a value that does not fulfill
the invariants, the program will fail with a runtime error.  For
example, this will fail::

  let x: [3]i32 = iota 2

While this will succeed and bind ``n`` to ``2``::

  let [n] x: [n]i32 = iota 2

Type Abbreviations
~~~~~~~~~~~~~~~~~~

.. productionlist::
   type_bind: "type" `id` `type_param`* "=" `type`
   type_param: "[" `id` "]" | "'" `id` | "'^" `id`

Type abbreviations function as shorthands for the purpose of
documentation or brevity.  After a type binding ``type t1 = t2``, the
name ``t1`` can be used as a shorthand for the type ``t2``.  Type
abbreviations do not create distinct types: the types ``t1`` and
``t2`` are entirely interchangeable.

A type abbreviation can have zero or more parameters.  A type
parameter enclosed with square brackets is a *shape parameter*, and
can be used in the definition as an array dimension size, or as a
dimension argument to other type abbreviations.  When passing an
argument for a shape parameter, it must be enclosed in square
brackets.  Example::

  type two_intvecs [n] = ([n]i32, [n]i32)

  let x: two_intvecs [2] = (iota 2, replicate 2 0)

Shape parameters work much like shape declarations for arrays.  Like
shape declarations, they can be elided via square brackets containing
nothing.

A type parameter prefixed with a single quote is a *type parameter*.
It is in scope as a type in the definition of the type abbreviation.
Whenever the type abbreviation is used in a type expression, a type
argument must be passed for the parameter.  Type arguments need not be
prefixed with single quotes::

  type two_vecs [n] 't = ([n]t, [n]t)
  type two_intvecs [n] = two_vecs [n] i32
  let x: two_vecs [2] i32 = (iota 2, replicate 2 0)

A type parameter prefixed with ``'^`` is a *lifted type parameter*.
These may be instantiated with types that may be functions.  On the
other hand, values of such types are subject to the same restrictions
as function types (cannot be put in an arrays, returned from ``if``,
or used as a ``loop`` parameter; see `Higher-order functions`_).

Expressions
-----------

Expressions are the basic construct of any Futhark program.  An
expression has a statically determined *type*, and produces a *value*
at runtime.  Futhark is an eager/strict language ("call by value").

The basic elements of expressions are called *atoms*, for example
literals and variables, but also more complicated forms.

.. productionlist::
   atom:   `literal`
       : | `qualid` ("." `fieldid`)*
       : | `stringlit`
       : | "(" ")"
       : | "(" `exp` ")" ("." `fieldid`)*
       : | "(" `exp` ("," `exp`)* ")"
       : | "{" "}"
       : | "{" field ("," `field`)* "}"
       : | `qualid` "[" `index` ("," `index`)* "]"
       : | "(" `exp` ")" "[" `index` ("," `index`)* "]"
       : | `quals` "." "(" `exp` ")"
       : | "[" `exp` ("," `exp`)* "]"
       : | "[" `exp` [".." `exp`] "..." `exp` "]"
       : | "(" `qualbinop` ")"
       : | "(" `exp` `qualbinop` ")"
       : | "(" `qualbinop` `exp` ")"
       : | "(" ( "." `field` )+ ")"
       : | "(" "." "[" `index` ("," `index`)* "]" ")"
   exp:   `atom`
      : | `exp` `qualbinop` `exp`
      : | `exp` `exp`
      : | `constructor` `exp`*
      : | `exp` ":" `type`
      : | `exp` [ ".." `exp` ] "..." `exp`
      : | `exp` [ ".." `exp` ] "..<" `exp`
      : | `exp` [ ".." `exp` ] "..>" `exp`
      : | "if" `exp` "then" `exp` "else" `exp`
      : | "let" `pat` "=" `exp` "in" `exp`
      : | "let" `id` "[" `index` ("," `index`)* "]" "=" `exp` "in" `exp`
      : | "let" `id` `type_param`* `pat`+ [":" `type`] "=" `exp` "in" `exp`
      : | "(" "\" `pat`+ [":" `type`] "->" `exp` ")"
      : | "loop" `pat` [("=" `exp`)] `loopform` "do" `exp`
      : | "unsafe" `exp`
      : | "assert" `atom` `atom`
      : | `exp` "with" "[" `index` ("," `index`)* "]" "=" `exp`
      : | `exp` "with" `fieldid` ("." `fieldid`)* "=" `exp`
      : | "match" `exp` ("case" `pat` "->" `exp`)+
   field:   `fieldid` "=" `exp`
        : | `id`
   pat:   `id`
      : | `literal`
      : |  "_"
      : | "(" ")"
      : | "(" `pat` ")"
      : | "(" `pat` ("," `pat`)+ ")"
      : | "{" "}"
      : | "{" `fieldid` ["=" `pat`] ["," `fieldid` ["=" `pat`]] "}"
      : | `constructor` `pat`*
      : | `pat` ":" `type`
   loopform :   "for" `id` "<" `exp`
            : | "for" `pat` "in" `exp`
            : | "while" `exp`
   index:   `exp` [":" [`exp`]] [":" [`exp`]]
        : | [`exp`] ":" `exp` [":" [`exp`]]
        : | [`exp`] [":" `exp`] ":" [`exp`]

Some of the built-in expression forms have parallel semantics, but it
is not guaranteed that the the parallel constructs in Futhark are
evaluated in parallel, especially if they are nested in complicated
ways.  Their purpose is to give the compiler as much freedom and
information is possible, in order to enable it to maximise the
efficiency of the generated code.

Resolving Ambiguities
~~~~~~~~~~~~~~~~~~~~~

The above grammar contains some ambiguities, which in the concrete
implementation is resolved via a combination of lexer and grammar
transformations.  For ease of understanding, they are presented here
in natural text.

* An expression ``x.y`` may either be a reference to the name ``y`` in
  the module ``x``, or the field ``y`` in the record ``x``.  Modules
  and values occupy the same name space, so this is disambiguated by
  the type of ``x``.

* A type ascription (``exp : type``) cannot appear as an array
  index, as it conflicts with the syntax for slicing.

* In ``f [x]``, there is am ambiguity between indexing the array ``f``
  at position ``x``, or calling the function ``f`` with the singleton
  array ``x``.  We resolve this the following way:

    * If there is a space between ``f`` and the opening bracket, it is
      treated as a function application.

    * Otherwise, it is an array index operation.

* An expression ``(-x)`` is parsed as the variable ``x`` negated and
  enclosed in parentheses, rather than an operator section partially
  applying the infix operator ``-``.

* The following table describes the precedence and associativity of
  infix operators.  All operators in the same row have the same
  precedence.  The rows are listed in increasing order of precedence.
  Note that not all operators listed here are used in expressions;
  nevertheless, they are still used for resolving ambiguities.

  =================  =============
  **Associativity**  **Operators**
  =================  =============
  left               ``,``
  left               ``:``
  left               ``||``
  left               ``&&``
  left               ``<=`` ``>=`` ``>`` ``<`` ``==`` ``!=``
  left               ``&`` ``^`` ``|``
  left               ``<<`` ``>>``
  left               ``+`` ``-``
  left               ``*`` ``/`` ``%`` ``//`` ``%%``
  left               ``|>``
  right              ``<|``
  right              ``->``
  left               juxtaposition
  =================  =============

.. _patterns:

Patterns
~~~~~~~~

We say that a pattern is *irrefutable* if it can never fail to match a
value of the appropriate type.  Concretely, this means that it does
not require any specific sum type constructor (unless the type in
question has only a single constructor), or any specific numeric or
boolean literal.  Patterns used in function parameters and ``let``
bindings must be irrefutable.  Patterns used in ``case`` need not be
irrefutable.

A pattern ``_`` matches any value.  A pattern consisting of a literal
value (e.g. a numeric constant) matches exactly that value.

Semantics of Simple Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`literal`
.........

Evaluates to itself.

`qualid`
........

A variable name; evaluates to its value in the current environment.

`stringlit`
...........

Evaluates to an array of type ``[]i32`` that contains the code points
of the characters as integers.

``()``
......

Evaluates to an empty tuple.

``( e )``
.........

Evaluates to the result of ``e``.

``(e1, e2, ..., eN)``
.....................

Evaluates to a tuple containing ``N`` values.  Equivalent to the
record literal ``{1=e1, 2=e2, ..., N=eN}``.

``{f1, f2, ..., fN}``
.....................

A record expression consists of a comma-separated sequence of *field
expressions*.  Each field expression defines the value of a field in
the record.  A field expression can take one of two forms:

  ``f = e``: defines a field with the name ``f`` and the value
  resulting from evaluating ``e``.

  ``f``: defines a field with the name ``f`` and the value of the
  variable ``f`` in scope.

Each field may only be defined once.

``a[i]``
........

Return the element at the given position in the array.  The index may
be a comma-separated list of indexes instead of just a single index.
If the number of indices given is less than the rank of the array, an
array is returned.

The array ``a`` must be a variable name or a parenthesized expression.
Futhermore, there *may not* be a space between ``a`` and the opening
bracket.  This disambiguates the array indexing ``a[i]``, from ``a
[i]``, which is a function call with a literal array.

``a[i:j:s]``
............

Return a slice of the array ``a`` from index ``i`` to ``j``, the
former inclusive and the latter exclusive, taking every ``s``-th
element.  The ``s`` parameter may not be zero.  If ``s`` is negative,
it means to start at ``i`` and descend by steps of size ``s`` to ``j``
(not inclusive).

It is generally a bad idea for ``s`` to be non-constant.
Slicing of multiple dimensions can be done by separating with commas,
and may be intermixed freely with indexing.

If ``s`` is elided it defaults to ``1``.  If ``i`` or ``j`` is elided, their
value depends on the sign of ``s``.  If ``s`` is positive, ``i`` become ``0``
and ``j`` become the length of the array.  If ``s`` is negative, ``i`` becomes
the length of the array minus one, and ``j`` becomes minus one.  This means that
``a[::-1]`` is the reverse of the array ``a``.

``[x, y, z]``
.............

Create an array containing the indicated elements.  Each element must
have the same type and shape.

``x..y...z``
..............

Construct an integer array whose first element is ``x`` and which
proceeds stride of ``y-x`` until reaching ``z`` (inclusive).  The
``..y`` part can be elided in which case a stride of 1 is used.  The
stride may not be zero.  An empty array is returned in cases where
``z`` would never be reached or ``x`` and ``y`` are the same value.

``x..y..<z``
............

Construct an integer array whose first elements is ``x``, and which
proceeds upwards with a stride of ``y`` until reaching ``z``
(exclusive).  The ``..y`` part can be elided in which case a stride of
1 is used.  An empty array is returned in cases where ``z`` would
never be reached or ``x`` and ``y`` are the same value.

``x..y..>z``
...............

Construct an integer array whose first elements is ``x``, and which
proceeds downwards with a stride of ``y`` until reaching ``z``
(exclusive).  The ``..y`` part can be elided in which case a stride of
-1 is used.  An empty array is returned in cases where ``z`` would
never be reached or ``x`` and ``y`` are the same value.

``e.f``
........

Access field ``f`` of the expression ``e``, which must be a record or
tuple.

``m.(e)``
.........

Evaluate the expression ``e`` with the module ``m`` locally opened, as
if by ``open``.  This can make some expressions easier to read and
write, without polluting the global scope with a declaration-level
``open``.

``x`` *binop* ``y``
...................

Apply an operator to ``x`` and ``y``.  Operators are functions like
any other, and can be user-defined.  Futhark pre-defines certain
"magical" *overloaded* operators that work on many different types.
Overloaded functions cannot be defined by the user.  Both operands
must have the same type.  The predefined operators and their semantics
are:

  ``**``

    Power operator, defined for all numeric types.

  ``//``, ``%%``

    Division and remainder on integers, with rounding towards zero.

  ``*``, ``/``, ``%``, ``+``, ``-``

    The usual arithmetic operators, defined for all numeric types.
    Note that ``/`` and ``%`` rounds towards negative infinity when
    used on integers - this is different from in C.

  ``^``, ``&``, ``|``, ``>>``, ``<<``

    Bitwise operators, respectively bitwise xor, and, or, arithmetic
    shift right and left, and logical shift right.  Shift amounts
    must be non-negative and the operands must be integers.  Note
    that, unlike in C, bitwise operators have *higher* priority than
    arithmetic operators.  This means that ``x & y == z`` is
    understood as ``(x & y) == z``, rather than ``x & (y == z)`` as
    it would in C.  Note that the latter is a type error in Futhark
    anyhow.

  ``==``, ``!=``

      Compare any two values of builtin or compound type for equality.

  ``<``, ``<=``.  ``>``, ``>=``

      Company any two values of numeric type for equality.

``x && y``
..........

Short-circuiting logical conjunction; both operands must be of type
``bool``.

``x || y``
..........

Short-circuiting logical disjunction; both operands must be of type
``bool``.

``f x``
.......

Apply the function ``f`` to the argument ``x``.

``#c x y z``
............

Apply the sum type constructor ``#x`` to the payload ``x``, ``y``, and
``z``.  A constructor application is always assumed to be saturated,
i.e. its entire payload provided.  This means that constructors may
not be partially applied.

``e : t``
.........

Annotate that ``e`` is expected to be of type ``t``, failing with a
type error if it is not.  If ``t`` is an array with shape
declarations, the correctness of the shape declarations is checked at
run-time.

Due to ambiguities, this syntactic form cannot appear as an array
index expression unless it is first enclosed in parentheses.  However,
as an array index must always be of type ``i32``, there is never a
reason to put an explicit type ascription there.

``! x``
.......

Logical negation if ``x`` is of type ``bool``.  Bitwise negation if
``x`` is of integral type.

``- x``
.......

Numerical negation of ``x``, which must be of numeric type.

``unsafe e``
............

Elide safety checks and assertions (such as bounds checking) that
occur during execution of ``e``.  This is useful if the compiler is
otherwise unable to avoid bounds checks (e.g. when using indirect
indexes), but you really do not want them there.  Make very sure that
the code is correct; eliding such checks can lead to memory
corruption.

``assert cond e``
.................

Terminate execution with an error if ``cond`` evaluates to false,
otherwise produce the result of evaluating ``e``.  Unless ``e``
produces a value that is used subsequently (it can just be a
variable), dead code elimination may remove the assertion.

``a with [i] = e``
...................

Return ``a``, but with the element at position ``i`` changed to
contain the result of evaluating ``e``.  Consumes ``a``.

``r with f = e``
.................

Return the record ``r``, but with field `f` changed to have value `e`.
The type of the field must remain unchanged.

``if c then a else b``
......................

If ``c`` evaluates to ``true``, evaluate ``a``, else evaluate ``b``.

Binding Expressions
~~~~~~~~~~~~~~~~~~~

``let pat = e in body``
.......................

Evaluate ``e`` and bind the result to the irrefutable pattern ``pat``
(see :ref:`patterns`) while evaluating ``body``.  The ``in`` keyword
is optional if ``body`` is a ``let`` expression. See also `Shape
Declarations`_.

``let a[i] = v in body``
........................................

Write ``v`` to ``a[i]`` and evaluate ``body``.  The given index need
not be complete and can also be a slice, but in these cases, the value
of ``v`` must be an array of the proper size.  This notation is
Syntactic sugar for ``let a = a with [i] = v in a``.

``let f params... = e in body``
...............................

Bind ``f`` to a function with the given parameters and definition
(``e``) and evaluate ``body``.  The function will be treated as
aliasing any free variables in ``e``.  The function is not in scope of
itself, and hence cannot be recursive.  See also `Shape
Declarations`_.

``loop pat = initial for x in a do loopbody``
.............................................

1. Bind ``pat`` to the initial values given in ``initial``.

2. For each element ``x`` in ``a``, evaluate ``loopbody`` and rebind
   ``pat`` to the result of the evaluation.

3. Return the final value of ``pat``.

The ``= initial`` can be left out, in which case initial values for
the pattern are taken from equivalently named variables in the
environment.  I.e., ``loop (x) = ...`` is equivalent to ``loop (x = x)
= ...``.

See also `Shape Declarations`_.

``loop pat = initial for x < n do loopbody``
............................................

Equivalent to ``loop (pat = initial) for x in [0..1..<n] do loopbody``.

``loop pat = initial = while cond do loopbody``
...............................................

1. Bind ``pat`` to the initial values given in ``initial``.

2. If ``cond`` evaluates to true, bind ``pat`` to the result of
   evaluating ``loopbody``, and repeat the step.

3. Return the final value of ``pat``.

See also `Shape Declarations`_.

``match x case p1 -> e1 case p2 -> e2``
.......................................

Match the value produced by ``x`` to each of the patterns in turn,
picking the first one that succeeds.  The result of the corresponding
expression is the value of the entire ``match`` expression.  All the
expressions associated with a ``case`` must have the same type (but
not necessarily match the type of ``x``).  It is a type error if there
is not a ``case`` for every possible value of ``x`` - inexhaustive
pattern matching is not allowed.

Function Expressions
~~~~~~~~~~~~~~~~~~~~

``\x y z: t -> e``
..................

Produces an anonymous function taking parameters ``x``, ``y``, and
``z``, returns type ``t``, and whose body is ``e``.  Lambdas do not
permit type parameters; use a named function if you want a polymorphic
function.

``(binop)``
...........

An *operator section* that is equivalent to ``\x y -> x *binop* y``.

``(x binop)``
.............

An *operator section* that is equivalent to ``\y -> x *binop* y``.

``(binop y)``
.............

An *operator section* that is equivalent to ``\x -> x *binop* y``.

``(.a.b.c)``
............

An *operator section* that is equivalent to ``\x -> x.a.b.c``.

``(.[i,j])``
............

An *operator section* that is equivalent to ``\x -> x[i,j]``.

Higher-order functions
----------------------

At a high level, Futhark functions are values, and can be used as any
other value.  However, to ensure that the compiler is able to compile
the higher-order functions efficiently via *defunctionalisation*,
certain type-driven restrictions exist on how functions can be used.
These also apply to any record or tuple containing a function (a
*functional type*):

* Arrays of functions are not permitted.

* A function cannot be returned from an ``if`` expression.

* A ``loop`` parameter cannot be a function.

Further, *type parameters* are divided into *non-lifted* (bound with
an apostrophe, e.g. ``'t``), and *lifted* (``'^t``).  Only lifted type
parameters may be instantiated with a functional type.  Within a
function, a lifted type parameter is treated as a functional type.
All abstract types declared in modules (see `Module System`_) are
considered non-lifted, and may not be functional.

See also `In-place updates`_ for details on how uniqueness types
interact with higher-order functions.

Type Inference
--------------

Futhark supports Hindley-Milner-style type inference, so in many cases
explicit type annotations can be left off.  Record field projection
cannot in isolation be fully inferred, and may need type annotations
where their inputs are bound.  The same goes when constructing sum
types, as Futhark cannot assume that a given constructor only belongs
to a single type.  Further, unique types (see `In-place updates`_)
must be explicitly annotated.

.. _in-place-updates:

In-place Updates
----------------

In-place updates do not provide observable side effects, but they do
provide a way to efficiently update an array in-place, with the
guarantee that the cost is proportional to the size of the value(s)
being written, not the size of the full array.

The ``a with [i] = v`` language construct, and derived forms,
performs an in-place update.  The compiler verifies that the original
array (``a``) is not used on any execution path following the in-place
update.  This involves also checking that no *alias* of ``a`` is used.
Generally, most language constructs produce new arrays, but some
(slicing) create arrays that alias their input arrays.

When defining a function parameter or return type, we can mark it as
*unique* by prefixing it with an asterisk.  For example::

  let modify (a: *[]i32) (i: i32) (x: i32): *[]i32 =
    a with [i] = a[i] + x

For bulk in-place updates with multiple values, use the ``scatter``
function in the basis library.  In the parameter declaration ``a:
*[i32]``, the asterisk means that the function ``modify`` has been
given "ownership" of the array ``a``, meaning that any caller of
``modify`` will never reference array ``a`` after the call again.
This allows the ``with`` expression to perform an in-place update.

After a call ``modify a i x``, neither ``a`` or any variable that
*aliases* ``a`` may be used on any following execution path.

Alias Analysis
~~~~~~~~~~~~~~

The rules used by the Futhark compiler to determine aliasing are
intuitive in the intra-procedural case.  Aliases are associated with
entire arrays.  Aliases of a record are tuple are tracked for each
element, not for the record or tuple itself.  Most constructs produce
fresh arrays, with no aliases.  The main exceptions are ``if``,
``loop``, function calls, and variable literals.

* After a binding ``let a = b``, that simply assigns a new name to an
  existing variable, the variable ``a`` aliases ``b``.  Similarly for
  record projections and patterns.

* The result of an ``if`` aliases the union of the aliases of the
  components.

* The result of a ``loop`` aliases the initial values, as well as any
  aliases that the merge parameters may assume at the end of an
  iteration, computed to a fixed point.

* The aliases of a value returned from a function is the most
  interesting case, and depends on whether the return value is
  declared *unique* (with an asterisk ``*``) or not.  If it is
  declared unique, then it has no aliases.  Otherwise, it aliases all
  arguments passed for *non-unique* parameters.

In-place Updates and Higher-Order Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Uniqueness typing generally interacts poorly with higher-order
functions.  The issue is that we cannot control how many times a
function argument is applied, or to what, so it is not safe to pass a
function that consumes its argument.  The following two conservative
rules govern the interaction between uniqueness types and higher-order
functions:

1. In the expression ``let p = e1 in ...``, if *any* in-place update
   takes place in the expression ``e1``, the value bound by ``p`` must
   not be or contain a function.

2. A function that consumes one of its arguments may not be passed as
   a higher-order argument to another function.

.. _module-system:

Module System
-------------

.. productionlist::
   mod_bind: "module" `id` `mod_param`* "=" [":" mod_type_exp] "=" `mod_exp`
   mod_param: "(" `id` ":" `mod_type_exp` ")"
   mod_type_bind: "module" "type" `id` `type_param`* "=" `mod_type_exp`

Futhark supports an ML-style higher-order module system.  *Modules*
can contain types, functions, and other modules and module types.
*Module types* are used to classify the contents of modules, and
*parametric modules* are used to abstract over modules (essentially
module-level functions).  In Standard ML, modules, module types and
parametric modules are called structs, signatures, and functors,
respectively.  Module names exist in the same name space as values,
but module types are their own name space.

Named modules are declared as::

  module name = ...

A named module type is defined as::

  module type name = ...

Where a module expression can be the name of another module, an
application of a parametric module, or a sequence of declarations
enclosed in curly braces::

  module Vec3 = {
    type t = ( f32 , f32 , f32 )
    let add(a: t) (b: t): t =
      let (a1, a2, a3) = a in
      let (b1, b2, b3) = b in
      (a1 + b1, a2 + b2 , a3 + b3)
  }

  module AlsoVec3 = Vec3

Functions and types within modules can be accessed using dot
notation::

    type vector = Vec3.t
    let double(v: vector): vector = Vec3.add v v

We can also use ``open Vec3`` to bring the names defined by ``Vec3``
into the current scope.  Multiple modules can be opened simultaneously
by separating their names with spaces.  In case several modules define
the same names, the ones mentioned last take precedence.  The first
argument to ``open`` may be a full module expression.

Named module types are defined as::

  module type ModuleTypeName = ...

A module type expression can be the name of another module type, or a
sequence of *specifications*, or *specs*, enclosed in curly braces.  A
spec can be a *value spec*, indicating the presence of a function or
value, an *abstract type spec*, or a *type abbreviation spec*.  For
example::

  module type Addable = {
    type t                 -- abstract type spec
    type two_ts = (t,t)    -- type abbreviation spec
    val add: t -> t -> t   -- value spec
  }

This module type specifies the presence of an *abstract type* ``t``,
as well as a function operating on values of type ``t``.  We can use
*module type ascription* to restrict a module to what is exposed by
some module type::

  module AbstractVec = Vec3 : Addable

The definition of ``AbstractVec.t`` is now hidden.  In fact, with this
module type, we can neither construct values of type ``AbstractVec.T``
or convert them to anything else, making this a rather useless use of
abstraction.  As a derived form, we can write ``module M: S = e`` to
mean ``module M = e : S``.

Parametric modules allow us to write definitions that abstract over
modules.  For example::

  module Times = \(M: Addable) -> {
    let times (x: M.t) (k: int): M.t =
      loop (x' = x) for i < k do
        T.add x' x
  }

We can instantiate ``Times`` with any module that fulfills the module
type ``Addable`` and get back a module that defines a function
``times``::

  module Vec3Times = Times Vec3

Now ``Vec3Times.times`` is a function of type ``Vec3.t -> int ->
Vec3.t``.  As a derived form, we can write ``module M p = e`` to mean
``module M = \p -> e``.

Module Expressions
~~~~~~~~~~~~~~~~~~

.. productionlist::
   mod_exp:   `qualid`
          : | `mod_exp` ":" `mod_type_exp`
          : | "\" "(" `id` ":" `mod_type_exp` ")" [":" `mod_type_exp`] "->" `mod_exp`
          : | `mod_exp` `mod_exp`
          : | "(" `mod_exp` ")"
          : | "{" `dec`* "}"
          : | "import" `stringlit`

A module expression produces a module.  Modules are collections of
bindings produced by declarations (`dec`).  In particular, a module
may contain other modules or module types.

``qualid``
..........

Evaluates to the module of the given name.

``(mod_exp)``
.............

Evaluates to ``mod_exp``.

``mod_exp : mod_type_exp``
..........................

*Module ascription* evaluates the module expression and the module
type expression, verifies that the module implements the module type,
then returns a module that exposes only the functionality described by
the module type.  This is how internal details of a module can be
hidden.

``\(p: mt1): mt2 -> e``
.......................

Constructs a *parametric module* (a function at the module level) that
accepts a parameter of module type ``mt1`` and returns a module of
type ``mt2``.  The latter is optional, but the parameter type is not.

``e1 e2``
.........

Apply the parametric module ``m1`` to the module ``m2``.

``{ decs }``
............

Returns a module that contains the given definitions.  The resulting
module defines any name defined by any declaration that is not
``local``, *in particular* including names made available via
``open``.

``import "foo"``
................

Returns a module that contains the definitions of the file ``"foo"``
relative to the current file.  See :ref:`other-files`.

Module Type Expressions
~~~~~~~~~~~~~~~~~~~~~~~

.. productionlist::
   mod_type_exp:   `qualid`
             : | "{" `spec`* "}"
             : | `mod_type_exp` "with" `qualid` `type_param`* "=" `type`
             : | "(" `mod_type_exp` ")"
             : | "(" `id` ":" `mod_type_exp` ")" "->" `mod_type_exp`
             : | `mod_type_exp` "->" `mod_type_exp`


.. productionlist::
   spec:   "val" `id` `type_param`* ":" `spec_type`
       : | "val" `binop` `type_param`* ":" `spec_type`
       : | "type" `id` `type_param`* "=" `type`
       : | "type" ["^"] `id` `type_param`*
       : | "module" `id` ":" `mod_type_exp`
       : | "include" `mod_type_exp`
   spec_type: `type` | `type` "->" `spec_type`

Module types classify modules, with the only (unimportant) difference
in expressivity being that modules can contain module types, but
module types cannot specify that a module must contain a specific
module types. They can specify of course that a module contains a
*submodule* of a specific module type.

.. _other-files:

Referring to Other Files
------------------------

You can refer to external files in a Futhark file like this::

  import "file"

The above will include all non-``local`` top-level definitions from
``file.fut`` is and make them available in the current file (but
will not export them).  The ``.fut`` extension is implied.

You can also include files from subdirectories::

  import "path/to/a/file"

The above will include the file ``path/to/a/file.fut`` relative to the
including file.

Qualified imports are also possible, where a module is created for the
file::

  module M = import "file"

In fact, a plain ``import "file"`` is equivalent to::

  local open import "file"
