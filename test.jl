@require "." DataGraph push assoc_in @struct

@struct Address(street::String)
@struct User(name::String, address::Union{Missing,Address})

a = User("a", Address("b"))
dg = push(DataGraph(), a)

@test dg[User]|>first == User("a", Address("b"))

dg = assoc_in(dg, [a :name] => "Jake")
@test dg[User]|>first == User("Jake", Address("b"))

dg = assoc_in(dg, (a.address, :street) => "mayfair")
@test dg[Address]|>first == Address("mayfair")

dg = assoc_in(dg, [a :address :street] => "coronation")
@test dg[Address]|>first == Address("coronation")

@test dg[Address]|>length == 1
@test dg[Address]|>collect == [Address("coronation")]
@test dg[User]|>length == 1
@test length(dg) == 2
@test Set(collect(dg)) == Set([User("Jake", Address("coronation")), Address("coronation")])
