include("main.jl")

@immutable Address(street::String)
@immutable User(name::String, address::Address)

a = User("a", Address("b"))
dg = push(DataGraph(), a)
@test dg[User]|>first == User("a", Address("b"))

dg = assoc_in(dg, [a :name] => "Jake")
@test dg[User]|>first == User("Jake", Address("b"))

dg = assoc_in(dg, [a.address :street] => "mayfair")
@test dg[Address]|>first == Address("mayfair")

dg = assoc_in(dg, [a :address :street] => "coronation")
@test dg[Address]|>first == Address("coronation")

@test dg[Address]|>length == 1
@test dg[User]|>length == 1
