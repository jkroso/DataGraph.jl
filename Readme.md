# DataGraph.jl

An immutable graph data structure. With mutable data, creating and changing graphs is easy. While with immutable data its actually impossible by default. But there are a couple ways to acheive it. One is to use a reference type similar to a symlink on a file system. The other is to store all objects indexed by an ID and replace all references to other objects with these ID's. Which is essentially just taking the first idea to its logical conclusion. It works better though since when an entity moves, symlinks to it break. With `DataGraph` all entities are indexed by their ID so they never move. Also symlinks are sometimes a bit of a leaky abstraction, hence `realpath`. The symlinks inside a `DataGraph` are never exsposed to you as the user so they can't cause any bugs.

Inspired in part by [DataScript](https://github.com/tonsky/datascript). Its essentially solving the same problem. As such `DataGraph` will eventually support reified transactions and a datalog inspired query engine. It will be much nicer though thanks to being implemented in Julia.

## Usage

```julia
@require "github.com/jkroso/DataGraph.jl" exports...
```
