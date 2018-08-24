@require "github.com/jkroso/Destructure.jl" @destruct
@require "github.com/jkroso/Prospects.jl" exports...

@struct DataGraph(data=Dict{DataType,Dict{UInt,Tuple}}(),
                  ids=Dict{UInt,UInt}(),
                  cache=Dict{UInt,Any}())

const empty_store = Dict{UInt,Tuple}()

push(d::DataGraph, x::T) where T = begin
  @assert !isprimitive(T) "why are you trying to store a primitive type?"
  ids = copy(d.ids)
  data = recursive_push(d.data, ids, x, T, rand(UInt))
  DataGraph(data, ids, d.cache)
end

recursive_push(d::AbstractDict, ids::Dict, x, T, id::UInt) = begin
  haskey(ids, objectid(x)) && return d
  ids[objectid(x)] = id
  row = map(fieldnames(T)) do f::Symbol
    fv = getfield(x, f)
    FT = typeof(fv)
    isprimitive(FT) && return FT, fv
    haskey(ids, objectid(fv)) && return FT, ids[objectid(fv)]
    fid = rand(UInt)
    d = recursive_push(d, ids, fv, FT, fid)
    FT, fid
  end
  store = get(d, T, empty_store)
  assoc(d, T, assoc(store, id, tuple(row...)))
end

const primitive_types = [
  Number,
  AbstractString,
  AbstractDict,
  AbstractArray,
  AbstractSet,
  Symbol,
  AbstractChar,
  AbstractRange
]

isprimitive(T::DataType) = any(P->T<:P, primitive_types)
isprimitive(T::UnionAll) = isprimitive(T.body)
isprimitive(T::Union) = false

@struct Table{T}(dg::DataGraph, store::Dict{UInt,Tuple})

Base.getindex(d::DataGraph, T::Type) = Table{T}(d, get(d.data, T, empty_store))
Base.get(d::DataGraph, T::Type, default) = d[T]
Base.eltype(::Table{T}) where T = T
Base.iterate(d::DataGraph, (table, table_state, graph_state)) = begin
  local temp = iterate(table, table_state)
  while temp == nothing
    temp2 = iterate(d.data, graph_state)
    temp2 == nothing && return nothing
    ((T, dict), graph_state) = temp2
    table = d[T]
    temp = iterate(table)
  end
  value, table_state = temp
  return (value, (table, table_state, graph_state))
end
Base.iterate(d::DataGraph) = begin
  temp = iterate(d.data)
  temp == nothing && return nothing
  (T,table),graph_state = temp
  iterate(d, (Table{T}(d,table), 0, graph_state))
end

Base.length(d::DataGraph) = mapreduce(kv->length(kv[2]), +, d.data, init=0)
Base.length(t::Table) = length(t.store)
Base.lastindex(t::Table) = length(t.store)

Base.iterate(t::Table) = iterate(t, 0)
Base.iterate(t::Table{T}, state) where T = begin
  i = iterate(t.store, state)
  i == nothing && return nothing
  (id, row), state = i
  (parse_row(t.dg, row, id, T), state)
end

parse_row(dg::DataGraph, row::Tuple, id::UInt, T::Type) = begin
  haskey(dg.cache, id) && return dg.cache[id]
  t = ccall(:jl_new_struct_uninit, Any, (Any,), T)
  dg.cache[id] = t
  for (i, (FT, fv)) in enumerate(row)
    if !isprimitive(FT)
      fv = parse_row(dg, dg.data[FT][fv], fv, FT)
    end
    ccall(:jl_set_nth_field, Nothing, (Any, Csize_t, Any), t, i-1, fv)
  end
  dg.ids[objectid(t)] = id
  return t
end

assoc_in(dg::DataGraph, p::Pair) = begin
  @destruct [[entity, keys...], value] = p
  T = typeof(entity).name.wrapper
  id = get(dg.ids, objectid(entity))
  recursive_assoc(dg, id, T, keys, value)
end

recursive_assoc(dg::DataGraph, id::UInt, T::DataType, path, value) = begin
  @destruct [key, rest...] = path
  row = get_in(dg.data, [T, id])
  fi = findfirst(isequal(key), fieldnames(T))
  fi == nothing && throw(KeyError(key))
  FT, fv = row[fi]
  if isprimitive(FT)
    DataGraph(assoc_in(dg.data, [T, id, fi] => (typeof(value), value)), dg.ids)
  else
    recursive_assoc(dg, fv, FT, rest, value)
  end
end

export DataGraph
