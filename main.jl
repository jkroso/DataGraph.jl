@require "github.com/jkroso/Destructure.jl" @destruct
@require "github.com/jkroso/Prospects.jl" exports...

@struct DataGraph(data=Dict{DataType,Dict{UInt,Tuple}}(),
                  ids=Dict{UInt,UInt}(),
                  cache=Dict{UInt,Any}())

const empty_store = Dict{UInt,Tuple}()

push{T}(d::DataGraph, x::T) = begin
  @assert !isprimitive(T) "why are you trying to store a primitive type?"
  ids = copy(d.ids)
  data = recursive_push(d.data, ids, x, T, rand(UInt))
  DataGraph(data, ids, d.cache)
end

recursive_push(d::Associative, ids::Dict, x, T, id::UInt) = begin
  haskey(ids, object_id(x)) && return d
  ids[object_id(x)] = id
  row = map(fieldnames(T)) do f::Symbol
    FT = fieldtype(T, f)
    fv = getfield(x, f)
    if FT <: Nullable
      FT = FT.parameters[1]
      fv = isnull(fv) ? nothing : get(fv)
    end
    isprimitive(FT) && return fv
    haskey(ids, object_id(fv)) && return ids[object_id(fv)]
    fid = rand(UInt)
    d = recursive_push(d, ids, fv, FT, fid)
    fid
  end
  store = get(d, T, empty_store)
  assoc(d, T, assoc(store, id, tuple(row...)))
end

const primitive_types = [Number,AbstractString,Associative,AbstractArray,Base.AbstractSet,Symbol]
isprimitive(T::DataType) = any(P->T<:P, primitive_types)
isprimitive(T::UnionAll) = isprimitive(T.body)

@struct Table{T}(dg::DataGraph, store::Dict{UInt,Tuple})

Base.getindex(d::DataGraph, T::Type) = Table{T}(d, get(d.data, T, empty_store))
Base.get(d::DataGraph, T::Type, default) = d[T]
Base.eltype{T}(::Table{T}) = T
Base.start(d::DataGraph) = begin
  dg_state = start(d.data)
  done(d.data, dg_state) && return (dg_state, Any, empty_store, 0)
  ((T, table), dg_state) = next(d.data, dg_state)
  (dg_state, T, table, start(table))
end
Base.done(d::DataGraph, state) = begin
  (dg_state, _, table, table_state) = state
  done(d.data, dg_state) && done(table, table_state)
end
Base.next(d::DataGraph, state) = begin
  (dg_state, T, table, table_state) = state
  while done(table, table_state)
    ((T, table), dg_state) = next(d.data, dg_state)
    table_state = start(table)
  end
  ((id, row), table_state) = next(table, table_state)
  (parse_row(d, row, id, T), (dg_state, T, table, table_state))
end
Base.length(d::DataGraph) = mapreduce(kv->length(kv[2]), +, 0, d.data)
Base.length(t::Table) = length(t.store)
Base.endof(t::Table) = length(t.store)
Base.start(t::Table) = start(t.store)
Base.done(t::Table, state) = done(t.store, state)
Base.next{T}(t::Table{T}, state) = begin
  ((id, row), next_state) = next(t.store, state)
  (parse_row(t.dg, row, id, T), next_state)
end

parse_row(dg::DataGraph, row::Tuple, id::UInt, T::Type) = begin
  haskey(dg.cache, id) && return dg.cache[id]
  t = ccall(:jl_new_struct_uninit, Any, (Any,), T)
  dg.cache[id] = t
  for (i, fv) in enumerate(row)
    FT = fieldtype(T, i)
    isnullable = FT <: Nullable
    if isnullable && fv === nothing
      ccall(:jl_set_nth_field, Void, (Any, Csize_t, Any), t, i-1, FT())
    else
      RT = isnullable ? FT.parameters[1] : FT
      if !isprimitive(RT)
        fv = parse_row(dg, dg.data[RT][fv], fv, RT)
      end
      ccall(:jl_set_nth_field, Void, (Any, Csize_t, Any), t, i-1, isnullable ? FT(fv) : fv)
    end
  end
  dg.ids[object_id(t)] = id
  return t
end

assoc_in(dg::DataGraph, p::Pair) = begin
  @destruct [[entity, keys...], value] = p
  T = typeof(entity).name.wrapper
  if T <: Nullable
    entity = get(entity)
    T = typeof(entity)
  end
  id = get(dg.ids, object_id(entity))
  recursive_assoc(dg, id, T, keys, value)
end

recursive_assoc(dg::DataGraph, id::UInt, T::DataType, path, value) = begin
  @destruct [key, rest...] = path
  row = get_in(dg.data, [T, id])
  FT = fieldtype(T, key)
  if FT <: Nullable FT = FT.parameters[1] end
  fi = findfirst(f->f ≡ key, fieldnames(T))
  fi > 0 || throw(KeyError(key))
  fv = row[fi]
  if FT <: Nullable
    fv = isnull(fv) ? nothing : get(fv)
  end
  if isprimitive(FT)
    DataGraph(assoc_in(dg.data, [T, id, fi] => value), dg.ids)
  else
    recursive_assoc(dg, fv, FT, rest, value)
  end
end

export DataGraph
