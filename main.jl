@require "github.com/jkroso/Prospects.jl" exports...

@immutable DataGraph(data=Dict{DataType,Dict{UInt,Tuple}}(),
                     identities=Dict{UInt,UInt}(),
                     cache=Dict{UInt,Any}())

const empty_store = Dict{UInt,Tuple}()

push{T}(d::DataGraph, x::T) = begin
  @assert !isprimitive(T) "why are you trying to store a primitive type?"
  ids = copy(d.identities)
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
isprimitive(T::TypeConstructor) = isprimitive(T.body)

@immutable Table{T}(dg::DataGraph, store::Dict{UInt,Tuple})

Base.getindex(d::DataGraph, T::Type) = Table{T}(d, get(d.data, T, empty_store))
Base.eltype{T}(::Table{T}) = T
Base.length(t::Table) = length(t.store)
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
    if !isprimitive(FT)
      RT = FT <: Nullable ? FT.parameters[1] : FT
      fv = parse_row(dg, get_in(dg.data, [RT, fv]), fv, RT)
    end
    ccall(:jl_set_nth_field, Void, (Any, Csize_t, Any), t, i-1, convert(FT, fv))
  end
  dg.identities[object_id(t)] = id
  return t
end

assoc_in(dg::DataGraph, p::Pair) = begin
  entity = first(p.first)
  T = typeof(entity).name.primary
  if T<:Nullable
    T = typeof(entity).parameters[1]
    entity = get(entity)
  end
  id = get(dg.identities, object_id(entity))
  recursive_assoc(dg, id, T, drop(p.first, 1), p.second)
end

recursive_assoc(dg::DataGraph, id::UInt, T::DataType, path, value) = begin
  row = get_in(dg.data, [T, id])
  key = first(path)
  FT = fieldtype(T, key)
  if FT <: Nullable FT = FT.parameters[1] end
  fi = findfirst(f->f ≡ key, fieldnames(T))
  fi > 0 || throw(KeyError(key))
  fv = row[fi]
  if FT <: Nullable fv = isnull(fv) ? nothing : get(fv) end
  if isprimitive(FT)
    DataGraph(assoc_in(dg.data, [T, id, fi] => value), dg.identities)
  else
    recursive_assoc(dg, fv, FT, drop(path, 1), value)
  end
end

export DataGraph
