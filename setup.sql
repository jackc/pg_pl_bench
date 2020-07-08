create extension plperl;

create function plpgsql_empty_loop_n(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function plpgsql_loop_n_select(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    select 1 into strict _n;
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function plpgsql_loop_n_select_increment(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    select _n + 1 into strict _n;
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function plpgsql_loop_n_assign_increment(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    _n := _n + 1;
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function perl_loop_n_increment(int) returns void
language plperl as $$
  my $times = $_[0];
  my $n = 0;
  for ($i = 0; $i < $times; $i++) {
    $n++;
  }
$$;

create function plpgsql_add(int, int) returns int
immutable
language plpgsql as $$
begin
  return $1 + $2;
end;
$$;

create function sql_add(int, int) returns int
immutable
language sql as $$
  select $1 + $2;
$$;

create function perl_add(int, int) returns int
language plperl as $$
  return $_[0] + $_[1];
$$;

create function plpgsql_loop_call_plpgsql_add(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    _n := plpgsql_add(_i, 1);
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function plpgsql_loop_call_sql_add(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    _n := sql_add(_i, 1);
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;

create function plpgsql_loop_call_perl_add(_times int) returns void
language plpgsql as $$
declare
  _i int = 0;
  _n int = 0;
begin
  loop
    _n := perl_add(_i, 1);
    _i := _i + 1;
    if _i >= _times then
      exit;
    end if;
  end loop;
end;
$$;
