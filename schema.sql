
drop table project cascade;

drop table function cascade;

drop table location cascade;
drop table hashfact cascade;

create table project(
   project serial primary key,
   name text
 );
 
 --create table location(
  -- location serial primary key, --identity(1,1)
  -- first_line integer,
  -- length integer
 --);

 
create table function(
	function serial primary key,
	base_line text, 
	fname text,
	file_path text
);

 CREATE TABLE hashfact(
    hashfact serial primary key,
    hash bytea, -- binary(160)
    function integer references function(function),
    project integer references project(project),
    --location integer references location(location),
    from_line integer,
    length integer,
    type smallint CHECK (type <= 3 and type >= 1)
);





