drop table if exists people;
create table people (
  id integer primary key autoincrement,
  name text not null,
  surname text not null,
  birth date not null,
  points integer not null
);