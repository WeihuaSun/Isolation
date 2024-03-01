create table users (
    key varchar(20) primary key,
    value varchar(280)
);

create table tweet (
    key varchar(20) primary key,
    value varchar(180)
);
create table last_tweet (
    key varchar(30) primary key,
    value varchar(60)
);
create table follow_list (
    key varchar(20) primary key,
    value varchar(3000)
);
create table following (
    key varchar(30) primary key,
    value varchar(80)
);
create table followers (
    key varchar(30) primary key,
    value varchar(80)
);

truncate table tweet;
truncate table users;
truncate table last_tweet;
truncate table follow_list;
truncate table following;
truncate table followers;
;

ssh -L 5433:localhost:5432 -p 41014 root@192.168.31.104