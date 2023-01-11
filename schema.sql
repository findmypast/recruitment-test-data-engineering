drop table if exists people;

create table `people` (
  `id` int not null auto_increment,
  `Given_name` varchar(80) default null,
  `Family_name` varchar(80) default null,
  `Date_of_birth` date default null,
  `Place_of_birth` varchar(80) default null,
  primary key (`id`)
);

drop table if exists places;

create table `places` (
  `id` int not null auto_increment,
  `City` varchar(80) default null,
  `County` varchar(80) default null,
  `Country` varchar(80) default null,
  primary key (`id`)
);