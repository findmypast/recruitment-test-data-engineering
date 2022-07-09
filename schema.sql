create table `person` (
  `id` int not null auto_increment,
  `given_name` varchar(80) default null,
  `family_name` varchar(80) default null,
  `date_of_birth` Date default null,
   `place_of_birth` varchar(80) default null,
   'place_id' bigint
  primary key (`id`)

);

create table `places` (
  `id` int not null auto_increment,
  `city` varchar(80) default null,
  `county` varchar(80) default null,
  `country` varchar(80) default null,
  primary key (`id`)
);
