
create table mq_messages (
  `id` bigint(20) not null auto_increment,
  `content` varchar(1024) default '' comment '事务消息',
  `topic` char(60) default '' comment '主题',
  `tag` char(60) default '' comment '标签',
  `status` tinyint default 1 comment '状态',
  `create_time` bigint(20) default 0 comment '创建时间',
  `delay` int(10) default 0 comment '延迟',
  PRIMARY KEY (`id`)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci