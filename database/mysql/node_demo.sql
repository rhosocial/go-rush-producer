create table node_info
(
    id           bigint unsigned auto_increment comment '节点编号'
        primary key,
    name         varchar(255)      default ''                   not null comment '节点名称（由节点自行提供）',
    node_version varchar(255)      default ''                   not null comment '节点版本号（x.y.z）或（x.y.z.build）或（git commit no 不少于十二位）',
    host         varchar(255)      default ''                   not null comment '节点套接字的域（ip/domain）',
    port         smallint unsigned default '8080'               not null comment '节点套接字的端口',
    level        tinyint unsigned                               not null comment '节点级别（0-master，1-slave）',
    superior_id  bigint unsigned   default '0'                  not null comment '上级ID。0表示没有上级。',
    `order`      int unsigned      default '0'                  not null comment '上级主节点失效后的接替顺序（数值越小优先级越高）',
    is_active    tinyint unsigned  default '0'                  not null comment '是否活跃（0-活跃，1-上级主节点发现失效，2-下级从节点发现失效）',
    created_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null comment '创建时间',
    updated_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null on update CURRENT_TIMESTAMP(3) comment '最后更新时间',
    version      bigint unsigned   default '0'                  not null comment '本条记录版本。从0开始。',
    constraint node_level_superior_order_index
        unique (level, superior_id, `order`) comment '节点级别、上级节点和接替顺序',
    constraint node_socket_index
        unique (host, port) comment '节点套接字索引'
)
    comment '节点信息';

create index node_active_index
    on node_info (is_active)
    comment '节点活跃索引';

create index node_info_id_index
    on node_info (id);

create table node_info_legacy
(
    id           bigint unsigned                                not null comment '（删除前最后一刻）节点编号'
        primary key,
    name         varchar(255)      default ''                   not null comment '（删除前最后一刻）节点名称（由节点自行提供）',
    node_version varchar(255)      default ''                   not null comment '（删除前最后一刻）节点版本号（x.y.z）或（x.y.z.build）或（git commit no 不少于十二位）',
    host         varchar(255)      default ''                   not null comment '节点套接字的域（ip/domain）',
    port         smallint unsigned default '8080'               not null comment '节点套接字的端口',
    level        tinyint unsigned                               not null comment '（删除前最后一刻）节点级别（0-master，1-slave）',
    superior_id  bigint unsigned   default '0'                  not null comment '（删除前最后一刻）上级ID。0表示没有上级。',
    `order`      int unsigned      default '0'                  not null comment '（删除前最后一刻）上级主节点失效后的接替顺序（数值越小优先级越高）',
    is_active    tinyint unsigned  default '0'                  not null comment '（删除前最后一刻）是否活跃（0-活跃，1-上级主节点发现失效，2-下级从节点发现失效）',
    created_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null comment '本条记录在node_info表的创建时间，而非本条记录在该表的创建时间',
    updated_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null on update CURRENT_TIMESTAMP(3) comment '最后更新时间，也即插入该表的时间',
    version      bigint unsigned   default '0'                  not null comment '本条记录版本。从0开始。'
)
    comment '节点信息(历史)';

create index node_active_index
    on node_info_legacy (is_active)
    comment '节点活跃索引';

create index node_info_id_index
    on node_info_legacy (id);

create index node_level_superior_order_index
    on node_info_legacy (level, superior_id, `order`)
    comment '节点接替顺序索引';

create index node_socket_index
    on node_info_legacy (host, port)
    comment '节点套接字索引';

create table node_log
(
    id         bigint unsigned auto_increment comment '变更日志ID'
        primary key,
    node_id    bigint unsigned                           not null comment '事件涉及节点ID',
    type       tinyint unsigned                          not null comment '事件类型',
    created_at timestamp(3) default CURRENT_TIMESTAMP(3) not null comment '事件发生时间',
    constraint node_log_node_node_id_fk
        foreign key (node_id) references node_info (id)
);

create index node_log_created_at_index
    on node_log (created_at desc);

create index node_log_node_id_index
    on node_log (node_id);

create index node_log_type_index
    on node_log (type);

