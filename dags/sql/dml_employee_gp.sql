create table if not exists t_employee
(
    department   varchar(512),
    rank         varchar(512),
    staff_number integer,
    full_name    varchar(512),
    birth_date   date,
    address      varchar(512),
    phone1       varchar(32),
    phone2       varchar(32),
    date_month   integer,
    work_time    double precision
)
    distributed by (staff_number);

create table if not exists t_employee_hist
(
    id           serial primary key,
    department   varchar(512),
    rank         varchar(512),
    staff_number integer,
    full_name    varchar(512),
    birth_date   date,
    address      varchar(512),
    phone1       varchar(32),
    phone2       varchar(32),
    date_month   integer,
    work_time    double precision
)
    distributed by (id);

