-- implement SCD2
-- store every changes of data
insert into t_employee_hist (department, rank, staff_number, full_name, birth_date, address,
                             phone1, phone2, date_month, work_time)
select e.department, e.rank, e.staff_number, e.full_name, e.birth_date, e.address,
       e.phone1, e.phone2, e.date_month, work_time
from t_employee  e where (department, rank, staff_number, full_name, birth_date, address,
                          phone1, phone2, date_month, work_time) not in
                         (select department, rank, staff_number, full_name, birth_date, address,
                                 phone1, phone2, date_month, work_time
                          from t_employee_hist);

commit;