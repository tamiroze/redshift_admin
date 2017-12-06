--see recent redshift queries
select *
from stv_recents
where status='Running';

--kill sesseion using PID from stv_recents
select pg_terminate_backend(121815);
