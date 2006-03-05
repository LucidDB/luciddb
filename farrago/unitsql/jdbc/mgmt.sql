-- Test management views

select count(url) from sys_boot.mgmt.sessions_view;

select sql_stmt from sys_boot.mgmt.statements_view;

select count(mof_id) from sys_boot.mgmt.objects_in_use_view;

