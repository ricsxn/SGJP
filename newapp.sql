--
-- Insert statements necessary to include a new application into the job perusal watching ...
--
insert into sgjp_applications (app_id,app_name,app_default_step) values (2,'CMSquares',10);
insert into sgjp_application_files(file_id,app_id,app_file_path,app_file_step) values (1,2,'sgjp.log',NULL);
insert into sgjp_application_files(file_id,app_id,app_file_path,app_file_step) values (2,2,'magicsquares-Output.txt',NULL);
insert into sgjp_application_files(file_id,app_id,app_file_path,app_file_step) values (3,2,'magicsquares-Error.txt',NULL);
insert into sgjp_application_files(file_id,app_id,app_file_path,app_file_step) values (4,2,'s6_counts.txt',NULL);

