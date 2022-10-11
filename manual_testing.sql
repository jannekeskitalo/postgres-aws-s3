create extension plpython3u;

create extension aws_s3;

drop table public.test_fast_query_export_to_s3;

create table public.test_fast_query_export_to_s3 as select md5(random()::text) as bar from generate_series(1,1000000);

select count(*) from public.test_fast_query_export_to_s3;

select * from
aws_s3.multipart_query_export_to_s3(
    'select * from public.test_fast_query_export_to_s3',
    'test-bucket',
    'foo.csv',
    167000,
    'eu-west-1',
    'dummy',
    'dummy',
    'dummy',
    endpoint_url := 'http://localstack:4566'
);