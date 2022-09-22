CREATE OR REPLACE FUNCTION aws_s3.fast_query_export_to_s3(
    query text,    
    bucket text,    
    file_path text,
    region text default null,
    access_key text default null,
    secret_key text default null,
    session_token text default null,
    options text default null, 
    endpoint_url text default null,
    OUT rows_uploaded bigint,
    OUT files_uploaded bigint,
    OUT bytes_uploaded bigint
) RETURNS SETOF RECORD
LANGUAGE plpython3u
AS $$
    def cache_import(module_name):
        module_cache = SD.get('__modules__', {})
        if module_name in module_cache:
            return module_cache[module_name]
        else:
            import importlib
            _module = importlib.import_module(module_name)
            if not module_cache:
                SD['__modules__'] = module_cache
            module_cache[module_name] = _module
            return _module
 
    def run_copy(query_in, output_file_in, options_in):
        plpy.info("Executing COPY")
        sql = "COPY ({query}) TO PROGRAM 'cat >{output_file}' {options}".format(
                query=query_in,
                output_file=output_file_in,
                options="({options})".format(options=options_in) if options_in else ''
            )
        plpy.info(f"COPY: {sql}")
        plan = plpy.prepare(sql)
        plan.execute()
        
    def process_output(tmpdir, output_file_in, bucket_in, file_path_in):
        buffer = io.BytesIO()
        with open(output_file, "rb") as fd:
            while True:
                data = fd.read(8192)
                if len(data) == 0:
                    plpy.info("Break")
                    break
                else:
                    buffer.write(data)
        plpy.info("Break read loop")
        buffer.seek(0,2)
        buffer_size = buffer.tell()
        plpy.info(f"Buffer size: {buffer_size}")
        buffer.seek(0, 0)
        plpy.info(f"Uploading object to S3: {bucket_in}/{file_path_in}")
        s3.upload_fileobj(buffer, bucket_in, file_path_in)
        return buffer_size


    # Imports
    boto3 = cache_import('boto3')
    tempfile = cache_import('tempfile')
    os = cache_import('os')
    uuid = cache_import('uuid')
    concurrent_futures = cache_import('concurrent.futures')
    io = cache_import('io')
    time = cache_import('time')
    
    plan = plpy.prepare("select name, current_setting('aws_s3.' || name, true) as value from (select unnest(array['access_key_id', 'secret_access_key', 'session_token', 'endpoint_url']) as name) a");
    default_aws_settings = {
        row['name']: row['value']
        for row in plan.execute()
    }

    aws_settings = {
        'aws_access_key_id': access_key if access_key else default_aws_settings.get('access_key_id', 'unknown'),
        'aws_secret_access_key': secret_key if secret_key else default_aws_settings.get('secret_access_key', 'unknown'),
        'aws_session_token': session_token if session_token else default_aws_settings.get('session_token'),
        'endpoint_url': endpoint_url if endpoint_url else default_aws_settings.get('endpoint_url')
    }

    s3 = boto3.client(
        's3',
        region_name=region,
        **aws_settings
    )
    
    tmpdir = tempfile.mkdtemp()
    output_file = os.path.join(tmpdir, 's3_export_fifo')
    os.mkfifo(output_file)
    plpy.info(f"Created FIFO: {output_file}")
    with concurrent_futures.ThreadPoolExecutor(max_workers=5) as executor:
        plpy.info("Starting processing")
        process_output_job = executor.submit(process_output, tmpdir=tmpdir, output_file_in=output_file, bucket_in=bucket, file_path_in=file_path)
        plpy.info("Thread submitted")
        run_copy(query_in=query, output_file_in=output_file, options_in=options)
        processed_bytes = process_output_job.result()
        plpy.info(f"Uploaded bytes: {processed_bytes}")
    os.remove(output_file)
    os.rmdir(tmpdir)
    yield (0, 0, 0)
$$;