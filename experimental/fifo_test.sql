CREATE OR REPLACE FUNCTION aws_s3.fifo_test(
    row_count int,
) RETURNS SETOF RECORD
LANGUAGE plpython3u
AS $$
    import os
    import uuid
    import concurrent.futures
    import tempfile
    import io
    import time


    def create_test_table(row_count_in: int):
        plpy.prepare
        sql = """
            create table public.fifo_test as
                select md5(random()::text) as 
                from generate_series(1,$1)
        """
        plan = plpy.prepare(sql, ["int"])
        plpy.execute(plan, [row_count])
        plpy.info(f"Created test table. Rows: {row_count}")

    def run_copy(fifo_in):
        plpy.info("Executing COPY")
        sql = f"COPY public.fifo_test TO '{fifo_in}' (FORMAT CSV)" 
        plpy.info(f"COPY: {sql}")
        plan = plpy.prepare(sql)
        plan.execute()
        
    def process_output(fifo_in):
        read_count = 0
        bytes_count = 0
        with open(fifo_in) as f:
            while True:
                data = f.read()
                if len(data) == 0:
                    plpy.info("Break")
                    break
                else:
                    read_count += 1
                    bytes_count += len(data)
        return read_count, bytes_count
    
    tmpdir = tempfile.mkdtemp()
    FIFO = os.path.join(tmpdir, "fifo_test")
    os.mkfifo(FIFO)
    plpy.info(f"Created FIFO: {FIFO}")
    with concurrent_futures.ThreadPoolExecutor(max_workers=5) as executor:
        plpy.info("Starting processing")
        process_output_job = executor.submit(process_output, fifo_in=FIFO)
        plpy.info("Thread submitted")
        run_copy()
        processed_bytes = process_output_job.result()
        plpy.info(f"Uploaded bytes: {processed_bytes}")
    os.remove(output_file)
    os.rmdir(tmpdir)
    yield (0, 0, 0)
$$;