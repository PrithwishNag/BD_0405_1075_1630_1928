# BD_0405_1075_1630_1928

Client code -> spawns jobs.
Master code -> accepts jobs and delivers to right worker
Worker code -> executes the assigned tasks by the mapper

Client: open terminal --> syntax: python <job request> <# jobs>
Master: open terminal --> syntax: python Master.py <config file> <schedulling algo>
Workers: open 3 terminals for 3 workers --> syntax: python Worker.py <port> <worker_id>

