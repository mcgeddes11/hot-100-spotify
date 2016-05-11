if __name__ == "__main__":

    # Some required luigi imports
    import luigi
    import luigi.scheduler
    import luigi.worker
    import os, logging
    import pandas
    import json
    import sys
    from datetime import date, datetime, timedelta
    from task_utils import createOutputDirectoryFromFilename
    from task_def import GetSongsByYear, GetTrackUrlsByYear

    if len(sys.argv) < 2:
        raise Exception("No config path file, exiting")
    else:
        configPath = sys.argv[1]

    # Load config
    with open(configPath) as f:
        config = json.load(f)

    # Create data repository if it doesn't yet exist
    createOutputDirectoryFromFilename(os.path.join(config["data_repository"],"fu.txt"))

    # Set up logging
    log_path = os.path.join(config["data_repository"], "process_log.log")
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    log_handle = logging.FileHandler(log_path)
    log_handle.setFormatter(formatter)
    logger = logging.getLogger('luigi-interface')
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handle)

    yrRange = range(config["year_range"][0], config["year_range"][1]+1)

    tasks = []

    for yr in yrRange:
        tasks.append(GetSongsByYear(config, yr))
        tasks.append(GetTrackUrlsByYear(config, yr, ))

    luigi.build(tasks, local_scheduler=True, workers=1)


