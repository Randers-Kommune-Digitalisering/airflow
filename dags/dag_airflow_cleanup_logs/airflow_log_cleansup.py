import os
import shutil
import logging

logger = logging.getLogger(__name__)


def cleanup_logs_by_disk_usage(directory: str, threshold_percent: int) -> None:
    total, used, _ = shutil.disk_usage(directory)
    used_percent = (used / total) * 100

    if used_percent < threshold_percent:
        logger.info(f"Disk usage is {used_percent:.2f}%%, below threshold. No cleanup needed.")
        return

    logger.info(f"Disk usage is {used_percent:.2f}%%, starting cleanup...")

    log_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                mtime = os.path.getmtime(file_path)
                log_files.append((file_path, mtime))
            except Exception as e:
                logger.error(f"Error accessing {file_path}: {e}")
                raise

    log_files.sort(key=lambda x: x[1])

    for file_path, _ in log_files:
        try:
            os.remove(file_path)
            logger.info(f"Deleted: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")
            raise

        _, used, _ = shutil.disk_usage(directory)
        used_percent = (used / total) * 100
        if used_percent < threshold_percent:
            logger.info(f"Cleanup complete. Disk usage is now {used_percent:.2f}%%.")
            break
