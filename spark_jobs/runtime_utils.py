import os


def _truthy(value):
    if value is None:
        return False

    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_streaming_runtime(default_processing_time="30 seconds"):
    """
    Trả về đúng 2 giá trị như weather_streaming.py đang expect:

    stop_after_batch, processing_time
    """

    stop_after_batch = _truthy(os.getenv("STOP_AFTER_BATCH"))
    processing_time = os.getenv("PROCESSING_TIME", default_processing_time)

    return stop_after_batch, processing_time


def apply_stream_trigger(writer, stop_after_batch=False, processing_time="30 seconds"):
    """
    Nếu stop_after_batch=True thì Spark xử lý một batch rồi dừng.
    Nếu False thì chạy streaming liên tục theo processing_time.
    """

    if stop_after_batch:
        return writer.trigger(once=True)

    return writer.trigger(processingTime=processing_time)