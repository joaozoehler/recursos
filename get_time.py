def get_time():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")
