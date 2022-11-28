def get_time():
    from datetime import datetime
    from pytz import timezone
    return datetime.now().astimezone(timezone("America/Sao_Paulo")).strftime("%Y_%m_%d_%H_%M_%S")
