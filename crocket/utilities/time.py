from datetime import datetime, timezone


def format_time(datetime_to_format, time_format="%Y-%m-%d %H:%M:%S.%f"):
    """
    Format datetime to string.
    Ex: 2017-09-22 12:28:22
    :return:
    """
    return datetime_to_format.strftime(time_format)


def convert_bittrex_timestamp_to_datetime(timestamp, time_format="%Y-%m-%dT%H:%M:%S.%f"):
    """
    Convert timestamp string to datetime.
    :param timestamp:
    :param time_format:
    :return:
    """
    try:
        converted_datetime = datetime.strptime(timestamp, time_format)
    except ValueError:
        converted_datetime = datetime.strptime('{}.0'.format(timestamp), time_format)

    return converted_datetime


def utc_to_local(utc_dt):
    """
    Convert UTC datetime to local datetime.
    :param utc_dt:
    :return:
    """
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)