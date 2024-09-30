from datetime import datetime
import pytz

class UTCTimeCalculator:
    def __init__(self,  
                 hour_in_EST: int):
        """
        Reusable class to calculate UTC hour given the hour in EST.

        :param scope: The CDK scope (Stack or Construct) where this alarm will be defined.
        :param hour_in_EST: The hour in EST to calculate.
        """
        self.hour_in_EST = hour_in_EST 

    def calculate_utc_hour(self) -> int:
         
        # Define the EST timezone
        est = pytz.timezone("America/New_York")

        # Get the current time in UTC
        utc_now = datetime.now(pytz.utc)

        # Get 3 AM EST on the current day
        est_time = est.localize(datetime(utc_now.year, utc_now.month, utc_now.day, self.hour_in_EST, 0, 0))

        # Convert 3 AM EST to UTC
        utc_time = est_time.astimezone(pytz.utc)

        # return the UTC hour
        return utc_time.hour
