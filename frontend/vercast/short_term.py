from vercast.utils import Forecast


class PerfectForesight(Forecast):
    """
    An anticipative forecasting method. The information is perfectly known over the forecast horizon.

    :param list data: A list of forecast data.
    """

    def __init__(self, data):
        Forecast.__init__(self, data)

    def compute_operation_forecasts(self, h, window):
        var_window = min(window, len(self.data)-h)
        return self.data[h:h+var_window] + [0] * (window - var_window)

