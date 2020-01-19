from borough_map.data_loader import DataLoader
from borough_map.map_view import MapView
import itertools
import logging
import pandas

logging.basicConfig()
LOGGER = logging.getLogger(__file__)


class Controller(object):
    """
    Controls the data and maps it to the map view.
    """

    def __init__(self):
        self.data_loader = DataLoader()
        self.map_view = MapView()
        self.data_loader.load_prepare_and_aggregate_data()
        self._start_year, self._end_year, self._end_month = 1995, 2019, 11
        self._frames = (self._end_year - self._start_year) * 12 + self._end_month
        self._input_iterator = self._get_year_month_pair_iterator()

    def show(self, year, month):
        colors = self.data_loader.get_mean_prices(year, month).values
        # colors[20] = 1E7
        self.map_view.initial_draw()
        self.map_view.set_colors_for_patches(colors)
        self.map_view.show()

    def _get_year_month_pair_iterator(self,):
        years = range(self._start_year, self._end_year + 1)
        months = range(1, 13)
        for (year, month) in itertools.product(years, months):
            if year == self._end_year and month > self._end_month:
                raise StopIteration()
            yield (year, month)

    def _update(self, i):
        year, month = next(self._input_iterator)
        colors = self.data_loader.get_median_prices(year, month).values
        self.map_view.set_colors_for_patches(colors)
        self.map_view.draw_year_month_on_axis(year, month)
        plot_x_data, plot_y_data = self.data_loader.get_line_data()
        self.map_view.plot_line(plot_x_data[:i], plot_y_data[:i])

    def animate(self):
        self.map_view.animate(self._update, self._frames)


if __name__ == "__main__":
    controller = Controller()
    #    controller.show(1996, 1)
    controller.animate()
    controller.map_view.initial_draw()
    controller.map_view.show()
