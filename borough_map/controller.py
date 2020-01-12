from borough_map.data_loader import DataLoader
from borough_map.map_view import MapView
import itertools
import logging

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
        start_year, end_year, end_month = 1995, 2019, 11
        self._frames = (end_year - start_year) * 12 + end_month
        self._input_iterator = self._get_year_month_pair_iterator(
            start_year, end_year, end_month
        )

    def show(self, year, month):
        colors = self.data_loader.get_mean_prices(year, month).values
        self.map_view.initial_draw()
        self.map_view.set_colors_for_patches(colors)
        self.map_view.show()

    def _get_year_month_pair_iterator(self, start_year, end_year, end_month):
        years = range(start_year, end_year + 1)
        months = range(1, 13)
        for (year, month) in itertools.product(years, months):
            if year == end_year and month > end_month:
                raise StopIteration()
            yield (year, month)

    def _update(self, i):
        year, month = next(self._input_iterator)
        colors = self.data_loader.get_mean_prices(year, month).values
        self.map_view.set_colors_for_patches(colors)
        self.map_view.draw_year_month_on_axis(year, month)
        # self.map_view.show()

    def animate(self):
        self.map_view.animate(self._update, self._frames)


if __name__ == "__main__":
    controller = Controller()
    controller.animate()
    controller.show(1996, 1)
    controller.map_view.initial_draw()
    controller.map_view.show()
