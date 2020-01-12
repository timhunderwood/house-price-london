from borough_map.data_loader import DataLoader
from borough_map.map_view import MapView
import itertools


class Controller(object):
    """
    Controls the data and maps it to the map view.
    """

    def __init__(self):
        self.data_loader = DataLoader()
        self.map_view = MapView()
        self.data_loader.load_prepare_and_aggregate_data()
        self._input_iterator = self._get_year_month_pair_iterator()

    def show(self, year, month):
        colors = self.data_loader.get_mean_prices(year, month).values
        self.map_view.initial_draw()
        self.map_view.set_colors_for_patches(colors)
        self.map_view.show()

    def _get_year_month_pair_iterator(self):
        years = range(1995, 2019)
        months = range(1, 13)
        return itertools.product(years, months)

    def _update(self, i):
        year, month = next(self._input_iterator)
        colors = self.data_loader.get_mean_prices(year, month).values
        self.map_view.set_colors_for_patches(colors)
        self.map_view.draw_year_month_on_axis(year, month)
        # self.map_view.show()

    def animate(self):
        self.map_view.animate(self._update)


if __name__ == "__main__":
    controller = Controller()
    controller.animate()
    controller.show(1996, 1)
    controller.map_view.initial_draw()
    controller.map_view.show()
